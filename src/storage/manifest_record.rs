#![allow(dead_code)]

use crate::common::{FusionError, Result};
use std::io::{Read, Write};

pub const MANIFEST_RECORD_BLOCK_SIZE: usize = 32 * 1024;
pub const MANIFEST_RECORD_HEADER_SIZE: usize = 7;

const CRC32C_POLY_REVERSED: u32 = 0x82f6_3b78;
const CRC32C_MASK_DELTA: u32 = 0xa282_ead8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum PhysicalRecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl PhysicalRecordType {
    fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            1 => Ok(Self::Full),
            2 => Ok(Self::First),
            3 => Ok(Self::Middle),
            4 => Ok(Self::Last),
            other => Err(corrupt(format!(
                "unknown manifest physical record type {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestRecordRecoveryMode {
    Strict,
    RecoverTornTail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestRecordOffset {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRecordReplay {
    pub records: Vec<Vec<u8>>,
    pub valid_bytes: u64,
    pub recovered_tail: bool,
}

pub struct ManifestRecordWriter<W: Write> {
    writer: W,
    block_offset: usize,
    logical_offset: u64,
}

impl<W: Write> ManifestRecordWriter<W> {
    pub fn new(writer: W) -> Self {
        Self::new_appending(writer, 0)
    }

    pub fn new_appending(writer: W, existing_len: u64) -> Self {
        Self {
            writer,
            block_offset: existing_len as usize % MANIFEST_RECORD_BLOCK_SIZE,
            logical_offset: existing_len,
        }
    }

    pub fn write_record(&mut self, payload: &[u8]) -> Result<ManifestRecordOffset> {
        let start = self.logical_offset;
        if payload.is_empty() {
            self.emit_physical_record(PhysicalRecordType::Full, payload)?;
            return Ok(ManifestRecordOffset {
                start,
                end: self.logical_offset,
            });
        }

        let mut remaining = payload;
        let mut begin = true;
        while !remaining.is_empty() {
            let block_remaining = MANIFEST_RECORD_BLOCK_SIZE - self.block_offset;
            if block_remaining < MANIFEST_RECORD_HEADER_SIZE {
                self.pad_block(block_remaining)?;
            }
            if block_remaining == MANIFEST_RECORD_HEADER_SIZE {
                self.emit_physical_record(PhysicalRecordType::First, &[])?;
                begin = false;
                continue;
            }

            let available =
                MANIFEST_RECORD_BLOCK_SIZE - self.block_offset - MANIFEST_RECORD_HEADER_SIZE;
            let fragment_len = remaining.len().min(available);
            let end = fragment_len == remaining.len();
            let record_type = match (begin, end) {
                (true, true) => PhysicalRecordType::Full,
                (true, false) => PhysicalRecordType::First,
                (false, true) => PhysicalRecordType::Last,
                (false, false) => PhysicalRecordType::Middle,
            };

            self.emit_physical_record(record_type, &remaining[..fragment_len])?;
            remaining = &remaining[fragment_len..];
            begin = false;
        }

        Ok(ManifestRecordOffset {
            start,
            end: self.logical_offset,
        })
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(FusionError::Io)
    }

    pub fn logical_offset(&self) -> u64 {
        self.logical_offset
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    fn pad_block(&mut self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            self.block_offset = 0;
            return Ok(());
        }
        let padding = [0u8; MANIFEST_RECORD_HEADER_SIZE];
        self.writer
            .write_all(&padding[..bytes])
            .map_err(FusionError::Io)?;
        self.logical_offset += bytes as u64;
        self.block_offset = 0;
        Ok(())
    }

    fn emit_physical_record(
        &mut self,
        record_type: PhysicalRecordType,
        fragment: &[u8],
    ) -> Result<()> {
        if fragment.len() > u16::MAX as usize {
            return Err(corrupt("manifest physical record fragment too large"));
        }
        let crc = mask_crc32c(record_crc32c(record_type as u8, fragment));
        let mut header = [0u8; MANIFEST_RECORD_HEADER_SIZE];
        header[..4].copy_from_slice(&crc.to_le_bytes());
        header[4..6].copy_from_slice(&(fragment.len() as u16).to_le_bytes());
        header[6] = record_type as u8;

        self.writer.write_all(&header).map_err(FusionError::Io)?;
        self.writer.write_all(fragment).map_err(FusionError::Io)?;

        let bytes = MANIFEST_RECORD_HEADER_SIZE + fragment.len();
        self.logical_offset += bytes as u64;
        self.block_offset += bytes;
        if self.block_offset == MANIFEST_RECORD_BLOCK_SIZE {
            self.block_offset = 0;
        }
        Ok(())
    }
}

pub struct ManifestRecordReader<R: Read> {
    reader: R,
    mode: ManifestRecordRecoveryMode,
    block_offset: usize,
    physical_offset: u64,
    last_valid_offset: u64,
    recovered_tail: bool,
    scratch: Vec<u8>,
}

impl<R: Read> ManifestRecordReader<R> {
    pub fn new(reader: R) -> Self {
        Self::with_mode(reader, ManifestRecordRecoveryMode::RecoverTornTail)
    }

    pub fn new_strict(reader: R) -> Self {
        Self::with_mode(reader, ManifestRecordRecoveryMode::Strict)
    }

    pub fn with_mode(reader: R, mode: ManifestRecordRecoveryMode) -> Self {
        Self {
            reader,
            mode,
            block_offset: 0,
            physical_offset: 0,
            last_valid_offset: 0,
            recovered_tail: false,
            scratch: Vec::new(),
        }
    }

    pub fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        self.scratch.clear();
        let mut assembling = false;

        loop {
            let Some((record_type, fragment, physical_end)) = self.read_physical_record()? else {
                if assembling {
                    self.recovered_tail = true;
                    if self.mode == ManifestRecordRecoveryMode::Strict {
                        return Err(corrupt(
                            "manifest fragmented record reached EOF before LAST record",
                        ));
                    }
                }
                return Ok(None);
            };

            match record_type {
                PhysicalRecordType::Full => {
                    if assembling {
                        return Err(corrupt(
                            "manifest FULL record encountered inside fragmented record",
                        ));
                    }
                    self.last_valid_offset = physical_end;
                    return Ok(Some(fragment));
                }
                PhysicalRecordType::First => {
                    if assembling {
                        return Err(corrupt(
                            "manifest FIRST record encountered inside fragmented record",
                        ));
                    }
                    self.scratch.extend_from_slice(&fragment);
                    assembling = true;
                }
                PhysicalRecordType::Middle => {
                    if !assembling {
                        return Err(corrupt(
                            "manifest MIDDLE record without preceding FIRST record",
                        ));
                    }
                    self.scratch.extend_from_slice(&fragment);
                }
                PhysicalRecordType::Last => {
                    if !assembling {
                        return Err(corrupt(
                            "manifest LAST record without preceding FIRST record",
                        ));
                    }
                    self.scratch.extend_from_slice(&fragment);
                    self.last_valid_offset = physical_end;
                    return Ok(Some(std::mem::take(&mut self.scratch)));
                }
            }
        }
    }

    pub fn last_valid_offset(&self) -> u64 {
        self.last_valid_offset
    }

    pub fn recovered_tail(&self) -> bool {
        self.recovered_tail
    }

    pub fn read_all(mut self) -> Result<ManifestRecordReplay> {
        let mut records = Vec::new();
        while let Some(record) = self.read_record()? {
            records.push(record);
        }
        Ok(ManifestRecordReplay {
            records,
            valid_bytes: self.last_valid_offset,
            recovered_tail: self.recovered_tail,
        })
    }

    fn read_physical_record(&mut self) -> Result<Option<(PhysicalRecordType, Vec<u8>, u64)>> {
        loop {
            let block_remaining = MANIFEST_RECORD_BLOCK_SIZE - self.block_offset;
            if block_remaining < MANIFEST_RECORD_HEADER_SIZE {
                if !self.read_and_validate_padding(block_remaining)? {
                    return Ok(None);
                }
                continue;
            }

            let header_start = self.physical_offset;
            let mut header = [0u8; MANIFEST_RECORD_HEADER_SIZE];
            let bytes_read = self.read_exact_or_tail(&mut header)?;
            if bytes_read == 0 {
                return Ok(None);
            }
            if bytes_read < MANIFEST_RECORD_HEADER_SIZE {
                self.recovered_tail = true;
                if self.mode == ManifestRecordRecoveryMode::Strict {
                    return Err(corrupt(format!(
                        "manifest torn physical record header at offset {header_start}"
                    )));
                }
                return Ok(None);
            }

            self.physical_offset += MANIFEST_RECORD_HEADER_SIZE as u64;
            self.block_offset += MANIFEST_RECORD_HEADER_SIZE;

            if header == [0u8; MANIFEST_RECORD_HEADER_SIZE] {
                if self.block_offset == MANIFEST_RECORD_BLOCK_SIZE {
                    self.block_offset = 0;
                    continue;
                }
                return Err(corrupt("zero manifest record header outside trailer"));
            }

            let expected_crc = u32::from_le_bytes(header[..4].try_into().unwrap());
            let length = u16::from_le_bytes(header[4..6].try_into().unwrap()) as usize;
            let record_type = PhysicalRecordType::from_byte(header[6])?;
            let available = MANIFEST_RECORD_BLOCK_SIZE - self.block_offset;
            if length > available {
                return Err(corrupt(format!(
                    "manifest physical record length {length} exceeds block remainder {available}"
                )));
            }

            let mut fragment = vec![0u8; length];
            let bytes_read = self.read_exact_or_tail(&mut fragment)?;
            if bytes_read < length {
                self.recovered_tail = true;
                if self.mode == ManifestRecordRecoveryMode::Strict {
                    return Err(corrupt(format!(
                        "manifest torn physical record payload at offset {header_start}"
                    )));
                }
                return Ok(None);
            }

            self.physical_offset += length as u64;
            self.block_offset += length;
            if self.block_offset == MANIFEST_RECORD_BLOCK_SIZE {
                self.block_offset = 0;
            }

            let actual_crc = mask_crc32c(record_crc32c(record_type as u8, &fragment));
            if actual_crc != expected_crc {
                return Err(corrupt(format!(
                    "manifest physical record checksum mismatch at offset {header_start}"
                )));
            }

            return Ok(Some((record_type, fragment, self.physical_offset)));
        }
    }

    fn read_and_validate_padding(&mut self, bytes: usize) -> Result<bool> {
        if bytes == 0 {
            self.block_offset = 0;
            return Ok(true);
        }
        let mut padding = vec![0u8; bytes];
        let bytes_read = self.read_exact_or_tail(&mut padding)?;
        if bytes_read == 0 {
            return Ok(false);
        }
        if bytes_read < bytes {
            self.recovered_tail = true;
            if self.mode == ManifestRecordRecoveryMode::Strict {
                return Err(corrupt("manifest torn block trailer"));
            }
            return Ok(false);
        }
        if padding.iter().any(|byte| *byte != 0) {
            return Err(corrupt("non-zero manifest record block trailer"));
        }
        self.physical_offset += bytes as u64;
        self.block_offset = 0;
        Ok(true)
    }

    fn read_exact_or_tail(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut read = 0;
        while read < buf.len() {
            match self.reader.read(&mut buf[read..]) {
                Ok(0) => break,
                Ok(n) => read += n,
                Err(error) => return Err(FusionError::Io(error)),
            }
        }
        Ok(read)
    }
}

fn record_crc32c(record_type: u8, payload: &[u8]) -> u32 {
    let crc = crc32c_extend(!0u32, &[record_type]);
    !crc32c_extend(crc, payload)
}

fn crc32c(bytes: &[u8]) -> u32 {
    !crc32c_extend(!0u32, bytes)
}

fn crc32c_extend(mut crc: u32, bytes: &[u8]) -> u32 {
    for byte in bytes {
        crc ^= *byte as u32;
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (CRC32C_POLY_REVERSED & mask);
        }
    }
    crc
}

fn mask_crc32c(crc: u32) -> u32 {
    crc.rotate_right(15).wrapping_add(CRC32C_MASK_DELTA)
}

fn corrupt(message: impl Into<String>) -> FusionError {
    FusionError::Storage(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_all_records(bytes: &[u8]) -> Result<(Vec<Vec<u8>>, u64)> {
        let replay = ManifestRecordReader::new(bytes).read_all()?;
        Ok((replay.records, replay.valid_bytes))
    }

    #[test]
    fn manifest_record_crc32c_matches_standard_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn manifest_record_round_trips_small_records() {
        let mut encoded = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut encoded);
            let first = writer.write_record(b"snapshot").unwrap();
            let second = writer.write_record(b"add-sstable").unwrap();
            writer.flush().unwrap();
            assert_eq!(first.start, 0);
            assert_eq!(first.end, second.start);
            assert_eq!(writer.logical_offset(), encoded.len() as u64);
        }

        let (records, last_valid_offset) = read_all_records(&encoded).unwrap();
        assert_eq!(records, vec![b"snapshot".to_vec(), b"add-sstable".to_vec()]);
        assert_eq!(last_valid_offset, encoded.len() as u64);
    }

    #[test]
    fn manifest_record_round_trips_empty_record() {
        let mut encoded = Vec::new();
        ManifestRecordWriter::new(&mut encoded)
            .write_record(b"")
            .unwrap();

        let (records, last_valid_offset) = read_all_records(&encoded).unwrap();
        assert_eq!(records, vec![Vec::<u8>::new()]);
        assert_eq!(last_valid_offset, encoded.len() as u64);
    }

    #[test]
    fn manifest_record_fragments_large_record_across_blocks() {
        let payload = vec![b'x'; MANIFEST_RECORD_BLOCK_SIZE * 2 + 123];
        let mut encoded = Vec::new();
        ManifestRecordWriter::new(&mut encoded)
            .write_record(&payload)
            .unwrap();

        let (records, last_valid_offset) = read_all_records(&encoded).unwrap();
        assert_eq!(records, vec![payload]);
        assert_eq!(last_valid_offset, encoded.len() as u64);
        assert!(encoded.len() > MANIFEST_RECORD_BLOCK_SIZE * 2);
    }

    #[test]
    fn manifest_record_recovers_prefix_when_tail_header_is_torn() {
        let mut encoded = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut encoded);
            writer.write_record(b"stable").unwrap();
            writer.write_record(b"tail").unwrap();
        }
        let stable_len = {
            let mut single = Vec::new();
            ManifestRecordWriter::new(&mut single)
                .write_record(b"stable")
                .unwrap();
            single.len()
        };
        encoded.truncate(stable_len + 3);

        let (records, last_valid_offset) = read_all_records(&encoded).unwrap();
        assert_eq!(records, vec![b"stable".to_vec()]);
        assert_eq!(last_valid_offset, stable_len as u64);
        let replay = ManifestRecordReader::new(encoded.as_slice())
            .read_all()
            .unwrap();
        assert!(replay.recovered_tail);
        assert_eq!(replay.valid_bytes, stable_len as u64);
    }

    #[test]
    fn manifest_record_recovers_prefix_when_tail_payload_is_torn() {
        let mut encoded = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut encoded);
            writer.write_record(b"stable").unwrap();
            writer.write_record(b"tail-payload").unwrap();
        }
        let stable_len = {
            let mut single = Vec::new();
            ManifestRecordWriter::new(&mut single)
                .write_record(b"stable")
                .unwrap();
            single.len()
        };
        encoded.truncate(encoded.len() - 2);

        let (records, last_valid_offset) = read_all_records(&encoded).unwrap();
        assert_eq!(records, vec![b"stable".to_vec()]);
        assert_eq!(last_valid_offset, stable_len as u64);
        let error = ManifestRecordReader::new_strict(encoded.as_slice())
            .read_all()
            .unwrap_err();
        assert!(error.to_string().contains("torn physical record payload"));
    }

    #[test]
    fn manifest_record_fails_on_middle_checksum_corruption() {
        let mut encoded = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut encoded);
            writer.write_record(b"stable").unwrap();
            writer.write_record(b"committed").unwrap();
        }
        let stable_len = {
            let mut single = Vec::new();
            ManifestRecordWriter::new(&mut single)
                .write_record(b"stable")
                .unwrap();
            single.len()
        };
        encoded[stable_len + MANIFEST_RECORD_HEADER_SIZE] ^= 0xff;

        let mut reader = ManifestRecordReader::new(encoded.as_slice());
        assert_eq!(reader.read_record().unwrap(), Some(b"stable".to_vec()));
        let error = reader.read_record().unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn manifest_record_fails_on_fragment_hole() {
        let record_type = PhysicalRecordType::Middle as u8;
        let payload = b"orphan-middle";
        let mut encoded = Vec::new();
        let crc = mask_crc32c(record_crc32c(record_type, payload));
        encoded.extend_from_slice(&crc.to_le_bytes());
        encoded.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        encoded.push(record_type);
        encoded.extend_from_slice(payload);

        let error = ManifestRecordReader::new(encoded.as_slice())
            .read_record()
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("MIDDLE record without preceding FIRST"));
    }

    #[test]
    fn manifest_record_fails_on_fragment_hole_before_later_full_record() {
        fn append_physical_record(
            encoded: &mut Vec<u8>,
            record_type: PhysicalRecordType,
            payload: &[u8],
        ) {
            let crc = mask_crc32c(record_crc32c(record_type as u8, payload));
            encoded.extend_from_slice(&crc.to_le_bytes());
            encoded.extend_from_slice(&(payload.len() as u16).to_le_bytes());
            encoded.push(record_type as u8);
            encoded.extend_from_slice(payload);
        }

        let mut encoded = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut encoded);
            writer.write_record(b"stable").unwrap();
        }
        append_physical_record(&mut encoded, PhysicalRecordType::First, b"dangling-first");
        append_physical_record(&mut encoded, PhysicalRecordType::Full, b"later-full");

        let mut reader = ManifestRecordReader::new(encoded.as_slice());
        assert_eq!(reader.read_record().unwrap(), Some(b"stable".to_vec()));
        let error = reader.read_record().unwrap_err();
        assert!(error
            .to_string()
            .contains("FULL record encountered inside fragmented record"));
    }

    #[test]
    fn manifest_record_pads_short_block_trailer() {
        let first_payload_len = MANIFEST_RECORD_BLOCK_SIZE - MANIFEST_RECORD_HEADER_SIZE - 3;
        let first = vec![b'a'; first_payload_len];
        let second = b"next-block".to_vec();
        let mut encoded = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut encoded);
            writer.write_record(&first).unwrap();
            writer.write_record(&second).unwrap();
        }

        assert_eq!(
            &encoded[MANIFEST_RECORD_BLOCK_SIZE - 3..MANIFEST_RECORD_BLOCK_SIZE],
            &[0, 0, 0]
        );
        let (records, _) = read_all_records(&encoded).unwrap();
        assert_eq!(records, vec![first, second]);
    }

    #[test]
    fn manifest_record_writes_zero_length_first_when_exact_header_space_remains() {
        let first_payload_len = MANIFEST_RECORD_BLOCK_SIZE - MANIFEST_RECORD_HEADER_SIZE * 2;
        let first = vec![b'a'; first_payload_len];
        let second = b"next-block".to_vec();
        let mut encoded = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut encoded);
            writer.write_record(&first).unwrap();
            assert_eq!(
                writer.block_offset,
                MANIFEST_RECORD_BLOCK_SIZE - MANIFEST_RECORD_HEADER_SIZE
            );
            writer.write_record(&second).unwrap();
        }

        let filler_offset = MANIFEST_RECORD_BLOCK_SIZE - MANIFEST_RECORD_HEADER_SIZE;
        assert_eq!(encoded[filler_offset + 4], 0);
        assert_eq!(encoded[filler_offset + 5], 0);
        assert_eq!(encoded[filler_offset + 6], PhysicalRecordType::First as u8);
        let (records, _) = read_all_records(&encoded).unwrap();
        assert_eq!(records, vec![first, second]);
    }

    #[test]
    fn manifest_record_appending_respects_existing_block_offset() {
        let mut encoded = Vec::new();
        let first_end = {
            let mut writer = ManifestRecordWriter::new(&mut encoded);
            writer.write_record(b"first").unwrap().end
        };

        {
            let mut writer = ManifestRecordWriter::new_appending(&mut encoded, first_end);
            let second_offset = writer.write_record(b"second").unwrap();
            assert_eq!(second_offset.start, first_end);
        }

        let replay = ManifestRecordReader::new(encoded.as_slice())
            .read_all()
            .unwrap();
        assert_eq!(replay.records, vec![b"first".to_vec(), b"second".to_vec()]);
        assert_eq!(replay.valid_bytes, encoded.len() as u64);
    }
}

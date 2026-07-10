#![allow(dead_code)]

use super::manifest_record::{
    ManifestRecordOffset, ManifestRecordReader, ManifestRecordReplay, ManifestRecordWriter,
};
use crate::common::{FusionError, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};

const MANIFEST_EDIT_MAGIC: &[u8; 4] = b"FMED";
const MANIFEST_EDIT_VERSION: u16 = 1;
const MANIFEST_EDIT_MAX_FILE_NAME_BYTES: usize = 4096;
const MANIFEST_EDIT_MAX_KEY_BYTES: usize = 64 * 1024;
const MANIFEST_EDIT_MAX_FILE_COUNT: usize = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestSstableFingerprint {
    pub file_len: u64,
    pub modified_unix_secs: u64,
    pub modified_subsec_nanos: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestSstableEntry {
    pub id: u64,
    pub file_name: String,
    pub fingerprint: ManifestSstableFingerprint,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub format_version: u32,
    pub max_ts: u64,
    pub content_fingerprint: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ManifestWalReplayFloor {
    pub wal_generation: u64,
    pub segment_id: u64,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestEdit {
    Snapshot {
        files: Vec<ManifestSstableEntry>,
        next_file_number: u64,
        high_watermark: u64,
        wal_replay_floor: Option<ManifestWalReplayFloor>,
    },
    AddSstable(ManifestSstableEntry),
    DeleteSstable {
        id: u64,
    },
    Compact {
        delete_ids: Vec<u64>,
        add: ManifestSstableEntry,
    },
    SetNextFileNumber(u64),
    SetHighWatermark(u64),
    SetWalReplayFloor(ManifestWalReplayFloor),
    VersionEdit {
        delete_ids: Vec<u64>,
        add_files: Vec<ManifestSstableEntry>,
        next_file_number: Option<u64>,
        high_watermark: Option<u64>,
        wal_replay_floor: Option<ManifestWalReplayFloor>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManifestVersionState {
    pub files: BTreeMap<u64, ManifestSstableEntry>,
    pub next_file_number: u64,
    pub high_watermark: u64,
    pub wal_replay_floor: Option<ManifestWalReplayFloor>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ManifestEditReplay {
    pub edits: Vec<ManifestEdit>,
    pub state: ManifestVersionState,
    pub valid_bytes: u64,
    pub recovered_tail: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum ManifestEditTag {
    Snapshot = 1,
    AddSstable = 2,
    DeleteSstable = 3,
    Compact = 4,
    SetNextFileNumber = 5,
    SetHighWatermark = 6,
    SetWalReplayFloor = 7,
    VersionEdit = 8,
}

impl ManifestEditTag {
    fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            1 => Ok(Self::Snapshot),
            2 => Ok(Self::AddSstable),
            3 => Ok(Self::DeleteSstable),
            4 => Ok(Self::Compact),
            5 => Ok(Self::SetNextFileNumber),
            6 => Ok(Self::SetHighWatermark),
            7 => Ok(Self::SetWalReplayFloor),
            8 => Ok(Self::VersionEdit),
            other => Err(corrupt(format!("unknown manifest edit tag {other}"))),
        }
    }
}

impl ManifestEdit {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(MANIFEST_EDIT_MAGIC);
        out.extend_from_slice(&MANIFEST_EDIT_VERSION.to_le_bytes());
        out.push(self.tag() as u8);

        match self {
            Self::Snapshot {
                files,
                next_file_number,
                high_watermark,
                wal_replay_floor,
            } => {
                write_u64(&mut out, *next_file_number);
                write_u64(&mut out, *high_watermark);
                write_optional_wal_floor(&mut out, *wal_replay_floor);
                write_entries(&mut out, files)?;
            }
            Self::AddSstable(entry) => write_entry(&mut out, entry)?,
            Self::DeleteSstable { id } => write_u64(&mut out, *id),
            Self::Compact { delete_ids, add } => {
                write_u32(&mut out, checked_count(delete_ids.len())?);
                for id in delete_ids {
                    write_u64(&mut out, *id);
                }
                write_entry(&mut out, add)?;
            }
            Self::SetNextFileNumber(next_file_number) => write_u64(&mut out, *next_file_number),
            Self::SetHighWatermark(high_watermark) => write_u64(&mut out, *high_watermark),
            Self::SetWalReplayFloor(floor) => write_wal_floor(&mut out, *floor),
            Self::VersionEdit {
                delete_ids,
                add_files,
                next_file_number,
                high_watermark,
                wal_replay_floor,
            } => {
                write_u32(&mut out, checked_count(delete_ids.len())?);
                for id in delete_ids {
                    write_u64(&mut out, *id);
                }
                write_entries(&mut out, add_files)?;
                write_optional_u64(&mut out, *next_file_number);
                write_optional_u64(&mut out, *high_watermark);
                write_optional_wal_floor(&mut out, *wal_replay_floor);
            }
        }

        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = EditBytes::new(bytes);
        reader.expect_magic(MANIFEST_EDIT_MAGIC)?;
        let version = reader.read_u16()?;
        if version != MANIFEST_EDIT_VERSION {
            return Err(corrupt(format!(
                "unsupported manifest edit version {version}"
            )));
        }
        let tag = ManifestEditTag::from_byte(reader.read_u8()?)?;
        let edit = match tag {
            ManifestEditTag::Snapshot => {
                let next_file_number = reader.read_u64()?;
                let high_watermark = reader.read_u64()?;
                let wal_replay_floor = reader.read_optional_wal_floor()?;
                let files = reader.read_entries()?;
                Self::Snapshot {
                    files,
                    next_file_number,
                    high_watermark,
                    wal_replay_floor,
                }
            }
            ManifestEditTag::AddSstable => Self::AddSstable(reader.read_entry()?),
            ManifestEditTag::DeleteSstable => Self::DeleteSstable {
                id: reader.read_u64()?,
            },
            ManifestEditTag::Compact => {
                let delete_count = reader.read_count("manifest compact delete ids")?;
                let mut delete_ids = Vec::with_capacity(delete_count);
                for _ in 0..delete_count {
                    delete_ids.push(reader.read_u64()?);
                }
                let add = reader.read_entry()?;
                Self::Compact { delete_ids, add }
            }
            ManifestEditTag::SetNextFileNumber => Self::SetNextFileNumber(reader.read_u64()?),
            ManifestEditTag::SetHighWatermark => Self::SetHighWatermark(reader.read_u64()?),
            ManifestEditTag::SetWalReplayFloor => Self::SetWalReplayFloor(reader.read_wal_floor()?),
            ManifestEditTag::VersionEdit => {
                let delete_count = reader.read_count("manifest version edit delete ids")?;
                let mut delete_ids = Vec::with_capacity(delete_count);
                for _ in 0..delete_count {
                    delete_ids.push(reader.read_u64()?);
                }
                let add_files = reader.read_entries()?;
                let next_file_number = reader.read_optional_u64()?;
                let high_watermark = reader.read_optional_u64()?;
                let wal_replay_floor = reader.read_optional_wal_floor()?;
                Self::VersionEdit {
                    delete_ids,
                    add_files,
                    next_file_number,
                    high_watermark,
                    wal_replay_floor,
                }
            }
        };
        reader.finish()?;
        Ok(edit)
    }

    fn tag(&self) -> ManifestEditTag {
        match self {
            Self::Snapshot { .. } => ManifestEditTag::Snapshot,
            Self::AddSstable(_) => ManifestEditTag::AddSstable,
            Self::DeleteSstable { .. } => ManifestEditTag::DeleteSstable,
            Self::Compact { .. } => ManifestEditTag::Compact,
            Self::SetNextFileNumber(_) => ManifestEditTag::SetNextFileNumber,
            Self::SetHighWatermark(_) => ManifestEditTag::SetHighWatermark,
            Self::SetWalReplayFloor(_) => ManifestEditTag::SetWalReplayFloor,
            Self::VersionEdit { .. } => ManifestEditTag::VersionEdit,
        }
    }
}

impl ManifestVersionState {
    pub fn apply(&mut self, edit: ManifestEdit) -> Result<()> {
        match edit {
            ManifestEdit::Snapshot {
                files,
                next_file_number,
                high_watermark,
                wal_replay_floor,
            } => {
                let mut next_files = BTreeMap::new();
                for entry in files {
                    validate_sstable_entry(&entry)?;
                    if next_files.insert(entry.id, entry).is_some() {
                        return Err(corrupt("duplicate SSTable id in manifest snapshot"));
                    }
                }
                self.files = next_files;
                self.next_file_number = next_file_number;
                self.high_watermark = high_watermark;
                self.wal_replay_floor = wal_replay_floor;
                self.validate_invariants()
            }
            ManifestEdit::AddSstable(entry) => {
                validate_sstable_entry(&entry)?;
                if self.files.contains_key(&entry.id) {
                    return Err(corrupt(format!(
                        "manifest AddSstable duplicates live SSTable {}",
                        entry.id
                    )));
                }
                let entry_id = entry.id;
                let next_file_number = entry.id.saturating_add(1);
                let previous_next_file_number = self.next_file_number;
                self.files.insert(entry.id, entry);
                if self.next_file_number < next_file_number {
                    self.next_file_number = next_file_number;
                }
                if let Err(error) = self.validate_invariants() {
                    self.files.remove(&entry_id);
                    self.next_file_number = previous_next_file_number;
                    return Err(error);
                }
                Ok(())
            }
            ManifestEdit::DeleteSstable { id } => {
                if self.files.remove(&id).is_none() {
                    return Err(corrupt(format!(
                        "manifest DeleteSstable references missing SSTable {id}"
                    )));
                }
                Ok(())
            }
            ManifestEdit::Compact { delete_ids, add } => {
                validate_sstable_entry(&add)?;
                if self.files.contains_key(&add.id) && !delete_ids.contains(&add.id) {
                    return Err(corrupt(format!(
                        "manifest Compact output duplicates live SSTable {}",
                        add.id
                    )));
                }
                let mut removed: Vec<ManifestSstableEntry> = Vec::with_capacity(delete_ids.len());
                for id in delete_ids {
                    let Some(entry) = self.files.remove(&id) else {
                        for entry in removed {
                            self.files.insert(entry.id, entry);
                        }
                        return Err(corrupt(format!(
                            "manifest Compact references missing SSTable {id}"
                        )));
                    };
                    removed.push(entry);
                }
                let next_file_number = add.id.saturating_add(1);
                let add_id = add.id;
                let previous_next_file_number = self.next_file_number;
                self.files.insert(add_id, add);
                if self.next_file_number < next_file_number {
                    self.next_file_number = next_file_number;
                }
                if let Err(error) = self.validate_invariants() {
                    self.files.remove(&add_id);
                    for entry in removed {
                        self.files.insert(entry.id, entry);
                    }
                    self.next_file_number = previous_next_file_number;
                    return Err(error);
                }
                Ok(())
            }
            ManifestEdit::SetNextFileNumber(next_file_number) => {
                let min_next = self.min_next_file_number();
                if next_file_number < min_next {
                    return Err(corrupt(format!(
                        "manifest next_file_number {next_file_number} is below live minimum {min_next}"
                    )));
                }
                self.next_file_number = next_file_number;
                Ok(())
            }
            ManifestEdit::SetHighWatermark(high_watermark) => {
                if high_watermark < self.high_watermark {
                    return Err(corrupt(format!(
                        "manifest high_watermark decreased from {} to {high_watermark}",
                        self.high_watermark
                    )));
                }
                let min_high_watermark = self.min_high_watermark();
                if high_watermark < min_high_watermark {
                    return Err(corrupt(format!(
                        "manifest high_watermark {high_watermark} is below live SSTable max_ts {min_high_watermark}"
                    )));
                }
                self.high_watermark = high_watermark;
                Ok(())
            }
            ManifestEdit::SetWalReplayFloor(floor) => {
                if self
                    .wal_replay_floor
                    .is_some_and(|previous| floor < previous)
                {
                    return Err(corrupt("manifest WAL replay floor decreased"));
                }
                self.wal_replay_floor = Some(floor);
                Ok(())
            }
            ManifestEdit::VersionEdit {
                delete_ids,
                add_files,
                next_file_number,
                high_watermark,
                wal_replay_floor,
            } => self.apply_version_edit(
                delete_ids,
                add_files,
                next_file_number,
                high_watermark,
                wal_replay_floor,
            ),
        }
    }

    fn apply_version_edit(
        &mut self,
        delete_ids: Vec<u64>,
        add_files: Vec<ManifestSstableEntry>,
        next_file_number: Option<u64>,
        high_watermark: Option<u64>,
        wal_replay_floor: Option<ManifestWalReplayFloor>,
    ) -> Result<()> {
        for entry in &add_files {
            validate_sstable_entry(entry)?;
        }
        if high_watermark.is_some_and(|value| value < self.high_watermark) {
            return Err(corrupt(format!(
                "manifest high_watermark decreased from {} to {}",
                self.high_watermark,
                high_watermark.unwrap()
            )));
        }
        if self
            .wal_replay_floor
            .is_some_and(|previous| wal_replay_floor.is_some_and(|floor| floor < previous))
        {
            return Err(corrupt("manifest WAL replay floor decreased"));
        }

        let previous = self.clone();
        let mut seen_deletes = BTreeSet::new();
        for id in delete_ids {
            if !seen_deletes.insert(id) {
                *self = previous;
                return Err(corrupt(format!(
                    "manifest VersionEdit duplicates delete SSTable {id}"
                )));
            }
            if self.files.remove(&id).is_none() {
                *self = previous;
                return Err(corrupt(format!(
                    "manifest VersionEdit references missing SSTable {id}"
                )));
            }
        }

        let mut min_next_file_number = self.next_file_number;
        for entry in add_files {
            let entry_id = entry.id;
            let entry_next_file_number = entry_id.saturating_add(1);
            if self.files.insert(entry_id, entry).is_some() {
                *self = previous;
                return Err(corrupt(format!(
                    "manifest VersionEdit duplicates live SSTable {entry_id}"
                )));
            }
            if min_next_file_number < entry_next_file_number {
                min_next_file_number = entry_next_file_number;
            }
        }

        self.next_file_number = next_file_number.unwrap_or(min_next_file_number);
        if let Some(high_watermark) = high_watermark {
            self.high_watermark = high_watermark;
        }
        if let Some(wal_replay_floor) = wal_replay_floor {
            self.wal_replay_floor = Some(wal_replay_floor);
        }

        if let Err(error) = self.validate_invariants() {
            *self = previous;
            return Err(error);
        }
        Ok(())
    }

    fn validate_next_file_number(&self) -> Result<()> {
        let min_next = self.min_next_file_number();
        if self.next_file_number < min_next {
            return Err(corrupt(format!(
                "manifest snapshot next_file_number {} is below live minimum {min_next}",
                self.next_file_number
            )));
        }
        Ok(())
    }

    fn validate_invariants(&self) -> Result<()> {
        self.validate_next_file_number()?;
        let min_high_watermark = self.min_high_watermark();
        if self.high_watermark < min_high_watermark {
            return Err(corrupt(format!(
                "manifest high_watermark {} is below live SSTable max_ts {min_high_watermark}",
                self.high_watermark
            )));
        }
        Ok(())
    }

    fn min_next_file_number(&self) -> u64 {
        self.files
            .keys()
            .next_back()
            .map(|id| id.saturating_add(1))
            .unwrap_or(1)
    }

    fn min_high_watermark(&self) -> u64 {
        self.files
            .values()
            .map(|entry| entry.max_ts)
            .max()
            .unwrap_or(0)
    }
}

pub fn write_manifest_edit<W: Write>(
    writer: &mut ManifestRecordWriter<W>,
    edit: &ManifestEdit,
) -> Result<ManifestRecordOffset> {
    writer.write_record(&edit.encode()?)
}

pub fn replay_manifest_edits<R: Read>(
    reader: ManifestRecordReader<R>,
) -> Result<ManifestEditReplay> {
    let ManifestRecordReplay {
        records,
        valid_bytes,
        recovered_tail,
    } = reader.read_all()?;
    let mut state = ManifestVersionState::default();
    let mut edits = Vec::with_capacity(records.len());
    for record in records {
        let edit = ManifestEdit::decode(&record)?;
        state.apply(edit.clone())?;
        edits.push(edit);
    }
    state.validate_invariants()?;
    Ok(ManifestEditReplay {
        edits,
        state,
        valid_bytes,
        recovered_tail,
    })
}

fn write_entries(out: &mut Vec<u8>, entries: &[ManifestSstableEntry]) -> Result<()> {
    write_u32(out, checked_count(entries.len())?);
    for entry in entries {
        write_entry(out, entry)?;
    }
    Ok(())
}

fn write_entry(out: &mut Vec<u8>, entry: &ManifestSstableEntry) -> Result<()> {
    validate_sstable_entry(entry)?;
    if entry.file_name.len() > MANIFEST_EDIT_MAX_FILE_NAME_BYTES {
        return Err(corrupt("manifest SSTable file_name is too long"));
    }
    write_u64(out, entry.id);
    write_string(out, &entry.file_name)?;
    write_u64(out, entry.fingerprint.file_len);
    write_u64(out, entry.fingerprint.modified_unix_secs);
    write_u32(out, entry.fingerprint.modified_subsec_nanos);
    write_bytes(out, &entry.first_key, MANIFEST_EDIT_MAX_KEY_BYTES)?;
    write_bytes(out, &entry.last_key, MANIFEST_EDIT_MAX_KEY_BYTES)?;
    write_u32(out, entry.format_version);
    write_u64(out, entry.max_ts);
    write_u64(out, entry.content_fingerprint);
    Ok(())
}

fn write_optional_wal_floor(out: &mut Vec<u8>, floor: Option<ManifestWalReplayFloor>) {
    match floor {
        Some(floor) => {
            out.push(1);
            write_wal_floor(out, floor);
        }
        None => out.push(0),
    }
}

fn write_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.push(1);
            write_u64(out, value);
        }
        None => out.push(0),
    }
}

fn write_wal_floor(out: &mut Vec<u8>, floor: ManifestWalReplayFloor) {
    write_u64(out, floor.wal_generation);
    write_u64(out, floor.segment_id);
    write_u64(out, floor.offset);
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    write_bytes(out, bytes, MANIFEST_EDIT_MAX_FILE_NAME_BYTES)
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8], max_len: usize) -> Result<()> {
    if bytes.len() > max_len {
        return Err(corrupt("manifest edit byte field exceeds maximum length"));
    }
    write_u32(out, checked_count(bytes.len())?);
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn validate_sstable_entry(entry: &ManifestSstableEntry) -> Result<()> {
    if entry.file_name.is_empty()
        || entry.file_name.contains('/')
        || entry.file_name.contains('\\')
        || entry.file_name.contains("..")
        || entry.file_name.contains('\n')
        || entry.file_name.contains('\r')
    {
        return Err(corrupt("manifest SSTable file_name must be a base name"));
    }
    let expected = format!("{}.sst", entry.id);
    if entry.file_name != expected {
        return Err(corrupt(format!(
            "manifest SSTable file_name {} does not match id {}; expected {expected}",
            entry.file_name, entry.id
        )));
    }
    Ok(())
}

fn checked_count(count: usize) -> Result<u32> {
    if count > u32::MAX as usize {
        return Err(corrupt("manifest edit count exceeds u32"));
    }
    Ok(count as u32)
}

struct EditBytes<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> EditBytes<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self, expected: &[u8]) -> Result<()> {
        let actual = self.read_exact(expected.len())?;
        if actual != expected {
            return Err(corrupt("manifest edit magic mismatch"));
        }
        Ok(())
    }

    fn read_entry(&mut self) -> Result<ManifestSstableEntry> {
        let id = self.read_u64()?;
        let file_name = self.read_string()?;
        let file_len = self.read_u64()?;
        let modified_unix_secs = self.read_u64()?;
        let modified_subsec_nanos = self.read_u32()?;
        let first_key = self.read_bytes(MANIFEST_EDIT_MAX_KEY_BYTES, "first_key")?;
        let last_key = self.read_bytes(MANIFEST_EDIT_MAX_KEY_BYTES, "last_key")?;
        let format_version = self.read_u32()?;
        let max_ts = self.read_u64()?;
        let content_fingerprint = self.read_u64()?;
        let entry = ManifestSstableEntry {
            id,
            file_name,
            fingerprint: ManifestSstableFingerprint {
                file_len,
                modified_unix_secs,
                modified_subsec_nanos,
            },
            first_key,
            last_key,
            format_version,
            max_ts,
            content_fingerprint,
        };
        validate_sstable_entry(&entry)?;
        Ok(entry)
    }

    fn read_entries(&mut self) -> Result<Vec<ManifestSstableEntry>> {
        let count = self.read_count("manifest snapshot files")?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(self.read_entry()?);
        }
        Ok(entries)
    }

    fn read_optional_wal_floor(&mut self) -> Result<Option<ManifestWalReplayFloor>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_wal_floor()?)),
            other => Err(corrupt(format!(
                "invalid manifest optional WAL floor tag {other}"
            ))),
        }
    }

    fn read_wal_floor(&mut self) -> Result<ManifestWalReplayFloor> {
        Ok(ManifestWalReplayFloor {
            wal_generation: self.read_u64()?,
            segment_id: self.read_u64()?,
            offset: self.read_u64()?,
        })
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            other => Err(corrupt(format!(
                "invalid manifest optional u64 tag {other}"
            ))),
        }
    }

    fn read_string(&mut self) -> Result<String> {
        let bytes = self.read_bytes(MANIFEST_EDIT_MAX_FILE_NAME_BYTES, "manifest string")?;
        String::from_utf8(bytes).map_err(|error| corrupt(format!("manifest UTF-8 error: {error}")))
    }

    fn read_bytes(&mut self, max_len: usize, label: &str) -> Result<Vec<u8>> {
        let len = self.read_count(label)?;
        if len > max_len {
            return Err(corrupt(format!("{label} exceeds maximum length")));
        }
        Ok(self.read_exact(len)?.to_vec())
    }

    fn read_count(&mut self, label: &str) -> Result<usize> {
        let count = self.read_u32()? as usize;
        if count > MANIFEST_EDIT_MAX_FILE_COUNT {
            return Err(corrupt(format!("{label} count {count} exceeds maximum")));
        }
        Ok(count)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.read_exact(N)?);
        Ok(out)
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| corrupt("manifest edit offset overflow"))?;
        if end > self.bytes.len() {
            return Err(corrupt("manifest edit payload truncated"));
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn finish(&self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(corrupt("manifest edit payload has trailing bytes"));
        }
        Ok(())
    }
}

fn corrupt(message: impl Into<String>) -> FusionError {
    FusionError::Storage(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::manifest_record::ManifestRecordReader;

    fn entry(id: u64) -> ManifestSstableEntry {
        ManifestSstableEntry {
            id,
            file_name: format!("{id}.sst"),
            fingerprint: ManifestSstableFingerprint {
                file_len: id * 100,
                modified_unix_secs: id * 10,
                modified_subsec_nanos: id as u32,
            },
            first_key: format!("key-{id:04}-first").into_bytes(),
            last_key: format!("key-{id:04}-last").into_bytes(),
            format_version: 3,
            max_ts: id,
            content_fingerprint: id * 1000,
        }
    }

    fn raw_add_sstable_edit(id: u64, file_name: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MANIFEST_EDIT_MAGIC);
        out.extend_from_slice(&MANIFEST_EDIT_VERSION.to_le_bytes());
        out.push(ManifestEditTag::AddSstable as u8);
        write_u64(&mut out, id);
        write_string(&mut out, file_name).unwrap();
        write_u64(&mut out, id * 100);
        write_u64(&mut out, id * 10);
        write_u32(&mut out, id as u32);
        write_bytes(
            &mut out,
            format!("key-{id:04}-first").as_bytes(),
            MANIFEST_EDIT_MAX_KEY_BYTES,
        )
        .unwrap();
        write_bytes(
            &mut out,
            format!("key-{id:04}-last").as_bytes(),
            MANIFEST_EDIT_MAX_KEY_BYTES,
        )
        .unwrap();
        write_u32(&mut out, 3);
        write_u64(&mut out, id);
        write_u64(&mut out, id * 1000);
        out
    }

    fn round_trip(edit: ManifestEdit) {
        let decoded = ManifestEdit::decode(&edit.encode().unwrap()).unwrap();
        assert_eq!(decoded, edit);
    }

    #[test]
    fn manifest_edit_round_trips_all_variants() {
        round_trip(ManifestEdit::Snapshot {
            files: vec![entry(1), entry(2)],
            next_file_number: 3,
            high_watermark: 42,
            wal_replay_floor: Some(ManifestWalReplayFloor {
                wal_generation: 1,
                segment_id: 7,
                offset: 11,
            }),
        });
        round_trip(ManifestEdit::AddSstable(entry(3)));
        round_trip(ManifestEdit::DeleteSstable { id: 2 });
        round_trip(ManifestEdit::Compact {
            delete_ids: vec![1, 2],
            add: entry(4),
        });
        round_trip(ManifestEdit::SetNextFileNumber(9));
        round_trip(ManifestEdit::SetHighWatermark(99));
        round_trip(ManifestEdit::SetWalReplayFloor(ManifestWalReplayFloor {
            wal_generation: 9,
            segment_id: 2,
            offset: 4096,
        }));
        round_trip(ManifestEdit::VersionEdit {
            delete_ids: vec![1, 2],
            add_files: vec![entry(8), entry(9)],
            next_file_number: Some(10),
            high_watermark: Some(99),
            wal_replay_floor: Some(ManifestWalReplayFloor {
                wal_generation: 10,
                segment_id: 3,
                offset: 8192,
            }),
        });
    }

    #[test]
    fn manifest_edit_replays_records_into_state() {
        let edits = vec![
            ManifestEdit::Snapshot {
                files: vec![entry(1), entry(2)],
                next_file_number: 3,
                high_watermark: 10,
                wal_replay_floor: None,
            },
            ManifestEdit::AddSstable(entry(3)),
            ManifestEdit::SetHighWatermark(12),
            ManifestEdit::SetWalReplayFloor(ManifestWalReplayFloor {
                wal_generation: 1,
                segment_id: 1,
                offset: 128,
            }),
            ManifestEdit::Compact {
                delete_ids: vec![1, 2],
                add: entry(4),
            },
            ManifestEdit::SetNextFileNumber(10),
        ];
        let mut bytes = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut bytes);
            for edit in &edits {
                write_manifest_edit(&mut writer, edit).unwrap();
            }
        }

        let replay = replay_manifest_edits(ManifestRecordReader::new(bytes.as_slice())).unwrap();
        assert_eq!(replay.edits, edits);
        assert_eq!(replay.valid_bytes, bytes.len() as u64);
        assert!(!replay.recovered_tail);
        assert_eq!(
            replay.state.files.keys().copied().collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert_eq!(replay.state.next_file_number, 10);
        assert_eq!(replay.state.high_watermark, 12);
        assert_eq!(
            replay.state.wal_replay_floor,
            Some(ManifestWalReplayFloor {
                wal_generation: 1,
                segment_id: 1,
                offset: 128,
            })
        );
    }

    #[test]
    fn manifest_edit_version_edit_atomically_applies_metadata_and_file_delta() {
        let edits = vec![
            ManifestEdit::Snapshot {
                files: vec![entry(1), entry(2), entry(3)],
                next_file_number: 4,
                high_watermark: 3,
                wal_replay_floor: None,
            },
            ManifestEdit::VersionEdit {
                delete_ids: vec![1, 2],
                add_files: vec![entry(10)],
                next_file_number: Some(11),
                high_watermark: Some(10),
                wal_replay_floor: Some(ManifestWalReplayFloor {
                    wal_generation: 1,
                    segment_id: 2,
                    offset: 4096,
                }),
            },
        ];
        let mut bytes = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut bytes);
            for edit in &edits {
                write_manifest_edit(&mut writer, edit).unwrap();
            }
        }

        let replay = replay_manifest_edits(ManifestRecordReader::new(bytes.as_slice())).unwrap();
        assert_eq!(replay.edits, edits);
        assert_eq!(
            replay.state.files.keys().copied().collect::<Vec<_>>(),
            vec![3, 10]
        );
        assert_eq!(replay.state.next_file_number, 11);
        assert_eq!(replay.state.high_watermark, 10);
        assert_eq!(
            replay.state.wal_replay_floor,
            Some(ManifestWalReplayFloor {
                wal_generation: 1,
                segment_id: 2,
                offset: 4096,
            })
        );
    }

    #[test]
    fn manifest_edit_replay_recovers_prefix_on_torn_tail() {
        let mut bytes = Vec::new();
        let first_end = {
            let mut writer = ManifestRecordWriter::new(&mut bytes);
            write_manifest_edit(
                &mut writer,
                &ManifestEdit::Snapshot {
                    files: vec![entry(1)],
                    next_file_number: 2,
                    high_watermark: 1,
                    wal_replay_floor: None,
                },
            )
            .unwrap()
            .end
        };
        {
            let mut writer = ManifestRecordWriter::new_appending(&mut bytes, first_end);
            write_manifest_edit(&mut writer, &ManifestEdit::AddSstable(entry(2))).unwrap();
        }
        bytes.truncate(bytes.len() - 3);

        let replay = replay_manifest_edits(ManifestRecordReader::new(bytes.as_slice())).unwrap();
        assert!(replay.recovered_tail);
        assert_eq!(replay.valid_bytes, first_end);
        assert_eq!(
            replay.state.files.keys().copied().collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn manifest_edit_decode_rejects_unknown_version_and_tag() {
        let mut bytes = ManifestEdit::SetNextFileNumber(7).encode().unwrap();
        bytes[4] = 2;
        assert!(ManifestEdit::decode(&bytes)
            .unwrap_err()
            .to_string()
            .contains("unsupported manifest edit version"));

        let mut bytes = ManifestEdit::SetNextFileNumber(7).encode().unwrap();
        bytes[6] = 255;
        assert!(ManifestEdit::decode(&bytes)
            .unwrap_err()
            .to_string()
            .contains("unknown manifest edit tag"));
    }

    #[test]
    fn manifest_edit_decode_rejects_truncated_and_trailing_payloads() {
        let mut bytes = ManifestEdit::SetHighWatermark(9).encode().unwrap();
        bytes.pop();
        assert!(ManifestEdit::decode(&bytes)
            .unwrap_err()
            .to_string()
            .contains("truncated"));

        let mut bytes = ManifestEdit::SetHighWatermark(9).encode().unwrap();
        bytes.push(0);
        assert!(ManifestEdit::decode(&bytes)
            .unwrap_err()
            .to_string()
            .contains("trailing bytes"));
    }

    #[test]
    fn manifest_edit_rejects_non_canonical_sstable_file_names() {
        let mut bad_path = entry(7);
        bad_path.file_name = "../7.sst".to_string();
        assert!(ManifestEdit::AddSstable(bad_path.clone())
            .encode()
            .unwrap_err()
            .to_string()
            .contains("base name"));

        assert!(ManifestVersionState::default()
            .apply(ManifestEdit::AddSstable(bad_path))
            .unwrap_err()
            .to_string()
            .contains("base name"));

        let mut mismatched = entry(7);
        mismatched.file_name = "8.sst".to_string();
        assert!(ManifestEdit::Compact {
            delete_ids: Vec::new(),
            add: mismatched
        }
        .encode()
        .unwrap_err()
        .to_string()
        .contains("does not match id"));

        let invalid_disk_edit = raw_add_sstable_edit(7, "nested/7.sst");
        assert!(ManifestEdit::decode(&invalid_disk_edit)
            .unwrap_err()
            .to_string()
            .contains("base name"));

        let invalid_disk_edit = raw_add_sstable_edit(7, "8.sst");
        assert!(ManifestEdit::decode(&invalid_disk_edit)
            .unwrap_err()
            .to_string()
            .contains("does not match id"));
    }

    #[test]
    fn manifest_edit_replay_rejects_non_canonical_sstable_file_name_records() {
        let mut bytes = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut bytes);
            writer
                .write_record(&raw_add_sstable_edit(7, "8.sst"))
                .unwrap();
        }

        assert!(
            replay_manifest_edits(ManifestRecordReader::new(bytes.as_slice()))
                .unwrap_err()
                .to_string()
                .contains("does not match id")
        );
    }

    #[test]
    fn manifest_edit_state_rejects_duplicate_and_missing_sstable_edits() {
        let mut state = ManifestVersionState::default();
        state.apply(ManifestEdit::SetHighWatermark(1)).unwrap();
        state.apply(ManifestEdit::AddSstable(entry(1))).unwrap();
        assert!(state
            .apply(ManifestEdit::AddSstable(entry(1)))
            .unwrap_err()
            .to_string()
            .contains("duplicates live SSTable"));
        assert!(state
            .apply(ManifestEdit::DeleteSstable { id: 99 })
            .unwrap_err()
            .to_string()
            .contains("missing SSTable"));
    }

    #[test]
    fn manifest_edit_state_rejects_regressions() {
        let mut state = ManifestVersionState::default();
        state.apply(ManifestEdit::SetHighWatermark(5)).unwrap();
        state.apply(ManifestEdit::AddSstable(entry(5))).unwrap();
        assert!(state
            .apply(ManifestEdit::SetNextFileNumber(5))
            .unwrap_err()
            .to_string()
            .contains("below live minimum"));

        state.apply(ManifestEdit::SetHighWatermark(10)).unwrap();
        assert!(state
            .apply(ManifestEdit::SetHighWatermark(9))
            .unwrap_err()
            .to_string()
            .contains("decreased"));

        state
            .apply(ManifestEdit::SetWalReplayFloor(ManifestWalReplayFloor {
                wal_generation: 1,
                segment_id: 3,
                offset: 100,
            }))
            .unwrap();
        assert!(state
            .apply(ManifestEdit::SetWalReplayFloor(ManifestWalReplayFloor {
                wal_generation: 0,
                segment_id: 2,
                offset: 999,
            }))
            .unwrap_err()
            .to_string()
            .contains("decreased"));
    }

    #[test]
    fn manifest_edit_state_rejects_high_watermark_below_live_sstable_max_ts() {
        let mut state = ManifestVersionState::default();
        assert!(state
            .apply(ManifestEdit::AddSstable(entry(10)))
            .unwrap_err()
            .to_string()
            .contains("below live SSTable max_ts"));
        assert!(state.files.is_empty());
        assert_eq!(state.next_file_number, 0);

        let mut bytes = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut bytes);
            write_manifest_edit(
                &mut writer,
                &ManifestEdit::Snapshot {
                    files: vec![entry(10)],
                    next_file_number: 11,
                    high_watermark: 9,
                    wal_replay_floor: None,
                },
            )
            .unwrap();
        }
        assert!(
            replay_manifest_edits(ManifestRecordReader::new(bytes.as_slice()))
                .unwrap_err()
                .to_string()
                .contains("below live SSTable max_ts")
        );
    }

    #[test]
    fn manifest_edit_replay_rejects_add_before_high_watermark_prefix() {
        let edits = [
            ManifestEdit::Snapshot {
                files: Vec::new(),
                next_file_number: 1,
                high_watermark: 0,
                wal_replay_floor: None,
            },
            ManifestEdit::AddSstable(entry(10)),
            ManifestEdit::SetHighWatermark(10),
        ];
        let mut bytes = Vec::new();
        {
            let mut writer = ManifestRecordWriter::new(&mut bytes);
            for edit in &edits {
                write_manifest_edit(&mut writer, edit).unwrap();
            }
        }

        assert!(
            replay_manifest_edits(ManifestRecordReader::new(bytes.as_slice()))
                .unwrap_err()
                .to_string()
                .contains("below live SSTable max_ts")
        );
    }

    #[test]
    fn manifest_edit_version_edit_rolls_back_on_invariant_failure() {
        let mut state = ManifestVersionState::default();
        state
            .apply(ManifestEdit::Snapshot {
                files: vec![entry(1), entry(2)],
                next_file_number: 3,
                high_watermark: 2,
                wal_replay_floor: None,
            })
            .unwrap();
        let previous = state.clone();

        assert!(state
            .apply(ManifestEdit::VersionEdit {
                delete_ids: vec![1],
                add_files: vec![entry(10)],
                next_file_number: Some(11),
                high_watermark: None,
                wal_replay_floor: None,
            })
            .unwrap_err()
            .to_string()
            .contains("below live SSTable max_ts"));
        assert_eq!(state, previous);
    }

    #[test]
    fn manifest_edit_snapshot_rejects_duplicate_ids_and_low_next_file_number() {
        let duplicate = ManifestEdit::Snapshot {
            files: vec![entry(1), entry(1)],
            next_file_number: 2,
            high_watermark: 0,
            wal_replay_floor: None,
        };
        assert!(ManifestVersionState::default()
            .apply(duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate SSTable id"));

        let low_next = ManifestEdit::Snapshot {
            files: vec![entry(10)],
            next_file_number: 10,
            high_watermark: 10,
            wal_replay_floor: None,
        };
        assert!(ManifestVersionState::default()
            .apply(low_next)
            .unwrap_err()
            .to_string()
            .contains("below live minimum"));
    }
}

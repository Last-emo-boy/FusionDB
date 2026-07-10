#![allow(dead_code)]

use super::manifest_edit::{
    replay_manifest_edits, write_manifest_edit, ManifestEdit, ManifestEditReplay,
    ManifestVersionState,
};
use super::manifest_record::{ManifestRecordOffset, ManifestRecordReader, ManifestRecordWriter};
use crate::common::{FusionError, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const MANIFEST_CURRENT_FILE: &str = "CURRENT";
pub const MANIFEST_FILE_PREFIX: &str = "MANIFEST-";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestCurrent {
    pub file_number: u64,
    pub file_name: String,
    pub path: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ManifestLogReplay {
    pub current: Option<ManifestCurrent>,
    pub edit_replay: ManifestEditReplay,
}

pub fn manifest_file_name(file_number: u64) -> String {
    format!("{MANIFEST_FILE_PREFIX}{file_number:06}")
}

pub fn parse_manifest_file_name(file_name: &str) -> Option<u64> {
    let suffix = file_name.strip_prefix(MANIFEST_FILE_PREFIX)?;
    if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let file_number = suffix.parse::<u64>().ok()?;
    if manifest_file_name(file_number) != file_name {
        return None;
    }
    Some(file_number)
}

pub fn current_path(manifest_dir: &Path) -> PathBuf {
    manifest_dir.join(MANIFEST_CURRENT_FILE)
}

pub fn manifest_path(manifest_dir: &Path, file_number: u64) -> PathBuf {
    manifest_dir.join(manifest_file_name(file_number))
}

pub fn write_manifest_file(
    manifest_dir: &Path,
    file_number: u64,
    first_edit: &ManifestEdit,
) -> Result<ManifestRecordOffset> {
    if !matches!(first_edit, ManifestEdit::Snapshot { .. }) {
        return Err(corrupt("new manifest file must start with Snapshot edit"));
    }
    fs::create_dir_all(manifest_dir).map_err(FusionError::Io)?;
    let file_name = manifest_file_name(file_number);
    let final_path = manifest_dir.join(&file_name);
    if final_path.exists() {
        return Err(corrupt(format!(
            "manifest file already exists: {}",
            final_path.display()
        )));
    }
    let tmp_path = manifest_dir.join(unique_tmp_name(&file_name));

    let write_result = (|| -> Result<ManifestRecordOffset> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(FusionError::Io)?;
        let offset = {
            let mut writer = ManifestRecordWriter::new(&mut file);
            let offset = write_manifest_edit(&mut writer, first_edit)?;
            writer.flush()?;
            offset
        };
        file.sync_all().map_err(FusionError::Io)?;
        fs::rename(&tmp_path, &final_path).map_err(FusionError::Io)?;
        sync_dir(manifest_dir)?;
        Ok(offset)
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result
}

pub fn append_manifest_edit_file(
    manifest_path: &Path,
    edit: &ManifestEdit,
) -> Result<ManifestRecordOffset> {
    let existing_len = recover_manifest_tail_for_append(manifest_path)?;
    let mut file = OpenOptions::new()
        .append(true)
        .open(manifest_path)
        .map_err(FusionError::Io)?;
    let offset = {
        let mut writer = ManifestRecordWriter::new_appending(&mut file, existing_len);
        let offset = write_manifest_edit(&mut writer, edit)?;
        writer.flush()?;
        offset
    };
    file.sync_all().map_err(FusionError::Io)?;
    Ok(offset)
}

fn recover_manifest_tail_for_append(manifest_path: &Path) -> Result<u64> {
    let replay = replay_manifest_path(manifest_path)?;
    if !replay.recovered_tail {
        return Ok(fs::metadata(manifest_path).map_err(FusionError::Io)?.len());
    }

    let file = OpenOptions::new()
        .write(true)
        .open(manifest_path)
        .map_err(FusionError::Io)?;
    file.set_len(replay.valid_bytes).map_err(FusionError::Io)?;
    file.sync_all().map_err(FusionError::Io)?;
    Ok(replay.valid_bytes)
}

pub fn install_current_file(manifest_dir: &Path, file_name: &str) -> Result<ManifestCurrent> {
    let file_number = validate_manifest_file_name(file_name)?;
    fs::create_dir_all(manifest_dir).map_err(FusionError::Io)?;
    let manifest_path = manifest_dir.join(file_name);
    if !manifest_path.exists() {
        return Err(corrupt(format!(
            "CURRENT target manifest does not exist: {}",
            manifest_path.display()
        )));
    }

    let tmp_path = manifest_dir.join(unique_tmp_name(MANIFEST_CURRENT_FILE));
    let write_result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(FusionError::Io)?;
        file.write_all(file_name.as_bytes())
            .map_err(FusionError::Io)?;
        file.write_all(b"\n").map_err(FusionError::Io)?;
        file.sync_all().map_err(FusionError::Io)?;
        fs::rename(&tmp_path, current_path(manifest_dir)).map_err(FusionError::Io)?;
        sync_dir(manifest_dir)
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result?;

    Ok(ManifestCurrent {
        file_number,
        file_name: file_name.to_string(),
        path: manifest_path,
    })
}

pub fn read_current_file(manifest_dir: &Path) -> Result<ManifestCurrent> {
    let path = current_path(manifest_dir);
    let contents = fs::read_to_string(&path).map_err(FusionError::Io)?;
    let file_name = parse_current_file_contents(&contents)?.to_string();
    let file_number = validate_manifest_file_name(&file_name)?;
    let manifest_path = manifest_dir.join(&file_name);
    if !manifest_path.exists() {
        return Err(corrupt(format!(
            "CURRENT target manifest does not exist: {}",
            manifest_path.display()
        )));
    }
    Ok(ManifestCurrent {
        file_number,
        path: manifest_path,
        file_name,
    })
}

pub fn replay_current_manifest(manifest_dir: &Path) -> Result<ManifestLogReplay> {
    let current = read_current_file(manifest_dir)?;
    let edit_replay = replay_manifest_path(&current.path)?;
    Ok(ManifestLogReplay {
        current: Some(current),
        edit_replay,
    })
}

pub fn recover_current_manifest_with_rollover(manifest_dir: &Path) -> Result<ManifestLogReplay> {
    let replay = replay_current_manifest(manifest_dir)?;
    if !replay.edit_replay.recovered_tail {
        return Ok(replay);
    }

    let current = replay
        .current
        .as_ref()
        .ok_or_else(|| corrupt("manifest recovery requires CURRENT"))?;
    let new_file_number = next_available_manifest_file_number(manifest_dir, current.file_number)?;
    let snapshot = snapshot_edit_from_state(&replay.edit_replay.state);
    write_manifest_file(manifest_dir, new_file_number, &snapshot)?;
    install_current_file(manifest_dir, &manifest_file_name(new_file_number))?;
    replay_current_manifest(manifest_dir)
}

pub fn replay_manifest_path(path: &Path) -> Result<ManifestEditReplay> {
    let file = File::open(path).map_err(FusionError::Io)?;
    if file.metadata().map_err(FusionError::Io)?.len() == 0 {
        return Err(corrupt("manifest file is empty"));
    }
    let replay = replay_manifest_edits(ManifestRecordReader::new(file))?;
    validate_manifest_replay_starts_with_snapshot(&replay)?;
    Ok(replay)
}

fn validate_manifest_replay_starts_with_snapshot(replay: &ManifestEditReplay) -> Result<()> {
    match replay.edits.first() {
        Some(ManifestEdit::Snapshot { .. }) => Ok(()),
        Some(_) => Err(corrupt("manifest file must start with Snapshot edit")),
        None => Err(corrupt("manifest file is empty")),
    }
}

fn snapshot_edit_from_state(state: &ManifestVersionState) -> ManifestEdit {
    ManifestEdit::Snapshot {
        files: state.files.values().cloned().collect(),
        next_file_number: state.next_file_number,
        high_watermark: state.high_watermark,
        wal_replay_floor: state.wal_replay_floor,
    }
}

fn next_available_manifest_file_number(
    manifest_dir: &Path,
    current_file_number: u64,
) -> Result<u64> {
    let mut candidate = current_file_number
        .checked_add(1)
        .ok_or_else(|| corrupt("manifest file number overflow"))?;
    loop {
        if !manifest_path(manifest_dir, candidate).exists() {
            return Ok(candidate);
        }
        candidate = candidate
            .checked_add(1)
            .ok_or_else(|| corrupt("manifest file number overflow"))?;
    }
}

fn validate_manifest_file_name(file_name: &str) -> Result<u64> {
    if file_name.contains('/')
        || file_name.contains('\\')
        || file_name.contains("..")
        || file_name.contains('\n')
        || file_name.contains('\r')
    {
        return Err(corrupt("manifest file name must be a base file name"));
    }
    parse_manifest_file_name(file_name)
        .ok_or_else(|| corrupt(format!("invalid manifest file name: {file_name}")))
}

fn parse_current_file_contents(contents: &str) -> Result<&str> {
    let line = if let Some(line) = contents.strip_suffix("\r\n") {
        line
    } else if let Some(line) = contents.strip_suffix('\n') {
        line
    } else {
        contents
    };
    if line.is_empty() {
        return Err(corrupt("CURRENT file is empty"));
    }
    if line.contains('\n') || line.contains('\r') {
        return Err(corrupt(
            "CURRENT file must contain exactly one manifest name",
        ));
    }
    Ok(line)
}

fn unique_tmp_name(base_name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{base_name}.{}.{}.tmp", std::process::id(), nanos)
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(FusionError::Io)
}

fn corrupt(message: impl Into<String>) -> FusionError {
    FusionError::Storage(message.into())
}

#[cfg(test)]
mod tests {
    use super::super::manifest_edit::{
        ManifestSstableEntry, ManifestSstableFingerprint, ManifestWalReplayFloor,
    };
    use super::*;
    use std::io::{Read, Seek, SeekFrom};

    fn unique_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fusiondb_manifest_log_{}_{}_{}",
            name,
            std::process::id(),
            unique_tmp_name("case")
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cleanup_dir(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }

    fn corrupt_byte(path: &Path, offset: u64) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xff;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
    }

    fn write_raw_manifest_edit_file(path: &Path, edit: &ManifestEdit) {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        {
            let mut writer = ManifestRecordWriter::new(&mut file);
            write_manifest_edit(&mut writer, edit).unwrap();
            writer.flush().unwrap();
        }
        file.sync_all().unwrap();
    }

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

    fn snapshot(files: Vec<ManifestSstableEntry>, next_file_number: u64) -> ManifestEdit {
        ManifestEdit::Snapshot {
            files,
            next_file_number,
            high_watermark: next_file_number,
            wal_replay_floor: Some(ManifestWalReplayFloor {
                wal_generation: 1,
                segment_id: 0,
                offset: 0,
            }),
        }
    }

    #[test]
    fn manifest_log_file_names_parse_and_reject_paths() {
        assert_eq!(manifest_file_name(1), "MANIFEST-000001");
        assert_eq!(parse_manifest_file_name("MANIFEST-000123"), Some(123));
        assert_eq!(parse_manifest_file_name("MANIFEST-1"), None);
        assert_eq!(parse_manifest_file_name("MANIFEST-0000010"), None);
        assert_eq!(parse_manifest_file_name("MANIFEST-"), None);
        assert_eq!(parse_manifest_file_name("MANIFEST-abc"), None);
        assert!(validate_manifest_file_name("../MANIFEST-000001").is_err());
        assert!(validate_manifest_file_name("nested/MANIFEST-000001").is_err());
    }

    #[test]
    fn manifest_log_writes_current_and_replays_manifest() {
        let dir = unique_dir("current_replay");
        let first_edit = snapshot(vec![entry(1)], 2);
        let offset = write_manifest_file(&dir, 1, &first_edit).unwrap();
        assert!(offset.end > 0);

        let current = install_current_file(&dir, &manifest_file_name(1)).unwrap();
        assert_eq!(current.file_number, 1);
        assert_eq!(read_current_file(&dir).unwrap(), current);

        let replay = replay_current_manifest(&dir).unwrap();
        assert_eq!(replay.current, Some(current));
        assert_eq!(
            replay
                .edit_replay
                .state
                .files
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(replay.edit_replay.state.next_file_number, 2);
        assert!(!replay.edit_replay.recovered_tail);

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_current_prefers_pointed_manifest_over_newer_orphan() {
        let dir = unique_dir("current_prefers_old");
        write_manifest_file(&dir, 1, &snapshot(vec![entry(1)], 2)).unwrap();
        write_manifest_file(&dir, 2, &snapshot(vec![entry(2)], 3)).unwrap();
        let current = install_current_file(&dir, &manifest_file_name(1)).unwrap();

        let replay = replay_current_manifest(&dir).unwrap();
        assert_eq!(replay.current, Some(current));
        assert_eq!(
            replay
                .edit_replay
                .state
                .files
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![1]
        );

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_ignores_current_tmp_when_current_points_old_manifest() {
        let dir = unique_dir("current_tmp");
        write_manifest_file(&dir, 1, &snapshot(vec![entry(1)], 2)).unwrap();
        write_manifest_file(&dir, 2, &snapshot(vec![entry(2)], 3)).unwrap();
        install_current_file(&dir, &manifest_file_name(1)).unwrap();
        fs::write(
            dir.join("CURRENT.tmp"),
            format!("{}\n", manifest_file_name(2)),
        )
        .unwrap();
        fs::write(
            dir.join(unique_tmp_name(MANIFEST_CURRENT_FILE)),
            format!("{}\n", manifest_file_name(2)),
        )
        .unwrap();

        let replay = replay_current_manifest(&dir).unwrap();
        assert_eq!(replay.current.as_ref().unwrap().file_number, 1);
        assert_eq!(
            replay
                .edit_replay
                .state
                .files
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![1]
        );

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_appends_edit_and_syncs_manifest_file() {
        let dir = unique_dir("append");
        write_manifest_file(&dir, 7, &snapshot(vec![entry(1)], 2)).unwrap();
        let path = manifest_path(&dir, 7);
        let before = fs::metadata(&path).unwrap().len();
        let offset = append_manifest_edit_file(&path, &ManifestEdit::AddSstable(entry(2))).unwrap();
        assert_eq!(offset.start, before);
        assert!(offset.end > offset.start);

        let replay = replay_manifest_path(&path).unwrap();
        assert_eq!(
            replay.state.files.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(replay.valid_bytes, fs::metadata(&path).unwrap().len());

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_append_after_tail_recovery_truncates_to_valid_bytes() {
        let dir = unique_dir("append_after_tail");
        write_manifest_file(&dir, 3, &snapshot(vec![entry(1)], 2)).unwrap();
        let path = manifest_path(&dir, 3);
        let valid_len = fs::metadata(&path).unwrap().len();
        append_manifest_edit_file(&path, &ManifestEdit::AddSstable(entry(2))).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(valid_len + 4)
            .unwrap();

        let recovered = replay_manifest_path(&path).unwrap();
        assert!(recovered.recovered_tail);
        assert_eq!(recovered.valid_bytes, valid_len);

        let offset = append_manifest_edit_file(&path, &ManifestEdit::AddSstable(entry(2))).unwrap();
        assert_eq!(offset.start, valid_len);
        let replay = replay_manifest_path(&path).unwrap();
        assert!(!replay.recovered_tail);
        assert_eq!(replay.valid_bytes, fs::metadata(&path).unwrap().len());
        assert_eq!(
            replay.state.files.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_replay_current_fails_on_middle_corruption() {
        let dir = unique_dir("middle_corruption");
        write_manifest_file(&dir, 4, &snapshot(vec![entry(1)], 2)).unwrap();
        let path = manifest_path(&dir, 4);
        let offset = append_manifest_edit_file(&path, &ManifestEdit::AddSstable(entry(2))).unwrap();
        install_current_file(&dir, &manifest_file_name(4)).unwrap();
        corrupt_byte(&path, offset.start + 7);

        assert!(replay_current_manifest(&dir)
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch"));

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_recovers_prefix_on_torn_tail() {
        let dir = unique_dir("tail");
        write_manifest_file(&dir, 3, &snapshot(vec![entry(1)], 2)).unwrap();
        let path = manifest_path(&dir, 3);
        let before = fs::metadata(&path).unwrap().len();
        append_manifest_edit_file(&path, &ManifestEdit::AddSstable(entry(2))).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(before + 4)
            .unwrap();

        let replay = replay_manifest_path(&path).unwrap();
        assert!(replay.recovered_tail);
        assert_eq!(replay.valid_bytes, before);
        assert_eq!(
            replay.state.files.keys().copied().collect::<Vec<_>>(),
            vec![1]
        );

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_recovery_without_tail_does_not_rollover_current() {
        let dir = unique_dir("recover_no_tail");
        write_manifest_file(&dir, 6, &snapshot(vec![entry(1)], 2)).unwrap();
        install_current_file(&dir, &manifest_file_name(6)).unwrap();

        let replay = recover_current_manifest_with_rollover(&dir).unwrap();
        assert_eq!(replay.current.as_ref().unwrap().file_number, 6);
        assert!(!manifest_path(&dir, 7).exists());
        assert!(!replay.edit_replay.recovered_tail);
        assert_eq!(
            replay
                .edit_replay
                .state
                .files
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![1]
        );

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_recovery_rolls_over_torn_tail_to_new_snapshot() {
        let dir = unique_dir("recover_rollover");
        write_manifest_file(&dir, 3, &snapshot(vec![entry(1)], 2)).unwrap();
        let path = manifest_path(&dir, 3);
        let valid_len = fs::metadata(&path).unwrap().len();
        append_manifest_edit_file(&path, &ManifestEdit::AddSstable(entry(2))).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(valid_len + 4)
            .unwrap();
        install_current_file(&dir, &manifest_file_name(3)).unwrap();

        let replay = recover_current_manifest_with_rollover(&dir).unwrap();
        assert_eq!(replay.current.as_ref().unwrap().file_number, 4);
        assert_eq!(read_current_file(&dir).unwrap().file_number, 4);
        assert!(manifest_path(&dir, 4).exists());
        assert!(!replay.edit_replay.recovered_tail);
        assert_eq!(
            replay
                .edit_replay
                .state
                .files
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert!(replay_manifest_path(&path).unwrap().recovered_tail);

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_recovery_rollover_skips_existing_orphan_manifest() {
        let dir = unique_dir("recover_skip_orphan");
        write_manifest_file(&dir, 3, &snapshot(vec![entry(1)], 2)).unwrap();
        write_manifest_file(&dir, 4, &snapshot(vec![entry(40)], 41)).unwrap();
        let path = manifest_path(&dir, 3);
        let valid_len = fs::metadata(&path).unwrap().len();
        append_manifest_edit_file(&path, &ManifestEdit::AddSstable(entry(2))).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(valid_len + 4)
            .unwrap();
        install_current_file(&dir, &manifest_file_name(3)).unwrap();

        let replay = recover_current_manifest_with_rollover(&dir).unwrap();
        assert_eq!(replay.current.as_ref().unwrap().file_number, 5);
        assert_eq!(read_current_file(&dir).unwrap().file_number, 5);
        assert!(manifest_path(&dir, 5).exists());
        assert_eq!(
            replay
                .edit_replay
                .state
                .files
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![1]
        );

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_rejects_missing_or_bad_current_targets() {
        let dir = unique_dir("bad_current");
        assert!(install_current_file(&dir, &manifest_file_name(99))
            .unwrap_err()
            .to_string()
            .contains("does not exist"));

        fs::write(current_path(&dir), "../MANIFEST-000001\n").unwrap();
        assert!(read_current_file(&dir)
            .unwrap_err()
            .to_string()
            .contains("base file name"));

        fs::write(
            current_path(&dir),
            format!("{}\nextra\n", manifest_file_name(1)),
        )
        .unwrap();
        assert!(read_current_file(&dir)
            .unwrap_err()
            .to_string()
            .contains("exactly one manifest name"));

        fs::write(current_path(&dir), "OPTIONS-000001\n").unwrap();
        assert!(read_current_file(&dir)
            .unwrap_err()
            .to_string()
            .contains("invalid manifest file name"));

        fs::write(current_path(&dir), format!("{}\n", manifest_file_name(1))).unwrap();
        assert!(read_current_file(&dir)
            .unwrap_err()
            .to_string()
            .contains("does not exist"));

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_accepts_current_crlf() {
        let dir = unique_dir("current_crlf");
        write_manifest_file(&dir, 5, &snapshot(vec![entry(1)], 2)).unwrap();
        fs::write(current_path(&dir), format!("{}\r\n", manifest_file_name(5))).unwrap();

        let current = read_current_file(&dir).unwrap();
        assert_eq!(current.file_number, 5);
        assert_eq!(current.file_name, manifest_file_name(5));

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_rejects_non_snapshot_new_manifest() {
        let dir = unique_dir("non_snapshot");
        assert!(
            write_manifest_file(&dir, 1, &ManifestEdit::SetNextFileNumber(2))
                .unwrap_err()
                .to_string()
                .contains("must start with Snapshot")
        );
        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_rejects_replay_when_manifest_does_not_start_with_snapshot() {
        let dir = unique_dir("replay_non_snapshot");
        let path = manifest_path(&dir, 1);
        write_raw_manifest_edit_file(&path, &ManifestEdit::SetNextFileNumber(2));
        install_current_file(&dir, &manifest_file_name(1)).unwrap();

        assert!(replay_current_manifest(&dir)
            .unwrap_err()
            .to_string()
            .contains("must start with Snapshot"));

        cleanup_dir(&dir);
    }

    #[test]
    fn manifest_log_rejects_replay_when_current_points_to_empty_manifest() {
        let dir = unique_dir("empty_manifest");
        let path = manifest_path(&dir, 1);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.sync_all().unwrap();
        install_current_file(&dir, &manifest_file_name(1)).unwrap();

        assert!(replay_current_manifest(&dir)
            .unwrap_err()
            .to_string()
            .contains("manifest file is empty"));

        cleanup_dir(&dir);
    }
}

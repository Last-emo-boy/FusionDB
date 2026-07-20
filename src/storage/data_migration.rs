//! Data V2 migration phase: the durable, monotonic record that fences every
//! data-family writer (P10-2.1).
//!
//! The record is a single Catalog-namespace KV entry. It is the sole authority
//! on migration behavior once initialized; the `structured_data_shadow_v2`
//! config flag only supplies the pre-record default. The ladder is strictly
//! monotonic — one phase per advance, no downgrade — and every binary refuses
//! phases above what it fully implements (`MAX_SUPPORTED_PHASE`).

use std::sync::Arc;
use std::sync::OnceLock;

use parking_lot::RwLock;

use crate::common::{FusionError, Result};
use crate::storage::keyspace::{encode_identifier_key, KeyNamespace};

pub(crate) const MIGRATION_PHASE_RECORD_LEN: usize = 18;
pub(crate) const MIGRATION_PHASE_RECORD_VERSION: u8 = 1;
const MIGRATION_PHASE_KEY_COMPONENT: &[u8] = b"data-v2-migration-phase";
const BACKFILL_STATE_KEY_COMPONENT: &[u8] = b"data-v2-backfill-state";
pub(crate) const BACKFILL_STATE_RECORD_VERSION: u8 = 1;
const BACKFILL_STATE_HEADER_LEN: usize = 40;

/// The migration ladder. `Legacy` (ordinal 0) is reserved and never
/// materialized: even with the shadow flag off, delete-side v2 tombstones and
/// DROP/TRUNCATE namespace cleanup run unconditionally, so no real store is
/// below `DeleteOnly`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub(crate) enum DataMigrationPhase {
    DeleteOnly = 1,
    WriteDeleteShadow = 2,
    Backfill = 3,
    Validated = 4,
    V2Readable = 5,
    V2Only = 6,
    LegacyGc = 7,
}

/// The highest phase whose full contract (write, read, and CDC behavior) this
/// binary implements. Opening, applying, or installing state above this phase
/// must be refused — running blind would diverge by node version.
pub(crate) const MAX_SUPPORTED_PHASE: DataMigrationPhase = DataMigrationPhase::Backfill;

/// The highest phase `CALL fusiondb_data_migration_advance` may target.
/// Kept separate from `MAX_SUPPORTED_PHASE` so later tickets can land phase
/// implementations dark (raise SUPPORTED) before unlocking the advance gate.
pub(crate) const MAX_ADVANCE_TARGET_PHASE: DataMigrationPhase = DataMigrationPhase::Backfill;

impl DataMigrationPhase {
    pub(crate) fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::DeleteOnly),
            2 => Some(Self::WriteDeleteShadow),
            3 => Some(Self::Backfill),
            4 => Some(Self::Validated),
            5 => Some(Self::V2Readable),
            6 => Some(Self::V2Only),
            7 => Some(Self::LegacyGc),
            _ => None,
        }
    }

    pub(crate) fn as_byte(self) -> u8 {
        self as u8
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::DeleteOnly => "delete-only",
            Self::WriteDeleteShadow => "write-delete-shadow",
            Self::Backfill => "backfill",
            Self::Validated => "validated",
            Self::V2Readable => "v2-readable",
            Self::V2Only => "v2-only",
            Self::LegacyGc => "legacy-gc",
        }
    }

    pub(crate) fn parse_name(name: &str) -> Option<Self> {
        match name {
            "delete-only" => Some(Self::DeleteOnly),
            "write-delete-shadow" => Some(Self::WriteDeleteShadow),
            "backfill" => Some(Self::Backfill),
            "validated" => Some(Self::Validated),
            "v2-readable" => Some(Self::V2Readable),
            "v2-only" => Some(Self::V2Only),
            "legacy-gc" => Some(Self::LegacyGc),
            _ => None,
        }
    }

    pub(crate) fn next(self) -> Option<Self> {
        Self::from_byte(self.as_byte() + 1)
    }

    pub(crate) fn shadow_writes_enabled(self) -> bool {
        self >= Self::WriteDeleteShadow
    }
}

/// The decoded durable record. `phase_seq` starts at 1 on INIT and increments
/// by exactly 1 per advance; it lets the Raft apply guard enforce monotonicity
/// without history and lets later tickets stamp verification tokens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DataMigrationPhaseRecord {
    pub(crate) phase: DataMigrationPhase,
    pub(crate) phase_seq: u64,
    pub(crate) updated_at_unix_ms: u64,
}

impl DataMigrationPhaseRecord {
    pub(crate) fn encode(&self) -> [u8; MIGRATION_PHASE_RECORD_LEN] {
        let mut out = [0u8; MIGRATION_PHASE_RECORD_LEN];
        out[0] = MIGRATION_PHASE_RECORD_VERSION;
        out[1] = self.phase.as_byte();
        out[2..10].copy_from_slice(&self.phase_seq.to_be_bytes());
        out[10..18].copy_from_slice(&self.updated_at_unix_ms.to_be_bytes());
        out
    }

    /// Strict decode: exact length, exact version, phase in 1..=7. Any other
    /// shape is a loud error — a torn or foreign value must never be read as
    /// a real phase.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != MIGRATION_PHASE_RECORD_LEN {
            return Err(FusionError::Storage(format!(
                "Data V2 migration phase record has invalid length {} (expected {})",
                bytes.len(),
                MIGRATION_PHASE_RECORD_LEN
            )));
        }
        if bytes[0] != MIGRATION_PHASE_RECORD_VERSION {
            return Err(FusionError::Storage(format!(
                "Data V2 migration phase record has unsupported version {} (expected {})",
                bytes[0], MIGRATION_PHASE_RECORD_VERSION
            )));
        }
        let phase = DataMigrationPhase::from_byte(bytes[1]).ok_or_else(|| {
            FusionError::Storage(format!(
                "Data V2 migration phase record has invalid phase ordinal {}",
                bytes[1]
            ))
        })?;
        let phase_seq = u64::from_be_bytes(bytes[2..10].try_into().expect("length checked"));
        let updated_at_unix_ms =
            u64::from_be_bytes(bytes[10..18].try_into().expect("length checked"));
        if phase_seq == 0 {
            return Err(FusionError::Storage(
                "Data V2 migration phase record has phase_seq 0 (must start at 1)".to_string(),
            ));
        }
        Ok(Self {
            phase,
            phase_seq,
            updated_at_unix_ms,
        })
    }
}

/// The durable record's key. Catalog namespace keeps it out of the Data
/// namespace cleanup scans, and the leading `\0FDBK` magic keeps it invisible
/// to every legacy string-prefix scan.
pub(crate) fn migration_phase_key() -> &'static [u8] {
    static KEY: OnceLock<Vec<u8>> = OnceLock::new();
    KEY.get_or_init(|| {
        encode_identifier_key(KeyNamespace::Catalog, &[MIGRATION_PHASE_KEY_COMPONENT])
            .expect("static migration phase key components are codec-valid")
    })
}

/// The durable backfill cursor. One record for the whole store, rewritten by
/// every chunk inside that chunk's own transaction, so progress and copied
/// rows are always durable together.
///
/// It is also the DDL conflict point: DROP/TRUNCATE rewrite this key once the
/// phase reaches `Backfill`, which forces a write-write conflict with any
/// in-flight chunk. Without that, a cleanup and a chunk touch disjoint keys
/// (the cleanup only tombstones what its own snapshot saw) and both commit,
/// stranding v2 rows for a table that no longer exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DataBackfillState {
    /// Shard count observed when the backfill started, or `None` when no
    /// router was configured. A topology change invalidates every cursor, so
    /// resuming across one is refused loudly.
    pub(crate) shard_count_at_start: Option<u64>,
    pub(crate) chunks_done: u64,
    pub(crate) rows_done: u64,
    pub(crate) updated_at_unix_ms: u64,
    pub(crate) complete: bool,
    /// The last legacy key copied. Resumption continues strictly after it, so
    /// a chunk that crashed mid-flight is simply redone (chunks are
    /// idempotent: a v2 put of an identical value is a no-op in effect).
    pub(crate) cursor: Option<Vec<u8>>,
}

impl DataBackfillState {
    /// The widest cursor the codec accepts. Comfortably above any key the
    /// storage engine can hold, so a checkpoint never becomes impossible; the
    /// bound exists to stop a corrupt record from allocating wildly.
    pub(crate) const MAX_CURSOR_BYTES: usize = 4 * 1024 * 1024;

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let cursor = self.cursor.as_deref().unwrap_or(&[]);
        if cursor.len() > Self::MAX_CURSOR_BYTES {
            return Err(FusionError::Storage(format!(
                "Data V2 backfill cursor is too large to checkpoint: {} > {}",
                cursor.len(),
                Self::MAX_CURSOR_BYTES
            )));
        }
        let mut out = Vec::with_capacity(BACKFILL_STATE_HEADER_LEN + cursor.len());
        out.push(BACKFILL_STATE_RECORD_VERSION);
        out.push(u8::from(self.complete));
        // A presence flag, not an in-band sentinel: any u64 is a legal shard
        // count and must round-trip distinctly from "no router".
        out.push(u8::from(self.shard_count_at_start.is_some()));
        out.extend_from_slice(&self.shard_count_at_start.unwrap_or(0).to_be_bytes());
        out.extend_from_slice(&self.chunks_done.to_be_bytes());
        out.extend_from_slice(&self.rows_done.to_be_bytes());
        out.extend_from_slice(&self.updated_at_unix_ms.to_be_bytes());
        out.push(u8::from(self.cursor.is_some()));
        out.extend_from_slice(&(cursor.len() as u32).to_be_bytes());
        out.extend_from_slice(cursor);
        Ok(out)
    }

    /// Strict decode: exact header, exact declared cursor length with no
    /// trailing bytes, and a cursor bounded by the key codec's own limits so
    /// a legally writable row can always be checkpointed.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let malformed = |detail: &str| {
            FusionError::Storage(format!(
                "Data V2 backfill state record is malformed: {detail}"
            ))
        };
        if bytes.len() < BACKFILL_STATE_HEADER_LEN {
            return Err(malformed("shorter than the fixed header"));
        }
        if bytes[0] != BACKFILL_STATE_RECORD_VERSION {
            return Err(malformed(&format!(
                "unsupported version {} (expected {BACKFILL_STATE_RECORD_VERSION})",
                bytes[0]
            )));
        }
        let complete = match bytes[1] {
            0 => false,
            1 => true,
            other => return Err(malformed(&format!("invalid completion flag {other}"))),
        };
        let has_shard_count = match bytes[2] {
            0 => false,
            1 => true,
            other => return Err(malformed(&format!("invalid shard-count flag {other}"))),
        };
        let raw_shard_count = u64::from_be_bytes(bytes[3..11].try_into().expect("length checked"));
        if !has_shard_count && raw_shard_count != 0 {
            return Err(malformed(
                "shard-count flag is unset but a count is present",
            ));
        }
        let shard_count_at_start = has_shard_count.then_some(raw_shard_count);
        let chunks_done = u64::from_be_bytes(bytes[11..19].try_into().expect("length checked"));
        let rows_done = u64::from_be_bytes(bytes[19..27].try_into().expect("length checked"));
        let updated_at_unix_ms =
            u64::from_be_bytes(bytes[27..35].try_into().expect("length checked"));
        let has_cursor = match bytes[35] {
            0 => false,
            1 => true,
            other => return Err(malformed(&format!("invalid cursor flag {other}"))),
        };
        let cursor_len =
            u32::from_be_bytes(bytes[36..40].try_into().expect("length checked")) as usize;
        if cursor_len > Self::MAX_CURSOR_BYTES {
            return Err(malformed(&format!(
                "cursor is too large: {cursor_len} > {}",
                Self::MAX_CURSOR_BYTES
            )));
        }
        if bytes.len() != BACKFILL_STATE_HEADER_LEN + cursor_len {
            return Err(malformed(
                "declared cursor length does not match the record",
            ));
        }
        if !has_cursor && cursor_len != 0 {
            return Err(malformed("cursor flag is unset but a cursor is present"));
        }
        Ok(Self {
            shard_count_at_start,
            chunks_done,
            rows_done,
            updated_at_unix_ms,
            complete,
            cursor: has_cursor.then(|| bytes[BACKFILL_STATE_HEADER_LEN..].to_vec()),
        })
    }
}

pub(crate) fn backfill_state_key() -> &'static [u8] {
    static KEY: OnceLock<Vec<u8>> = OnceLock::new();
    KEY.get_or_init(|| {
        encode_identifier_key(KeyNamespace::Catalog, &[BACKFILL_STATE_KEY_COMPONENT])
            .expect("static backfill state key components are codec-valid")
    })
}

/// What a transaction pins when it fences: the phase it acted on and the
/// record sequence it observed. `phase_seq == 0` is the no-record state (the
/// config-flag default), so INIT itself changes the fence and aborts
/// concurrent fenced transactions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FenceSnapshot {
    pub(crate) phase: DataMigrationPhase,
    pub(crate) phase_seq: u64,
}

/// The cache's three states, exposed so callers can distinguish "no durable
/// record" (apply your own configured default) from "not yet read".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CachedFenceState {
    Unknown,
    NoRecord,
    Record(FenceSnapshot),
}

enum FenceState {
    /// Not yet read, or invalidated after a Raft apply / snapshot install.
    Unloaded,
    /// A read confirmed no record exists; behavior follows the config flag.
    Missing,
    Loaded(Arc<FenceSnapshot>),
}

/// Process-wide cache of the durable phase, shared by every clone of a
/// `FusionStorage`. Reads are one `RwLock` read + `Arc` clone; the commit
/// path publishes new values inside the commit critical section so the fence
/// observes a total order with data commits.
pub(crate) struct DataMigrationFence {
    state: RwLock<FenceState>,
    flag_default: DataMigrationPhase,
}

impl DataMigrationFence {
    pub(crate) fn new(structured_data_shadow_v2: bool) -> Self {
        let flag_default = if structured_data_shadow_v2 {
            DataMigrationPhase::WriteDeleteShadow
        } else {
            DataMigrationPhase::DeleteOnly
        };
        Self {
            state: RwLock::new(FenceState::Unloaded),
            flag_default,
        }
    }

    /// The fence used when no record exists (pre-INIT stores).
    pub(crate) fn flag_default_snapshot(&self) -> FenceSnapshot {
        FenceSnapshot {
            phase: self.flag_default,
            phase_seq: 0,
        }
    }

    /// Fast-path read. `None` means the state is unknown (unloaded or
    /// invalidated) and the caller must consult storage via `resolve_with`.
    pub(crate) fn cached(&self) -> Option<FenceSnapshot> {
        match &*self.state.read() {
            FenceState::Unloaded => None,
            FenceState::Missing => Some(self.flag_default_snapshot()),
            FenceState::Loaded(snapshot) => Some(**snapshot),
        }
    }

    /// Like [`cached`], but reports the no-record case explicitly so a caller
    /// with its own configured default (the Executor's flag) can supply it
    /// instead of the storage-baked one. Pre-record behavior must follow the
    /// same flag it followed before this fence existed.
    pub(crate) fn cached_state(&self) -> CachedFenceState {
        match &*self.state.read() {
            FenceState::Unloaded => CachedFenceState::Unknown,
            FenceState::Missing => CachedFenceState::NoRecord,
            FenceState::Loaded(snapshot) => CachedFenceState::Record(**snapshot),
        }
    }

    /// Publish the outcome of a storage read performed by the caller.
    ///
    /// Monotonic by construction: the reader may hold an older MVCC snapshot
    /// than a concurrently published advance, and regressing the cache would
    /// let a later write fence on a stale phase and still pass the equality
    /// check at commit. A lower (or absent) sequence therefore never
    /// overwrites a higher one; the caller is told the authoritative value.
    pub(crate) fn resolve_with(&self, record: Option<&DataMigrationPhaseRecord>) -> FenceSnapshot {
        let mut state = self.state.write();
        let observed = match record {
            None => self.flag_default_snapshot(),
            Some(record) => FenceSnapshot {
                phase: record.phase,
                phase_seq: record.phase_seq,
            },
        };
        if let FenceState::Loaded(current) = &*state {
            if current.phase_seq >= observed.phase_seq {
                return **current;
            }
        }
        match record {
            None => {
                *state = FenceState::Missing;
            }
            Some(_) => {
                *state = FenceState::Loaded(Arc::new(observed));
            }
        }
        observed
    }

    /// Publish a just-committed record. Called inside the commit critical
    /// section so every later commit observes the new fence.
    pub(crate) fn publish_committed(&self, record: &DataMigrationPhaseRecord) {
        let snapshot = FenceSnapshot {
            phase: record.phase,
            phase_seq: record.phase_seq,
        };
        *self.state.write() = FenceState::Loaded(Arc::new(snapshot));
    }

    /// Forget the cached state. Called after Raft apply and snapshot install;
    /// the next observer re-reads through storage.
    pub(crate) fn invalidate(&self) {
        *self.state.write() = FenceState::Unloaded;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrip_encodes_18_fixed_bytes() {
        let record = DataMigrationPhaseRecord {
            phase: DataMigrationPhase::WriteDeleteShadow,
            phase_seq: 7,
            updated_at_unix_ms: 1_752_000_000_123,
        };
        let bytes = record.encode();
        assert_eq!(bytes.len(), MIGRATION_PHASE_RECORD_LEN);
        assert_eq!(bytes[0], MIGRATION_PHASE_RECORD_VERSION);
        assert_eq!(DataMigrationPhaseRecord::decode(&bytes).unwrap(), record);
    }

    #[test]
    fn decode_rejects_every_malformed_shape() {
        let valid = DataMigrationPhaseRecord {
            phase: DataMigrationPhase::DeleteOnly,
            phase_seq: 1,
            updated_at_unix_ms: 0,
        }
        .encode();

        assert!(DataMigrationPhaseRecord::decode(&valid[..17]).is_err());
        let mut long = valid.to_vec();
        long.push(0);
        assert!(DataMigrationPhaseRecord::decode(&long).is_err());

        let mut wrong_version = valid;
        wrong_version[0] = 2;
        assert!(DataMigrationPhaseRecord::decode(&wrong_version).is_err());

        let mut legacy_phase = valid;
        legacy_phase[1] = 0;
        assert!(DataMigrationPhaseRecord::decode(&legacy_phase).is_err());

        let mut unknown_phase = valid;
        unknown_phase[1] = 99;
        assert!(DataMigrationPhaseRecord::decode(&unknown_phase).is_err());

        let mut zero_seq = valid;
        zero_seq[2..10].copy_from_slice(&0u64.to_be_bytes());
        assert!(DataMigrationPhaseRecord::decode(&zero_seq).is_err());
    }

    fn backfill_state(cursor: Option<&[u8]>, shard_count: Option<u64>) -> DataBackfillState {
        DataBackfillState {
            shard_count_at_start: shard_count,
            chunks_done: 7,
            rows_done: 4096,
            updated_at_unix_ms: 1_752_000_000_123,
            complete: false,
            cursor: cursor.map(<[u8]>::to_vec),
        }
    }

    #[test]
    fn backfill_state_round_trips_every_field() {
        for state in [
            backfill_state(None, None),
            backfill_state(Some(b"data:orders:0001"), Some(16)),
            // Every u64 is a legal shard count, including the value a
            // sentinel-based encoding would have swallowed.
            backfill_state(Some(b""), Some(u64::MAX)),
            backfill_state(Some(&[0xff; 512]), Some(0)),
            DataBackfillState {
                complete: true,
                ..backfill_state(Some(b"shard:9:data:t:1"), Some(4))
            },
        ] {
            let encoded = state.encode().expect("encode");
            assert_eq!(DataBackfillState::decode(&encoded).unwrap(), state);
        }
    }

    #[test]
    fn backfill_state_decode_rejects_malformed_shapes() {
        let valid = backfill_state(Some(b"data:orders:1"), Some(8))
            .encode()
            .unwrap();

        assert!(DataBackfillState::decode(&valid[..valid.len() - 1]).is_err());
        let mut extra = valid.clone();
        extra.push(0);
        assert!(
            DataBackfillState::decode(&extra).is_err(),
            "trailing bytes must be rejected"
        );

        for (index, byte, reason) in [
            (0usize, 2u8, "version"),
            (1, 2, "completion flag"),
            (2, 2, "shard-count flag"),
            (35, 2, "cursor flag"),
        ] {
            let mut corrupt = valid.clone();
            corrupt[index] = byte;
            assert!(
                DataBackfillState::decode(&corrupt).is_err(),
                "{reason} must be validated"
            );
        }

        // Cursor flag unset while a cursor is present.
        let mut lying = valid.clone();
        lying[35] = 0;
        assert!(DataBackfillState::decode(&lying).is_err());

        // Shard-count flag unset while a count is present.
        let mut ghost_count = backfill_state(None, None).encode().unwrap();
        ghost_count[3..11].copy_from_slice(&7u64.to_be_bytes());
        assert!(DataBackfillState::decode(&ghost_count).is_err());
    }

    /// An over-long cursor must fail loudly at write time. Silently encoding
    /// one would durably store a record this same binary then refuses to
    /// decode, wedging every later chunk and every DROP/TRUNCATE.
    #[test]
    fn backfill_state_refuses_to_encode_an_undecodable_cursor() {
        let oversized = vec![b'x'; DataBackfillState::MAX_CURSOR_BYTES + 1];
        let error = backfill_state(Some(&oversized), None)
            .encode()
            .expect_err("an oversized cursor must not be encoded");
        assert!(error.to_string().contains("too large to checkpoint"));

        let at_limit = vec![b'x'; DataBackfillState::MAX_CURSOR_BYTES];
        let encoded = backfill_state(Some(&at_limit), None)
            .encode()
            .expect("a cursor at the limit is encodable");
        assert!(
            DataBackfillState::decode(&encoded).is_ok(),
            "anything encodable must be decodable"
        );
    }

    #[test]
    fn backfill_state_key_is_a_distinct_catalog_sibling() {
        assert!(backfill_state_key().starts_with(b"\0FDBK"));
        assert_eq!(backfill_state_key()[6], 6);
        assert_ne!(backfill_state_key(), migration_phase_key());
        assert_eq!(backfill_state_key(), backfill_state_key());
    }

    #[test]
    fn ladder_is_strictly_monotonic() {
        let mut phase = DataMigrationPhase::DeleteOnly;
        let mut seen = vec![phase];
        while let Some(next) = phase.next() {
            assert!(next > phase);
            seen.push(next);
            phase = next;
        }
        assert_eq!(phase, DataMigrationPhase::LegacyGc);
        assert_eq!(seen.len(), 7);
        for phase in seen {
            assert_eq!(DataMigrationPhase::parse_name(phase.name()), Some(phase));
            assert_eq!(DataMigrationPhase::from_byte(phase.as_byte()), Some(phase));
        }
        assert_eq!(DataMigrationPhase::parse_name("legacy"), None);
    }

    #[test]
    fn migration_phase_key_is_catalog_namespaced_and_stable() {
        let key = migration_phase_key();
        assert!(key.starts_with(b"\0FDBK"));
        assert_eq!(key[6], 6);
        assert_eq!(migration_phase_key(), key);
    }

    #[test]
    fn fence_states_transition_and_publish() {
        let fence = DataMigrationFence::new(false);
        assert_eq!(fence.cached(), None);

        assert_eq!(
            fence.resolve_with(None),
            FenceSnapshot {
                phase: DataMigrationPhase::DeleteOnly,
                phase_seq: 0
            }
        );
        assert_eq!(fence.cached(), Some(fence.flag_default_snapshot()));

        let record = DataMigrationPhaseRecord {
            phase: DataMigrationPhase::WriteDeleteShadow,
            phase_seq: 2,
            updated_at_unix_ms: 5,
        };
        fence.publish_committed(&record);
        assert_eq!(
            fence.cached(),
            Some(FenceSnapshot {
                phase: DataMigrationPhase::WriteDeleteShadow,
                phase_seq: 2
            })
        );

        fence.invalidate();
        assert_eq!(fence.cached(), None);

        let flag_on = DataMigrationFence::new(true);
        assert_eq!(
            flag_on.flag_default_snapshot().phase,
            DataMigrationPhase::WriteDeleteShadow
        );
    }
}

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
pub(crate) const MAX_SUPPORTED_PHASE: DataMigrationPhase = DataMigrationPhase::WriteDeleteShadow;

/// The highest phase `CALL fusiondb_data_migration_advance` may target.
/// Kept separate from `MAX_SUPPORTED_PHASE` so later tickets can land phase
/// implementations dark (raise SUPPORTED) before unlocking the advance gate.
pub(crate) const MAX_ADVANCE_TARGET_PHASE: DataMigrationPhase =
    DataMigrationPhase::WriteDeleteShadow;

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

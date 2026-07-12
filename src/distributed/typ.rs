use openraft::BasicNode;
use std::io::Cursor;

pub type NodeId = u64;

pub const MUTATION_BATCH_VERSION: u16 = 1;

/// A deterministic state-machine request. SQL is evaluated exactly once by
/// the leader; followers only apply the resulting byte-level mutations.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum Request {
    MutationBatch(MutationBatch),
    /// A fail-closed decoder target for the pre-mutation-batch JSON shape.
    /// Mixed-version clusters still require a stop-the-world upgrade because
    /// old nodes cannot apply deterministic mutation batches.
    LegacySql {
        sql: String,
    },
}

#[derive(serde::Deserialize)]
enum TaggedRequest {
    MutationBatch(MutationBatch),
    LegacySql { sql: String },
}

impl From<TaggedRequest> for Request {
    fn from(value: TaggedRequest) -> Self {
        match value {
            TaggedRequest::MutationBatch(batch) => Self::MutationBatch(batch),
            TaggedRequest::LegacySql { sql } => Self::LegacySql { sql },
        }
    }
}

impl<'de> serde::Deserialize<'de> for Request {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            #[derive(serde::Deserialize)]
            #[serde(untagged)]
            enum HumanRequest {
                Tagged(TaggedRequest),
                Legacy { sql: String },
            }

            return Ok(
                match <HumanRequest as serde::Deserialize>::deserialize(deserializer)? {
                    HumanRequest::Tagged(request) => request.into(),
                    HumanRequest::Legacy { sql } => Self::LegacySql { sql },
                },
            );
        }

        <TaggedRequest as serde::Deserialize>::deserialize(deserializer).map(Into::into)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MutationBatch {
    pub version: u16,
    pub preconditions: Vec<KvPrecondition>,
    pub mutations: Vec<KvMutation>,
    pub side_index_mutations: Vec<SideIndexMutation>,
    pub response: Response,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KvPrecondition {
    pub key: Vec<u8>,
    pub expected: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum KvMutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SideIndexMutation {
    TrigramAdd {
        table: String,
        column: String,
        numeric_id: u64,
        row_id: String,
        text: String,
    },
    TrigramRemove {
        table: String,
        column: String,
        numeric_id: u64,
        text: String,
    },
    VectorInsert {
        index: String,
        id: String,
        /// IEEE-754 bits keep the Raft payload byte-stable and `Eq`.
        vector_bits: Vec<u32>,
    },
    VectorDelete {
        index: String,
        id: String,
    },
}

/// Response from the state machine after applying a request.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Response {
    pub success: bool,
    pub message: String,
    pub results: Vec<ReplicatedQueryResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReplicatedQueryResult {
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<ReplicatedValue>>,
    },
    Success {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReplicatedValue {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    Decimal(String),
    String(String),
    Date(i32),
    Timestamp(i64),
    Interval(i64),
    Blob(Vec<u8>),
    VectorBits(Vec<u32>),
    Array(Vec<ReplicatedValue>),
    Object(Vec<(String, ReplicatedValue)>),
}

impl Response {
    pub fn success(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: message.into(),
            results: Vec::new(),
        }
    }

    pub fn success_with_results(
        message: impl Into<String>,
        results: Vec<ReplicatedQueryResult>,
    ) -> Self {
        Self {
            success: true,
            message: message.into(),
            results,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: message.into(),
            results: Vec::new(),
        }
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D            = Request,
        R            = Response,
        NodeId       = NodeId,
        Node         = BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

#[cfg(test)]
mod tests {
    use super::*;

    fn mutation_request() -> Request {
        Request::MutationBatch(MutationBatch {
            version: MUTATION_BATCH_VERSION,
            preconditions: Vec::new(),
            mutations: Vec::new(),
            side_index_mutations: Vec::new(),
            response: Response::success("ok"),
        })
    }

    #[test]
    fn request_decodes_legacy_json_into_fail_closed_variant() {
        let request: Request = serde_json::from_str(r#"{"sql":"DELETE FROM accounts"}"#).unwrap();
        assert_eq!(
            request,
            Request::LegacySql {
                sql: "DELETE FROM accounts".to_string()
            }
        );
    }

    #[test]
    fn request_current_json_and_bincode_round_trip() {
        let request = mutation_request();
        let json = serde_json::to_vec(&request).unwrap();
        assert_eq!(serde_json::from_slice::<Request>(&json).unwrap(), request);

        let binary = bincode::serialize(&request).unwrap();
        assert_eq!(bincode::deserialize::<Request>(&binary).unwrap(), request);
    }
}

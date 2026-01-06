use thiserror::Error;

#[derive(Error, Debug)]
pub enum FusionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Parser error: {0}")]
    Parser(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, FusionError>;

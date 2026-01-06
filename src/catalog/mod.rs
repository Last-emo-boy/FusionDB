use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexType {
    None,
    BTree,
    FTS,
    HNSW,
}

impl Default for IndexType {
    fn default() -> Self {
        IndexType::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub is_primary: bool,
    #[serde(default)]
    pub is_indexed: bool, // Keep for backward compatibility/simple check
    #[serde(default)]
    pub index_type: IndexType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self { name, columns }
    }

    pub fn get_primary_key_index(&self) -> Option<usize> {
        self.columns.iter().position(|c| c.is_primary)
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum IndexType {
    #[default]
    None,
    BTree,
    FTS,
    HNSW,
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
    #[serde(default)]
    pub default_value: Option<String>,
    #[serde(default = "default_true")]
    pub is_nullable: bool,
    #[serde(default)]
    pub is_unique: bool,
    #[serde(default)]
    pub check_expr: Option<String>,
}

fn default_true() -> bool {
    true
}

fn column_type_starts_with_ascii_case_insensitive(value: &str, prefix: &str) -> bool {
    match value.as_bytes().get(..prefix.len()) {
        Some(candidate) => candidate.eq_ignore_ascii_case(prefix.as_bytes()),
        None => false,
    }
}

impl Column {
    /// Whether this column participates in the trigram fuzzy-text index:
    /// an indexed text-typed column (BTree or FTS). Single source of truth
    /// shared by DML maintenance and the storage-level rebuild.
    pub fn is_trigram_text_column(&self) -> bool {
        if !self.is_indexed || !matches!(self.index_type, IndexType::BTree | IndexType::FTS) {
            return false;
        }
        let data_type = self.data_type.trim();
        data_type.eq_ignore_ascii_case("TEXT")
            || data_type.eq_ignore_ascii_case("STRING")
            || data_type.eq_ignore_ascii_case("VARCHAR")
            || data_type.eq_ignore_ascii_case("CHAR")
            || column_type_starts_with_ascii_case_insensitive(data_type, "VARCHAR(")
            || column_type_starts_with_ascii_case_insensitive(data_type, "CHAR(")
            || column_type_starts_with_ascii_case_insensitive(data_type, "CHARACTER")
    }
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
        self.columns
            .iter()
            .position(|c| c.name == name)
            .or_else(|| {
                self.columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(name))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> TableSchema {
        TableSchema::new(
            "users".to_string(),
            vec![
                Column {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary: true,
                    is_indexed: true,
                    index_type: IndexType::BTree,
                    default_value: None,
                    is_nullable: false,
                    is_unique: true,
                    check_expr: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "TEXT".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: IndexType::None,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
                Column {
                    name: "email".to_string(),
                    data_type: "TEXT".to_string(),
                    is_primary: false,
                    is_indexed: true,
                    index_type: IndexType::FTS,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
            ],
        )
    }

    #[test]
    fn test_get_primary_key_index() {
        let schema = sample_schema();
        assert_eq!(schema.get_primary_key_index(), Some(0));
    }

    #[test]
    fn test_get_primary_key_index_none() {
        let schema = TableSchema::new(
            "no_pk".to_string(),
            vec![Column {
                name: "val".to_string(),
                data_type: "TEXT".to_string(),
                is_primary: false,
                is_indexed: false,
                index_type: IndexType::None,
                default_value: None,
                is_nullable: true,
                is_unique: false,
                check_expr: None,
            }],
        );
        assert_eq!(schema.get_primary_key_index(), None);
    }

    #[test]
    fn test_get_column_index_found() {
        let schema = sample_schema();
        assert_eq!(schema.get_column_index("name"), Some(1));
        assert_eq!(schema.get_column_index("email"), Some(2));
    }

    #[test]
    fn test_get_column_index_matches_unquoted_postgres_identifiers_case_insensitively() {
        let schema = sample_schema();
        assert_eq!(schema.get_column_index("NAME"), Some(1));
        assert_eq!(schema.get_column_index("Email"), Some(2));
    }

    #[test]
    fn test_get_column_index_not_found() {
        let schema = sample_schema();
        assert_eq!(schema.get_column_index("nonexistent"), None);
    }

    #[test]
    fn test_index_type_default() {
        let idx: IndexType = IndexType::default();
        assert_eq!(idx, IndexType::None);
    }

    #[test]
    fn test_schema_serialization_roundtrip() {
        let schema = sample_schema();
        let bytes = bincode::serialize(&schema).unwrap();
        let deserialized: TableSchema = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.name, "users");
        assert_eq!(deserialized.columns.len(), 3);
        assert!(deserialized.columns[0].is_primary);
        assert_eq!(deserialized.columns[2].index_type, IndexType::FTS);
    }
}

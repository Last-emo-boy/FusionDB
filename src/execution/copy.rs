use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{
    CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, Ident, Statement,
};

use super::{Executor, QueryResult};

const COPY_INSERT_BATCH_ROWS: usize = 1_000;

#[derive(Debug, Clone)]
struct CopyFromOptions {
    format_csv: bool,
    header: bool,
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    null_marker: String,
}

impl Default for CopyFromOptions {
    fn default() -> Self {
        Self {
            format_csv: false,
            header: false,
            delimiter: b'\t',
            quote: b'"',
            escape: None,
            null_marker: "\\N".to_string(),
        }
    }
}

impl Executor {
    pub(crate) async fn handle_copy(
        &self,
        source: &CopySource,
        to: bool,
        target: &CopyTarget,
        options: &[CopyOption],
        legacy_options: &[CopyLegacyOption],
        values: &[Option<String>],
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        if to {
            return Err(FusionError::NotImplemented(
                "COPY TO is not supported yet".to_string(),
            ));
        }

        if !values.is_empty() {
            return Err(FusionError::NotImplemented(
                "COPY FROM STDIN payload is not supported yet".to_string(),
            ));
        }

        let CopySource::Table {
            table_name,
            columns,
        } = source
        else {
            return Err(FusionError::Execution(
                "COPY FROM requires a table target".to_string(),
            ));
        };

        let filename = match target {
            CopyTarget::File { filename } => filename,
            CopyTarget::Stdin => {
                return Err(FusionError::NotImplemented(
                    "COPY FROM STDIN is not supported yet".to_string(),
                ))
            }
            CopyTarget::Program { .. } => {
                return Err(FusionError::NotImplemented(
                    "COPY FROM PROGRAM is not supported".to_string(),
                ))
            }
            CopyTarget::Stdout => {
                return Err(FusionError::Execution(
                    "COPY FROM STDOUT is invalid".to_string(),
                ))
            }
        };

        let copy_options = Self::copy_from_options(options, legacy_options)?;
        let rows = self.read_copy_file(filename, &copy_options)?;
        let count = self
            .insert_copy_rows(table_name.to_string(), columns, rows, txn)
            .await?;

        Ok(QueryResult::Success {
            message: format!("Copied {} rows", count),
        })
    }

    fn copy_from_options(
        options: &[CopyOption],
        legacy_options: &[CopyLegacyOption],
    ) -> Result<CopyFromOptions> {
        let mut parsed = CopyFromOptions::default();

        for option in options {
            match option {
                CopyOption::Format(format) => {
                    let value = format.value.to_ascii_lowercase();
                    if value != "csv" && value != "text" {
                        return Err(FusionError::Execution(format!(
                            "COPY FORMAT {} is not supported",
                            format.value
                        )));
                    }
                    parsed.format_csv = value == "csv";
                    if parsed.format_csv && parsed.delimiter == b'\t' {
                        parsed.delimiter = b',';
                    }
                }
                CopyOption::Header(value) => parsed.header = *value,
                CopyOption::Delimiter(value) => parsed.delimiter = Self::copy_ascii_char(*value)?,
                CopyOption::Null(value) => parsed.null_marker = value.clone(),
                CopyOption::Quote(value) => parsed.quote = Self::copy_ascii_char(*value)?,
                CopyOption::Escape(value) => parsed.escape = Some(Self::copy_ascii_char(*value)?),
                CopyOption::Encoding(value)
                    if !value.eq_ignore_ascii_case("utf8")
                        && !value.eq_ignore_ascii_case("utf-8") =>
                {
                    return Err(FusionError::Execution(format!(
                        "COPY ENCODING {} is not supported",
                        value
                    )));
                }
                CopyOption::Encoding(_) => {}
                CopyOption::Freeze(_)
                | CopyOption::ForceQuote(_)
                | CopyOption::ForceNotNull(_)
                | CopyOption::ForceNull(_) => {
                    return Err(FusionError::NotImplemented(format!(
                        "COPY option {} is not supported yet",
                        option
                    )));
                }
            }
        }

        for option in legacy_options {
            match option {
                CopyLegacyOption::Csv(csv_options) => {
                    parsed.format_csv = true;
                    if parsed.delimiter == b'\t' {
                        parsed.delimiter = b',';
                    }
                    for csv_option in csv_options {
                        match csv_option {
                            CopyLegacyCsvOption::Header => parsed.header = true,
                            CopyLegacyCsvOption::Quote(value) => {
                                parsed.quote = Self::copy_ascii_char(*value)?
                            }
                            CopyLegacyCsvOption::Escape(value) => {
                                parsed.escape = Some(Self::copy_ascii_char(*value)?)
                            }
                            CopyLegacyCsvOption::ForceQuote(_)
                            | CopyLegacyCsvOption::ForceNotNull(_) => {
                                return Err(FusionError::NotImplemented(format!(
                                    "COPY CSV option {} is not supported yet",
                                    csv_option
                                )));
                            }
                        }
                    }
                }
                CopyLegacyOption::Header => parsed.header = true,
                CopyLegacyOption::Delimiter(value) => {
                    parsed.delimiter = Self::copy_ascii_char(*value)?
                }
                CopyLegacyOption::Null(value) => parsed.null_marker = value.clone(),
                other => {
                    return Err(FusionError::NotImplemented(format!(
                        "COPY option {} is not supported yet",
                        other
                    )));
                }
            }
        }

        Ok(parsed)
    }

    fn copy_ascii_char(value: char) -> Result<u8> {
        if value.is_ascii() {
            Ok(value as u8)
        } else {
            Err(FusionError::Execution(format!(
                "COPY delimiter/quote/escape must be ASCII: {}",
                value
            )))
        }
    }

    fn read_copy_file(&self, filename: &str, options: &CopyFromOptions) -> Result<Vec<Vec<Value>>> {
        let file = std::fs::File::open(filename).map_err(|e| {
            FusionError::Execution(format!("COPY failed to open {}: {}", filename, e))
        })?;
        let mut builder = csv::ReaderBuilder::new();
        builder
            .has_headers(options.header)
            .delimiter(options.delimiter)
            .quote(options.quote)
            .flexible(false);
        if let Some(escape) = options.escape {
            builder.escape(Some(escape));
        }

        let mut reader = builder.from_reader(file);
        let mut rows = Vec::new();
        for record in reader.records() {
            let record = record
                .map_err(|e| FusionError::Execution(format!("COPY CSV parse error: {}", e)))?;
            rows.push(
                record
                    .iter()
                    .map(|field| Self::copy_field_to_value(field, &options.null_marker))
                    .collect(),
            );
        }
        Ok(rows)
    }

    fn copy_field_to_value(field: &str, null_marker: &str) -> Value {
        if field == null_marker {
            return Value::Null;
        }
        let trimmed = field.trim();
        if trimmed.eq_ignore_ascii_case("null") {
            return Value::Null;
        }
        if let Ok(value) = trimmed.parse::<i64>() {
            return Value::Integer(value);
        }
        if let Ok(value) = trimmed.parse::<f64>() {
            return Value::Float(value);
        }
        match trimmed.to_ascii_lowercase().as_str() {
            "true" | "t" => Value::Boolean(true),
            "false" | "f" => Value::Boolean(false),
            _ => Value::String(field.to_string()),
        }
    }

    async fn insert_copy_rows(
        &self,
        table_name: String,
        columns: &[Ident],
        rows: Vec<Vec<Value>>,
        txn: &mut dyn Transaction,
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut count = 0usize;
        for batch in rows.chunks(COPY_INSERT_BATCH_ROWS) {
            self.insert_copy_batch(&table_name, columns, batch, txn)
                .await?;
            count += batch.len();
        }
        Ok(count)
    }

    async fn insert_copy_batch(
        &self,
        table_name: &str,
        columns: &[Ident],
        rows: &[Vec<Value>],
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let values = rows
            .iter()
            .map(|row| {
                format!(
                    "({})",
                    row.iter()
                        .map(Self::copy_value_to_sql)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let column_list = if columns.is_empty() {
            String::new()
        } else {
            format!(
                " ({})",
                columns
                    .iter()
                    .map(|column| column.value.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let sql = format!(
            "INSERT INTO {}{} VALUES {}",
            table_name, column_list, values
        );
        let statements = crate::parser::parse_sql(&sql)?;
        let Some(Statement::Insert(insert)) = statements.first() else {
            return Err(FusionError::Execution(
                "COPY failed to build INSERT statement".to_string(),
            ));
        };

        let result = self
            .handle_insert(
                insert.table.to_string(),
                &insert.columns,
                &insert.source,
                &insert.returning,
                &insert.on,
                txn,
                &[],
            )
            .await?;
        match result {
            QueryResult::Success { .. } => Ok(()),
            other => Err(FusionError::Execution(format!(
                "COPY expected INSERT success, got {:?}",
                other
            ))),
        }
    }

    fn copy_value_to_sql(value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Boolean(value) => value.to_string(),
            Value::Integer(value) => value.to_string(),
            Value::Float(value) => value.to_string(),
            Value::String(value) => format!("'{}'", value.replace('\'', "''")),
            Value::Blob(_) | Value::Vector(_) | Value::Array(_) | Value::Object(_) => {
                format!("'{}'", value.to_string().replace('\'', "''"))
            }
        }
    }
}

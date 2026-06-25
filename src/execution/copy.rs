use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{
    CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, Ident, ObjectName,
    ObjectNamePart, Statement,
};
use std::io::{Cursor, Read};

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
    fn copy_trace_enabled() -> bool {
        std::env::var("FUSIONDB_COPY_TRACE")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false)
    }

    fn copy_trace(message: impl AsRef<str>) {
        if Self::copy_trace_enabled() {
            eprintln!("[copy-trace] {}", message.as_ref());
        }
    }

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
                if !values.is_empty() {
                    return Err(FusionError::NotImplemented(
                        "inline COPY FROM STDIN payload is not supported yet".to_string(),
                    ));
                }
                return Err(FusionError::NotImplemented(
                    "COPY FROM STDIN is not supported yet".to_string(),
                ));
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
        Self::copy_trace(format!(
            "file copy read start table={} file={} csv={} delimiter={:?}",
            table_name, filename, copy_options.format_csv, copy_options.delimiter as char
        ));
        let rows = self.read_copy_file(filename, &copy_options)?;
        let table_name = Self::copy_table_name(table_name)?;
        Self::copy_trace(format!(
            "file copy insert start table={} rows={}",
            table_name,
            rows.len()
        ));
        let count = self
            .insert_copy_rows(table_name, columns, rows, txn)
            .await?;

        Ok(QueryResult::Success {
            message: format!("Copied {} rows", count),
        })
    }

    pub(crate) async fn execute_copy_stdin_payload(
        &self,
        statement: &Statement,
        payload: &[u8],
        txn: &mut dyn Transaction,
    ) -> Result<usize> {
        let Statement::Copy {
            source,
            to,
            target,
            options,
            legacy_options,
            ..
        } = statement
        else {
            return Err(FusionError::Execution(
                "COPY STDIN execution requires a COPY statement".to_string(),
            ));
        };

        if *to {
            return Err(FusionError::NotImplemented(
                "COPY TO is not supported yet".to_string(),
            ));
        }
        if !matches!(target, CopyTarget::Stdin) {
            return Err(FusionError::Execution(
                "COPY STDIN execution requires COPY FROM STDIN".to_string(),
            ));
        }

        let CopySource::Table {
            table_name,
            columns,
        } = source
        else {
            return Err(FusionError::Execution(
                "COPY FROM STDIN requires a table target".to_string(),
            ));
        };

        let copy_options = Self::copy_from_options(options, legacy_options)?;
        Self::copy_trace(format!(
            "stdin copy read start statement={} bytes={} csv={} delimiter={:?}",
            statement,
            payload.len(),
            copy_options.format_csv,
            copy_options.delimiter as char
        ));
        let rows = Self::read_copy_bytes(payload, &copy_options)?;
        let table_name = Self::copy_table_name(table_name)?;
        self.ensure_copy_rows_owned_locally(&table_name, columns, &rows, txn)
            .await?;
        Self::copy_trace(format!(
            "stdin copy insert start table={} columns={} rows={} first_row={:?}",
            table_name,
            columns.len(),
            rows.len(),
            rows.first()
        ));
        self.insert_copy_rows(table_name, columns, rows, txn).await
    }

    async fn ensure_copy_rows_owned_locally(
        &self,
        table_name: &str,
        columns: &[Ident],
        rows: &[Vec<Value>],
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let Some(router) = self.shard_router.clone() else {
            return Ok(());
        };
        let Some(schema) = self
            .load_table_schema_for_shard_routing(table_name, txn)
            .await?
        else {
            return Ok(());
        };
        let Some(pk_idx) = schema.get_primary_key_index() else {
            return Ok(());
        };
        let composite_unique_indexes = self
            .load_composite_unique_indexes_for_table(table_name, txn)
            .await?;
        if composite_unique_indexes
            .iter()
            .any(|index| index.name.ends_with("_pkey"))
        {
            return Ok(());
        }
        let Some(pk_value_idx) = Self::copy_primary_key_value_index(columns, &schema, pk_idx)
        else {
            return Ok(());
        };

        for row in rows {
            if !Self::copy_row_shape_can_be_routed(columns, &schema, row) {
                return Ok(());
            }
            let Some(value) = row.get(pk_value_idx) else {
                return Ok(());
            };
            let value = Self::coerce_value_to_column_type(
                value.clone(),
                &schema.columns[pk_idx].data_type,
            )?;
            let Some(row_id) = Self::value_to_primary_row_id(&value) else {
                return Ok(());
            };
            let route = router.route_key(table_name, &row_id);
            let local_node_id = router.local_node_id();
            if route.owner_node_id != local_node_id {
                return Err(FusionError::ShardRouteConflict(format!(
                    "Shard route conflict: COPY on table {} with shard key {} routes to shard {} owned by node {} at {}; local node {} must not execute this write",
                    route.table,
                    route.shard_key,
                    route.shard_id,
                    route.owner_node_id,
                    route.owner_addr,
                    local_node_id
                )));
            }
        }
        Ok(())
    }

    fn copy_primary_key_value_index(
        columns: &[Ident],
        schema: &crate::catalog::TableSchema,
        pk_idx: usize,
    ) -> Option<usize> {
        if columns.is_empty() {
            return Some(pk_idx);
        }

        let pk_column = &schema.columns[pk_idx].name;
        columns
            .iter()
            .position(|column| column.value.eq_ignore_ascii_case(pk_column))
    }

    fn copy_row_shape_can_be_routed(
        columns: &[Ident],
        schema: &crate::catalog::TableSchema,
        row: &[Value],
    ) -> bool {
        if columns.is_empty() {
            row.len() == schema.columns.len()
        } else {
            row.len() == columns.len()
        }
    }

    fn copy_table_name(table_name: &ObjectName) -> Result<String> {
        table_name
            .0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .map(|ident| ident.value.clone())
            .ok_or_else(|| {
                FusionError::Execution(format!("COPY table target {} is not supported", table_name))
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
                    let is_csv = format.value.eq_ignore_ascii_case("csv");
                    if !is_csv && !format.value.eq_ignore_ascii_case("text") {
                        return Err(FusionError::Execution(format!(
                            "COPY FORMAT {} is not supported",
                            format.value
                        )));
                    }
                    parsed.format_csv = is_csv;
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
        Self::read_copy_reader(file, options)
    }

    fn read_copy_bytes(payload: &[u8], options: &CopyFromOptions) -> Result<Vec<Vec<Value>>> {
        let row_capacity = Self::copy_payload_row_capacity(payload, options);
        Self::read_copy_reader_with_capacity(Cursor::new(payload), options, row_capacity)
    }

    fn read_copy_reader<R: Read>(reader: R, options: &CopyFromOptions) -> Result<Vec<Vec<Value>>> {
        Self::read_copy_reader_with_capacity(reader, options, 0)
    }

    fn read_copy_reader_with_capacity<R: Read>(
        reader: R,
        options: &CopyFromOptions,
        row_capacity: usize,
    ) -> Result<Vec<Vec<Value>>> {
        let mut builder = csv::ReaderBuilder::new();
        builder
            .has_headers(options.header)
            .delimiter(options.delimiter)
            .quote(options.quote)
            .flexible(false);
        if let Some(escape) = options.escape {
            builder.escape(Some(escape));
        }

        let mut reader = builder.from_reader(reader);
        let mut rows = Vec::with_capacity(row_capacity);
        for record in reader.records() {
            let record = record
                .map_err(|e| FusionError::Execution(format!("COPY CSV parse error: {}", e)))?;
            let mut row = Vec::with_capacity(record.len());
            for field in record.iter() {
                row.push(Self::copy_field_to_value(field, &options.null_marker));
            }
            rows.push(row);
        }
        Ok(rows)
    }

    fn copy_payload_row_capacity(payload: &[u8], options: &CopyFromOptions) -> usize {
        if payload.is_empty() {
            return 0;
        }

        let mut line_count = payload.iter().filter(|byte| **byte == b'\n').count();
        if payload
            .last()
            .is_some_and(|byte| *byte != b'\n' && *byte != b'\r')
        {
            line_count += 1;
        }

        line_count.saturating_sub(usize::from(options.header))
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
        if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("t") {
            Value::Boolean(true)
        } else if trimmed.eq_ignore_ascii_case("false") || trimmed.eq_ignore_ascii_case("f") {
            Value::Boolean(false)
        } else {
            Value::String(field.to_string())
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
            Self::copy_trace(format!(
                "copy batch insert start table={} batch_rows={} count_so_far={}",
                table_name,
                batch.len(),
                count
            ));
            self.insert_copy_batch(&table_name, columns, batch, txn)
                .await?;
            count += batch.len();
            Self::copy_trace(format!(
                "copy batch insert done table={} count={}",
                table_name, count
            ));
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
        Self::copy_trace(format!(
            "copy batch direct insert table={} rows={} columns={}",
            table_name,
            rows.len(),
            columns.len()
        ));
        self.insert_direct_rows(table_name, columns, rows, txn)
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_payload_row_capacity_accounts_for_headers_and_trailing_rows() {
        let csv_options = CopyFromOptions {
            format_csv: true,
            header: true,
            delimiter: b',',
            ..CopyFromOptions::default()
        };
        assert_eq!(
            Executor::copy_payload_row_capacity(b"id,name\n1,Alice\n2,Bob\n", &csv_options),
            2
        );
        assert_eq!(
            Executor::copy_payload_row_capacity(b"id,name\n1,Alice", &csv_options),
            1
        );

        let text_options = CopyFromOptions::default();
        assert_eq!(
            Executor::copy_payload_row_capacity(b"1\tAlice\n2\tBob\n", &text_options),
            2
        );
    }

    #[test]
    fn read_copy_bytes_preserves_csv_rows_with_capacity_hint() {
        let options = CopyFromOptions {
            format_csv: true,
            header: true,
            delimiter: b',',
            null_marker: "NULL".to_string(),
            ..CopyFromOptions::default()
        };

        let rows =
            Executor::read_copy_bytes(b"id,name,age\n1,Alice,30\n2,Bob,NULL\n", &options).unwrap();

        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Integer(1),
                    Value::String("Alice".to_string()),
                    Value::Integer(30)
                ],
                vec![
                    Value::Integer(2),
                    Value::String("Bob".to_string()),
                    Value::Null
                ],
            ]
        );
    }

    #[test]
    fn copy_from_options_matches_format_case_without_lowercase_allocation() {
        let csv_options =
            Executor::copy_from_options(&[CopyOption::Format(Ident::new("cSv"))], &[]).unwrap();
        assert!(csv_options.format_csv);
        assert_eq!(csv_options.delimiter, b',');

        let text_options =
            Executor::copy_from_options(&[CopyOption::Format(Ident::new("TeXt"))], &[]).unwrap();
        assert!(!text_options.format_csv);
        assert_eq!(text_options.delimiter, b'\t');

        let error = Executor::copy_from_options(&[CopyOption::Format(Ident::new("json"))], &[])
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Execution error: COPY FORMAT json is not supported"
        );
    }

    #[test]
    fn copy_field_to_value_matches_boolean_case_without_lowercase_allocation() {
        assert_eq!(
            Executor::copy_field_to_value(" TrUe ", "NULL"),
            Value::Boolean(true)
        );
        assert_eq!(
            Executor::copy_field_to_value(" F ", "NULL"),
            Value::Boolean(false)
        );
        assert_eq!(
            Executor::copy_field_to_value(" truth ", "NULL"),
            Value::String(" truth ".to_string())
        );
    }
}

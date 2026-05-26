use bytes::{BufMut, BytesMut};
use futures::{Sink, SinkExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex; // Required for client.send

use pgwire::api::auth::{
    finish_authentication, protocol_negotiation, save_startup_parameters_to_metadata,
    DefaultServerParameterProvider, LoginInfo, StartupHandler,
};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::{ClientInfo, PgWireConnectionState, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::{ParameterDescription, RowDescription};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Close, Describe, Execute, Parse, ParseComplete, Sync as PgSync,
};
use pgwire::messages::response::CommandComplete;
use pgwire::messages::startup::Authentication;
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::PgWireFrontendMessage;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, Statement,
};

use crate::common::{FusionError, Value}; // Import FusionError
use crate::execution::{Executor, QueryResult};
use crate::parser::parse_sql;
use crate::storage::{Storage, Transaction};

struct Session {
    transaction: Option<Box<dyn Transaction>>,
    statements: HashMap<String, StatementData>, // name -> statement data
    portals: HashMap<String, PortalData>,       // name -> portal data
}

struct StatementData {
    query: String,
    parameter_types: Vec<Type>,
}

struct PortalData {
    #[allow(dead_code)]
    statement_name: String,
    query: String,
    params: Vec<Value>,
}

pub struct PgHandler {
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    query_parser: Arc<NoopQueryParser>,
    session: Arc<Mutex<Session>>,
}

impl PgHandler {
    pub fn new(executor: Arc<Executor>, storage: Arc<dyn Storage>) -> Self {
        Self {
            executor,
            storage,
            query_parser: Arc::new(NoopQueryParser::new()),
            session: Arc::new(Mutex::new(Session {
                transaction: None,
                statements: HashMap::new(),
                portals: HashMap::new(),
            })),
        }
    }

    fn username_for_client<C: ClientInfo>(client: &C) -> String {
        LoginInfo::from_client_info(client)
            .user()
            .unwrap_or_default()
            .to_string()
    }

    fn auth_error(message: impl Into<String>) -> pgwire::error::ErrorInfo {
        pgwire::error::ErrorInfo::new("ERROR".to_string(), "42501".to_string(), message.into())
    }

    fn pg_type_for_value(value: &Value) -> Type {
        match value {
            Value::Boolean(_) => Type::BOOL,
            Value::Integer(_) => Type::INT8,
            Value::Float(_) => Type::FLOAT8,
            Value::Blob(_) => Type::BYTEA,
            Value::String(_)
            | Value::Vector(_)
            | Value::Array(_)
            | Value::Object(_)
            | Value::Null => Type::TEXT,
        }
    }

    fn pg_type_for_column_type(data_type: &str) -> Type {
        let upper = data_type.trim().to_uppercase();
        match upper.as_str() {
            "BOOL" | "BOOLEAN" => Type::BOOL,
            "SMALLINT" | "INT2" => Type::INT2,
            "INT" | "INT4" | "INTEGER" => Type::INT4,
            "BIGINT" | "INT8" => Type::INT8,
            "REAL" | "FLOAT4" => Type::FLOAT4,
            "FLOAT" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => Type::FLOAT8,
            "BYTEA" | "BLOB" | "BINARY" | "VARBINARY" => Type::BYTEA,
            "DATE" => Type::DATE,
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "DATETIME" => Type::TIMESTAMP,
            "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Type::TIMESTAMPTZ,
            "TIME" | "TIME WITHOUT TIME ZONE" => Type::TIME,
            "TIMETZ" | "TIME WITH TIME ZONE" => Type::TIMETZ,
            "NUMERIC" | "DECIMAL" => Type::NUMERIC,
            "INTERVAL" => Type::INTERVAL,
            _ if upper.starts_with("VARCHAR")
                || upper.starts_with("CHAR")
                || upper.ends_with("TEXT")
                || upper == "STRING" =>
            {
                Type::TEXT
            }
            _ if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") => Type::NUMERIC,
            _ if upper.starts_with("FLOAT") || upper.starts_with("DOUBLE") => Type::FLOAT8,
            _ => Type::TEXT,
        }
    }

    fn pg_type_for_sql_type(data_type: &sqlparser::ast::DataType) -> Type {
        use sqlparser::ast::DataType;
        match data_type {
            DataType::Bool | DataType::Boolean => Type::BOOL,
            DataType::TinyInt(_)
            | DataType::TinyIntUnsigned(_)
            | DataType::SmallInt(_)
            | DataType::SmallIntUnsigned(_)
            | DataType::Int2(_)
            | DataType::Int2Unsigned(_) => Type::INT2,
            DataType::Int(_)
            | DataType::Int4(_)
            | DataType::Integer(_)
            | DataType::IntUnsigned(_)
            | DataType::Int4Unsigned(_)
            | DataType::IntegerUnsigned(_)
            | DataType::MediumInt(_)
            | DataType::MediumIntUnsigned(_)
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => Type::INT4,
            DataType::BigInt(_)
            | DataType::Int8(_)
            | DataType::Int64
            | DataType::BigIntUnsigned(_)
            | DataType::Int8Unsigned(_)
            | DataType::UBigInt
            | DataType::UInt64 => Type::INT8,
            DataType::Float4 | DataType::Float32 | DataType::Real | DataType::RealUnsigned => {
                Type::FLOAT4
            }
            DataType::Float(_)
            | DataType::Float8
            | DataType::Float64
            | DataType::Double(_)
            | DataType::DoubleUnsigned(_)
            | DataType::DoublePrecision
            | DataType::DoublePrecisionUnsigned => Type::FLOAT8,
            DataType::Numeric(_)
            | DataType::Decimal(_)
            | DataType::DecimalUnsigned(_)
            | DataType::BigNumeric(_)
            | DataType::BigDecimal(_)
            | DataType::Dec(_)
            | DataType::DecUnsigned(_) => Type::NUMERIC,
            DataType::Date | DataType::Date32 => Type::DATE,
            DataType::Time(_, _) => Type::TIME,
            DataType::Timestamp(_, _)
            | DataType::TimestampNtz(_)
            | DataType::Datetime(_)
            | DataType::Datetime64(_, _) => Type::TIMESTAMP,
            DataType::Interval { .. } => Type::INTERVAL,
            DataType::Binary(_)
            | DataType::Varbinary(_)
            | DataType::Blob(_)
            | DataType::TinyBlob
            | DataType::MediumBlob
            | DataType::LongBlob
            | DataType::Bytes(_)
            | DataType::Bytea => Type::BYTEA,
            _ => Type::TEXT,
        }
    }

    fn value_as_pg_text(value: Value) -> Option<String> {
        match value {
            Value::Null => None,
            Value::Boolean(b) => Some(if b { "t".to_string() } else { "f".to_string() }),
            Value::Integer(i) => Some(i.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::String(s) => Some(s),
            Value::Blob(b) => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let mut out = String::with_capacity(2 + b.len() * 2);
                out.push_str("\\x");
                for byte in b {
                    out.push(HEX[(byte >> 4) as usize] as char);
                    out.push(HEX[(byte & 0x0f) as usize] as char);
                }
                Some(out)
            }
            Value::Vector(v) => Some(format!("{:?}", v)),
            Value::Array(v) => Some(format!("{:?}", v)),
            Value::Object(v) => Some(format!("{:?}", v)),
        }
    }

    fn infer_text_fields(columns: &[String], rows: &[Vec<Value>]) -> Arc<Vec<FieldInfo>> {
        Arc::new(
            columns
                .iter()
                .enumerate()
                .map(|(idx, name)| {
                    let datatype = rows
                        .iter()
                        .filter_map(|row| row.get(idx))
                        .find(|value| !matches!(value, Value::Null))
                        .map(Self::pg_type_for_value)
                        .unwrap_or(Type::TEXT);

                    FieldInfo::new(name.clone(), None, None, datatype, FieldFormat::Text)
                })
                .collect::<Vec<_>>(),
        )
    }

    fn infer_binary_fields(columns: &[String], rows: &[Vec<Value>]) -> Arc<Vec<FieldInfo>> {
        Arc::new(
            columns
                .iter()
                .enumerate()
                .map(|(idx, name)| {
                    let datatype = rows
                        .iter()
                        .filter_map(|row| row.get(idx))
                        .find(|value| !matches!(value, Value::Null))
                        .map(Self::pg_type_for_value)
                        .unwrap_or(Type::TEXT);

                    FieldInfo::new(name.clone(), None, None, datatype, FieldFormat::Binary)
                })
                .collect::<Vec<_>>(),
        )
    }

    fn encode_row(
        fields: Arc<Vec<FieldInfo>>,
        row: Vec<Value>,
    ) -> PgWireResult<pgwire::messages::data::DataRow> {
        let mut encoder = DataRowEncoder::new(fields);
        for val in row {
            let text = Self::value_as_pg_text(val);
            encoder.encode_field(&text)?;
        }
        Ok(encoder.take_row())
    }

    fn encode_binary_row(
        fields: Arc<Vec<FieldInfo>>,
        row: Vec<Value>,
    ) -> pgwire::messages::data::DataRow {
        let mut out = BytesMut::with_capacity(row.len() * 16);
        let mut field_count = 0i16;

        for (idx, value) in row.into_iter().enumerate() {
            field_count += 1;
            let Some(field) = fields.get(idx) else {
                out.put_i32(-1);
                continue;
            };

            let mut value_buf = BytesMut::with_capacity(16);
            if Self::put_binary_value(&mut value_buf, field.datatype(), value) {
                out.put_i32(value_buf.len() as i32);
                out.extend_from_slice(&value_buf);
            } else {
                out.put_i32(-1);
            }
        }

        pgwire::messages::data::DataRow::new(out, field_count)
    }

    fn put_binary_value(out: &mut BytesMut, data_type: &Type, value: Value) -> bool {
        match value {
            Value::Null => false,
            Value::Boolean(b) => {
                out.put_u8(u8::from(b));
                true
            }
            Value::Integer(i) => {
                match *data_type {
                    Type::INT2 => out.put_i16(i as i16),
                    Type::INT4 => out.put_i32(i as i32),
                    _ => out.put_i64(i),
                }
                true
            }
            Value::Float(f) => {
                match *data_type {
                    Type::FLOAT4 => out.put_f32(f as f32),
                    Type::INT2 => out.put_i16(f as i16),
                    Type::INT4 => out.put_i32(f as i32),
                    Type::INT8 => out.put_i64(f as i64),
                    _ => out.put_f64(f),
                }
                true
            }
            Value::String(s) => {
                out.extend_from_slice(s.as_bytes());
                true
            }
            Value::Blob(b) => {
                out.extend_from_slice(&b);
                true
            }
            Value::Vector(v) => {
                out.extend_from_slice(format!("{:?}", v).as_bytes());
                true
            }
            Value::Array(v) => {
                out.extend_from_slice(format!("{:?}", v).as_bytes());
                true
            }
            Value::Object(v) => {
                out.extend_from_slice(format!("{:?}", v).as_bytes());
                true
            }
        }
    }

    fn infer_parameter_types_from_query(query: &str, provided_oids: &[u32]) -> Vec<Type> {
        let placeholder_count = parse_sql(query)
            .ok()
            .map(|statements| {
                statements
                    .iter()
                    .map(Self::max_placeholder_in_statement)
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or_else(|| Self::max_placeholder_in_text(query));
        let count = placeholder_count.max(provided_oids.len());
        let mut types = vec![Type::TEXT; count];
        for (idx, oid) in provided_oids.iter().enumerate() {
            if let Some(ty) = Type::from_oid(*oid) {
                if idx < types.len() {
                    types[idx] = ty;
                }
            }
        }

        if let Ok(statements) = parse_sql(query) {
            for stmt in &statements {
                Self::infer_parameter_types_from_statement(stmt, &mut types);
            }
        }

        types
    }

    fn infer_parameter_types_from_statement(stmt: &Statement, types: &mut [Type]) {
        match stmt {
            Statement::Query(query) => Self::infer_parameter_types_from_query_ast(query, types),
            Statement::Update(update) => {
                if let Some(selection) = &update.selection {
                    Self::infer_parameter_types_from_expr(selection, None, types);
                }
                for assignment in &update.assignments {
                    Self::infer_parameter_types_from_expr(&assignment.value, None, types);
                }
            }
            Statement::Delete(delete) => {
                if let Some(selection) = &delete.selection {
                    Self::infer_parameter_types_from_expr(selection, None, types);
                }
            }
            Statement::Insert(insert) => {
                if let Some(source) = &insert.source {
                    Self::infer_parameter_types_from_query_ast(source, types);
                }
            }
            Statement::Explain { statement, .. } => {
                Self::infer_parameter_types_from_statement(statement, types)
            }
            _ => {}
        }
    }

    fn infer_parameter_types_from_query_ast(query: &sqlparser::ast::Query, types: &mut [Type]) {
        if let SetExpr::Select(select) = query.body.as_ref() {
            let schema = select.from.first().and_then(|table| {
                if table.joins.is_empty() {
                    if let sqlparser::ast::TableFactor::Table { name, .. } = &table.relation {
                        Some((name.to_string(), Vec::<(String, Type)>::new()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            });

            if let Some(selection) = &select.selection {
                Self::infer_parameter_types_from_expr(selection, schema.as_ref(), types);
            }
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item
                {
                    Self::infer_parameter_types_from_expr(expr, schema.as_ref(), types);
                }
            }
        }
    }

    fn infer_parameter_types_from_expr(
        expr: &Expr,
        _schema_hint: Option<&(String, Vec<(String, Type)>)>,
        types: &mut [Type],
    ) {
        match expr {
            Expr::BinaryOp { left, op, right }
                if matches!(op, sqlparser::ast::BinaryOperator::Eq) =>
            {
                if let Some((idx, ty)) = Self::placeholder_column_type_pair(left, right) {
                    if let Some(slot) = types.get_mut(idx.saturating_sub(1)) {
                        *slot = ty;
                    }
                }
                if let Some((idx, ty)) = Self::placeholder_column_type_pair(right, left) {
                    if let Some(slot) = types.get_mut(idx.saturating_sub(1)) {
                        *slot = ty;
                    }
                }
                Self::infer_parameter_types_from_expr(left, None, types);
                Self::infer_parameter_types_from_expr(right, None, types);
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::infer_parameter_types_from_expr(left, None, types);
                Self::infer_parameter_types_from_expr(right, None, types);
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Nested(expr)
            | Expr::Cast { expr, .. }
            | Expr::Ceil { expr, .. }
            | Expr::Floor { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::InSubquery { expr, .. } => {
                Self::infer_parameter_types_from_expr(expr, None, types)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::infer_parameter_types_from_expr(expr, None, types);
                Self::infer_parameter_types_from_expr(low, None, types);
                Self::infer_parameter_types_from_expr(high, None, types);
            }
            Expr::InList { expr, list, .. } => {
                Self::infer_parameter_types_from_expr(expr, None, types);
                for item in list {
                    Self::infer_parameter_types_from_expr(item, None, types);
                }
            }
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                Self::infer_parameter_types_from_expr(expr, None, types);
                Self::infer_parameter_types_from_expr(pattern, None, types);
            }
            Expr::Function(func) => {
                if let FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                            Self::infer_parameter_types_from_expr(expr, None, types);
                        }
                    }
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(expr) = operand {
                    Self::infer_parameter_types_from_expr(expr, None, types);
                }
                for when in conditions {
                    Self::infer_parameter_types_from_expr(&when.condition, None, types);
                    Self::infer_parameter_types_from_expr(&when.result, None, types);
                }
                if let Some(expr) = else_result {
                    Self::infer_parameter_types_from_expr(expr, None, types);
                }
            }
            Expr::Array(array) => {
                for expr in &array.elem {
                    Self::infer_parameter_types_from_expr(expr, None, types);
                }
            }
            _ => {}
        }
    }

    fn placeholder_column_type_pair(
        placeholder_expr: &Expr,
        column_expr: &Expr,
    ) -> Option<(usize, Type)> {
        let Expr::Value(value) = placeholder_expr else {
            return None;
        };
        let sqlparser::ast::Value::Placeholder(p) = &value.value else {
            return None;
        };
        let idx = p
            .strip_prefix('$')
            .unwrap_or(p)
            .parse::<usize>()
            .ok()
            .filter(|idx| *idx > 0)?;

        let column_name = match column_expr {
            Expr::Identifier(ident) => ident.value.as_str(),
            Expr::CompoundIdentifier(idents) => idents.last()?.value.as_str(),
            _ => return None,
        };
        let upper = column_name.to_ascii_uppercase();
        let ty = if upper.ends_with("ID") || upper == "ID" || upper.ends_with("_ID") {
            Type::INT8
        } else if upper.contains("AMOUNT")
            || upper.contains("BALANCE")
            || upper.contains("PRICE")
            || upper.contains("SCORE")
            || upper.contains("TOTAL")
        {
            Type::FLOAT8
        } else {
            Type::TEXT
        };
        Some((idx, ty))
    }

    fn decode_text_param(bytes: &[u8], param_type: &Type) -> Value {
        let s = String::from_utf8_lossy(bytes).to_string();
        match *param_type {
            Type::INT2 | Type::INT4 | Type::INT8 => s
                .trim()
                .parse::<i64>()
                .map(Value::Integer)
                .unwrap_or(Value::String(s)),
            Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => s
                .trim()
                .parse::<f64>()
                .map(Value::Float)
                .unwrap_or(Value::String(s)),
            Type::BOOL => match s.trim().to_ascii_lowercase().as_str() {
                "t" | "true" | "1" | "yes" | "on" => Value::Boolean(true),
                "f" | "false" | "0" | "no" | "off" => Value::Boolean(false),
                _ => Value::String(s),
            },
            Type::BYTEA => Value::Blob(bytes.to_vec()),
            _ => {
                if let Ok(i) = s.parse::<i64>() {
                    Value::Integer(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    Value::Float(f)
                } else {
                    Value::String(s)
                }
            }
        }
    }

    fn decode_binary_param(bytes: &[u8], param_type: &Type) -> Value {
        match *param_type {
            Type::INT2 if bytes.len() == 2 => {
                Value::Integer(i16::from_be_bytes([bytes[0], bytes[1]]) as i64)
            }
            Type::INT4 if bytes.len() == 4 => {
                Value::Integer(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64)
            }
            Type::INT8 if bytes.len() == 8 => Value::Integer(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])),
            Type::FLOAT4 if bytes.len() == 4 => Value::Float(f32::from_bits(u32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ])) as f64),
            Type::FLOAT8 if bytes.len() == 8 => Value::Float(f64::from_bits(u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))),
            Type::BOOL if bytes.len() == 1 => Value::Boolean(bytes[0] != 0),
            Type::BYTEA => Value::Blob(bytes.to_vec()),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::UNKNOWN => {
                Value::String(String::from_utf8_lossy(bytes).to_string())
            }
            _ => String::from_utf8(bytes.to_vec())
                .map(|s| Self::decode_text_param(s.as_bytes(), param_type))
                .unwrap_or_else(|_| Value::Blob(bytes.to_vec())),
        }
    }

    fn max_placeholder_in_text(query: &str) -> usize {
        let bytes = query.as_bytes();
        let mut max_placeholder = 0usize;
        let mut idx = 0usize;
        while idx < bytes.len() {
            if bytes[idx] == b'$' {
                let mut end = idx + 1;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if end > idx + 1 {
                    if let Ok(value) = query[idx + 1..end].parse::<usize>() {
                        max_placeholder = max_placeholder.max(value);
                    }
                }
                idx = end;
            } else {
                idx += 1;
            }
        }
        max_placeholder
    }

    fn max_placeholder_in_statement(stmt: &Statement) -> usize {
        match stmt {
            Statement::Query(query) => Self::max_placeholder_in_query(query),
            Statement::Insert(insert) => {
                let mut max_placeholder = insert
                    .source
                    .as_ref()
                    .map(|query| Self::max_placeholder_in_query(query))
                    .unwrap_or(0);
                if let Some(returning) = &insert.returning {
                    for item in returning {
                        max_placeholder =
                            max_placeholder.max(Self::max_placeholder_in_select_item(item));
                    }
                }
                max_placeholder
            }
            Statement::Update(update) => {
                let mut max_placeholder = update
                    .selection
                    .as_ref()
                    .map(Self::max_placeholder_in_expr)
                    .unwrap_or(0);
                for assignment in &update.assignments {
                    max_placeholder =
                        max_placeholder.max(Self::max_placeholder_in_expr(&assignment.value));
                }
                if let Some(returning) = &update.returning {
                    for item in returning {
                        max_placeholder =
                            max_placeholder.max(Self::max_placeholder_in_select_item(item));
                    }
                }
                max_placeholder
            }
            Statement::Delete(delete) => delete
                .selection
                .as_ref()
                .map(Self::max_placeholder_in_expr)
                .unwrap_or(0),
            Statement::Explain { statement, .. } => Self::max_placeholder_in_statement(statement),
            _ => 0,
        }
    }

    fn max_placeholder_in_query(query: &sqlparser::ast::Query) -> usize {
        let mut max_placeholder = 0usize;
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                max_placeholder = max_placeholder.max(Self::max_placeholder_in_query(&cte.query));
            }
        }
        max_placeholder =
            max_placeholder.max(Self::max_placeholder_in_set_expr(query.body.as_ref()));
        if let Some(order_by) = &query.order_by {
            if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
                for expr in exprs {
                    max_placeholder =
                        max_placeholder.max(Self::max_placeholder_in_expr(&expr.expr));
                }
            }
        }
        max_placeholder
    }

    fn max_placeholder_in_set_expr(set_expr: &SetExpr) -> usize {
        match set_expr {
            SetExpr::Select(select) => {
                let mut max_placeholder = 0usize;
                for item in &select.projection {
                    max_placeholder =
                        max_placeholder.max(Self::max_placeholder_in_select_item(item));
                }
                if let Some(selection) = &select.selection {
                    max_placeholder = max_placeholder.max(Self::max_placeholder_in_expr(selection));
                }
                if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                    for expr in exprs {
                        max_placeholder = max_placeholder.max(Self::max_placeholder_in_expr(expr));
                    }
                }
                if let Some(having) = &select.having {
                    max_placeholder = max_placeholder.max(Self::max_placeholder_in_expr(having));
                }
                max_placeholder
            }
            SetExpr::Query(query) => Self::max_placeholder_in_query(query),
            SetExpr::SetOperation { left, right, .. } => Self::max_placeholder_in_set_expr(left)
                .max(Self::max_placeholder_in_set_expr(right)),
            _ => 0,
        }
    }

    fn max_placeholder_in_select_item(item: &SelectItem) -> usize {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                Self::max_placeholder_in_expr(expr)
            }
            _ => 0,
        }
    }

    fn max_placeholder_in_expr(expr: &Expr) -> usize {
        match expr {
            Expr::Value(value) => {
                if let sqlparser::ast::Value::Placeholder(p) = &value.value {
                    p.strip_prefix('$')
                        .unwrap_or(p)
                        .parse::<usize>()
                        .unwrap_or(0)
                } else {
                    0
                }
            }
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => 0,
            Expr::BinaryOp { left, right, .. } => {
                Self::max_placeholder_in_expr(left).max(Self::max_placeholder_in_expr(right))
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Nested(expr)
            | Expr::Cast { expr, .. }
            | Expr::Ceil { expr, .. }
            | Expr::Floor { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::InSubquery { expr, .. } => Self::max_placeholder_in_expr(expr),
            Expr::Between {
                expr, low, high, ..
            } => Self::max_placeholder_in_expr(expr)
                .max(Self::max_placeholder_in_expr(low))
                .max(Self::max_placeholder_in_expr(high)),
            Expr::InList { expr, list, .. } => {
                let mut max_placeholder = Self::max_placeholder_in_expr(expr);
                for item in list {
                    max_placeholder = max_placeholder.max(Self::max_placeholder_in_expr(item));
                }
                max_placeholder
            }
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                Self::max_placeholder_in_expr(expr).max(Self::max_placeholder_in_expr(pattern))
            }
            Expr::Function(func) => {
                let mut max_placeholder = 0usize;
                if let FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                            max_placeholder =
                                max_placeholder.max(Self::max_placeholder_in_expr(expr));
                        }
                    }
                }
                max_placeholder
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let mut max_placeholder = operand
                    .as_ref()
                    .map(|expr| Self::max_placeholder_in_expr(expr))
                    .unwrap_or(0);
                for when in conditions {
                    max_placeholder = max_placeholder
                        .max(Self::max_placeholder_in_expr(&when.condition))
                        .max(Self::max_placeholder_in_expr(&when.result));
                }
                if let Some(expr) = else_result {
                    max_placeholder = max_placeholder.max(Self::max_placeholder_in_expr(expr));
                }
                max_placeholder
            }
            Expr::Array(array) => {
                let mut max_placeholder = 0usize;
                for expr in &array.elem {
                    max_placeholder = max_placeholder.max(Self::max_placeholder_in_expr(expr));
                }
                max_placeholder
            }
            _ => 0,
        }
    }

    async fn describe_query_fields(&self, query: &str) -> PgWireResult<Vec<FieldInfo>> {
        let statements = parse_sql(query).map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                "Parse Error: {:?}",
                e
            ))))
        })?;
        let Some(stmt) = statements.first() else {
            return Ok(Vec::new());
        };
        self.describe_statement_fields(stmt).await
    }

    async fn describe_statement_fields(&self, stmt: &Statement) -> PgWireResult<Vec<FieldInfo>> {
        match stmt {
            Statement::Query(query) => self.describe_select_query_fields(query).await,
            Statement::Insert(insert) => {
                let Some(returning) = &insert.returning else {
                    return Ok(Vec::new());
                };
                Ok(returning
                    .iter()
                    .map(|item| {
                        FieldInfo::new(item.to_string(), None, None, Type::TEXT, FieldFormat::Text)
                    })
                    .collect())
            }
            _ => Ok(Vec::new()),
        }
    }

    async fn describe_select_query_fields(
        &self,
        query: &sqlparser::ast::Query,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(Vec::new());
        };
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return Ok(self.describe_projection_fallback(&select.projection));
        }
        let sqlparser::ast::TableFactor::Table { name, .. } = &select.from[0].relation else {
            return Ok(self.describe_projection_fallback(&select.projection));
        };

        let table_name = name.to_string();
        let txn = self.storage.begin_transaction().await.map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                "Describe storage error: {:?}",
                e
            ))))
        })?;
        let schema_key = format!("schema:{}", table_name);
        let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await.map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                "Describe schema error: {:?}",
                e
            ))))
        })?
        else {
            return Ok(self.describe_projection_fallback(&select.projection));
        };
        let schema: crate::catalog::TableSchema =
            bincode::deserialize(&schema_bytes).map_err(|e| {
                PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                    "Schema deserialization error: {}",
                    e
                ))))
            })?;

        let wildcard = select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        });
        if wildcard {
            return Ok(schema
                .columns
                .iter()
                .map(|column| {
                    FieldInfo::new(
                        column.name.clone(),
                        None,
                        None,
                        Self::pg_type_for_column_type(&column.data_type),
                        FieldFormat::Text,
                    )
                })
                .collect());
        }

        let mut fields = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let name = match expr {
                        Expr::Identifier(ident) => ident.value.clone(),
                        _ => expr.to_string(),
                    };
                    fields.push(FieldInfo::new(
                        name,
                        None,
                        None,
                        self.pg_type_for_projection_expr(expr, &schema),
                        FieldFormat::Text,
                    ));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    fields.push(FieldInfo::new(
                        alias.value.clone(),
                        None,
                        None,
                        self.pg_type_for_projection_expr(expr, &schema),
                        FieldFormat::Text,
                    ));
                }
                _ => {}
            }
        }
        Ok(fields)
    }

    fn describe_projection_fallback(&self, projection: &[SelectItem]) -> Vec<FieldInfo> {
        projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::UnnamedExpr(expr) => Some(FieldInfo::new(
                    expr.to_string(),
                    None,
                    None,
                    Self::pg_type_for_literal_expr(expr).unwrap_or(Type::TEXT),
                    FieldFormat::Text,
                )),
                SelectItem::ExprWithAlias { expr, alias } => Some(FieldInfo::new(
                    alias.value.clone(),
                    None,
                    None,
                    Self::pg_type_for_literal_expr(expr).unwrap_or(Type::TEXT),
                    FieldFormat::Text,
                )),
                _ => None,
            })
            .collect()
    }

    fn pg_type_for_projection_expr(
        &self,
        expr: &Expr,
        schema: &crate::catalog::TableSchema,
    ) -> Type {
        match expr {
            Expr::Identifier(ident) => schema
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&ident.value))
                .map(|column| Self::pg_type_for_column_type(&column.data_type))
                .unwrap_or(Type::TEXT),
            Expr::CompoundIdentifier(idents) => {
                let col_name = idents
                    .last()
                    .map(|ident| ident.value.as_str())
                    .unwrap_or("");
                schema
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(col_name))
                    .map(|column| Self::pg_type_for_column_type(&column.data_type))
                    .unwrap_or(Type::TEXT)
            }
            Expr::Cast { data_type, .. } => Self::pg_type_for_sql_type(data_type),
            Expr::BinaryOp { left, op, right } => match op {
                sqlparser::ast::BinaryOperator::Eq
                | sqlparser::ast::BinaryOperator::NotEq
                | sqlparser::ast::BinaryOperator::Gt
                | sqlparser::ast::BinaryOperator::GtEq
                | sqlparser::ast::BinaryOperator::Lt
                | sqlparser::ast::BinaryOperator::LtEq
                | sqlparser::ast::BinaryOperator::And
                | sqlparser::ast::BinaryOperator::Or => Type::BOOL,
                sqlparser::ast::BinaryOperator::StringConcat => Type::TEXT,
                _ => {
                    let left_type = self.pg_type_for_projection_expr(left, schema);
                    let right_type = self.pg_type_for_projection_expr(right, schema);
                    if left_type == Type::FLOAT8
                        || right_type == Type::FLOAT8
                        || left_type == Type::FLOAT4
                        || right_type == Type::FLOAT4
                    {
                        Type::FLOAT8
                    } else {
                        Type::INT8
                    }
                }
            },
            Expr::UnaryOp { expr, .. }
            | Expr::Nested(expr)
            | Expr::Ceil { expr, .. }
            | Expr::Floor { expr, .. } => self.pg_type_for_projection_expr(expr, schema),
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                match name.as_str() {
                    "COUNT" | "ROW_NUMBER" | "RANK" | "DENSE_RANK" => Type::INT8,
                    "SUM" | "AVG" | "MIN" | "MAX" => Type::FLOAT8,
                    "NOW" | "CURRENT_TIMESTAMP" => Type::TIMESTAMP,
                    "CURRENT_DATE" => Type::DATE,
                    "COALESCE" | "NULLIF" => {
                        if let FunctionArguments::List(args) = &func.args {
                            args.args
                                .iter()
                                .find_map(|arg| {
                                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                                        Some(self.pg_type_for_projection_expr(expr, schema))
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(Type::TEXT)
                        } else {
                            Type::TEXT
                        }
                    }
                    _ => Type::TEXT,
                }
            }
            Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. } => Type::BOOL,
            Expr::Value(_) => Self::pg_type_for_literal_expr(expr).unwrap_or(Type::TEXT),
            _ => Type::TEXT,
        }
    }

    fn pg_type_for_literal_expr(expr: &Expr) -> Option<Type> {
        let Expr::Value(value) = expr else {
            return None;
        };
        match &value.value {
            sqlparser::ast::Value::Boolean(_) => Some(Type::BOOL),
            sqlparser::ast::Value::Number(n, _) => {
                if n.parse::<i64>().is_ok() {
                    Some(Type::INT8)
                } else {
                    Some(Type::FLOAT8)
                }
            }
            sqlparser::ast::Value::SingleQuotedString(_)
            | sqlparser::ast::Value::DoubleQuotedString(_)
            | sqlparser::ast::Value::EscapedStringLiteral(_)
            | sqlparser::ast::Value::NationalStringLiteral(_) => Some(Type::TEXT),
            sqlparser::ast::Value::Null => Some(Type::TEXT),
            _ => None,
        }
    }

    async fn execute_first_statement(
        &self,
        query: &str,
        params: &[Value],
    ) -> std::result::Result<QueryResult, FusionError> {
        let statements = parse_sql(query)
            .map_err(|e| FusionError::Execution(format!("Parse Error: {:?}", e)))?;
        let Some(stmt) = statements.first() else {
            return Ok(QueryResult::Success {
                message: "EMPTY".to_string(),
            });
        };

        let mut session = self.session.lock().await;
        if let Some(txn) = session.transaction.as_mut() {
            self.executor
                .execute_in_transaction_with_params(stmt, &mut **txn, params)
                .await
        } else {
            drop(session);
            let mut txn = self.storage.begin_transaction().await?;
            let res = self
                .executor
                .execute_in_transaction_with_params(stmt, &mut *txn, params)
                .await;
            if res.is_ok() {
                let _ = txn.commit().await;
            } else {
                let _ = txn.rollback().await;
            }
            res
        }
    }
}

/// Auth source that validates passwords against configured credentials and RBAC records.
struct FusionAuthSource {
    password: String,
    storage: Arc<dyn Storage>,
}

impl std::fmt::Debug for FusionAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusionAuthSource")
            .field("password", &"<redacted>")
            .field("storage", &"<storage>")
            .finish()
    }
}

impl FusionAuthSource {
    async fn authenticate(&self, login: &LoginInfo<'_>, password: &str) -> PgWireResult<()> {
        let username = login.user().unwrap_or_default();
        if username.is_empty() || username.eq_ignore_ascii_case("postgres") {
            return if self.password == password {
                Ok(())
            } else {
                Err(PgWireError::InvalidPassword(username.to_string()))
            };
        }

        let mut txn = self.storage.begin_transaction().await.map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                "RBAC storage error: {:?}",
                e
            ))))
        })?;

        match crate::auth::get_user(&mut *txn, username)
            .await
            .map_err(|e| {
                PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                    "RBAC lookup error: {:?}",
                    e
                ))))
            })? {
            Some(user) if user.verify_password(password) => Ok(()),
            Some(_) | None => Err(PgWireError::InvalidPassword(username.to_string())),
        }
    }
}

#[derive(Debug)]
struct FusionStartupHandler {
    auth_source: Arc<FusionAuthSource>,
    parameter_provider: DefaultServerParameterProvider,
}

#[async_trait::async_trait]
impl StartupHandler for FusionStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let login_info = LoginInfo::from_client_info(client);
                let password = pwd.password;
                self.auth_source
                    .authenticate(&login_info, &password)
                    .await?;
                finish_authentication(client, &self.parameter_provider).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// Wrapper that implements PgWireServerHandlers, returning real handlers
/// instead of the default NoopHandler.
pub struct PgServerFactory {
    startup: Arc<FusionStartupHandler>,
    handler: Arc<PgHandler>,
}

impl pgwire::api::PgWireServerHandlers for PgServerFactory {
    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        self.startup.clone()
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for PgHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        eprintln!("PG Simple Query called: {}", query);

        let username = Self::username_for_client(client);
        if let Err(e) = self.executor.authorize_sql(&username, query).await {
            return Ok(vec![Response::Error(Box::new(Self::auth_error(format!(
                "Authorization Error: {:?}",
                e
            ))))]);
        }

        let mut responses = Vec::new();

        // Parse SQL
        let statements = match parse_sql(query) {
            Ok(stmts) => stmts,
            Err(e) => {
                return Ok(vec![Response::Error(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_string(),
                        "42000".to_string(),
                        format!("Parse Error: {:?}", e),
                    ),
                ))])
            }
        };

        let mut session = self.session.lock().await;

        for stmt in statements {
            // Handle Transaction Control Statements
            match stmt {
                Statement::StartTransaction { .. } => {
                    if session.transaction.is_some() {
                        responses.push(Response::Execution(Tag::new(
                            "WARNING: Transaction already in progress",
                        )));
                    } else {
                        match self.storage.begin_transaction().await {
                            Ok(txn) => {
                                session.transaction = Some(txn);
                                responses.push(Response::Execution(Tag::new("BEGIN")));
                            }
                            Err(e) => {
                                return Ok(vec![Response::Error(Box::new(
                                    pgwire::error::ErrorInfo::new(
                                        "ERROR".to_string(),
                                        "XX000".to_string(),
                                        format!("Failed to begin transaction: {:?}", e),
                                    ),
                                ))]);
                            }
                        }
                    }
                    continue;
                }
                Statement::Commit { .. } => {
                    if let Some(txn) = session.transaction.take() {
                        match txn.commit().await {
                            Ok(_) => responses.push(Response::Execution(Tag::new("COMMIT"))),
                            Err(e) => {
                                return Ok(vec![Response::Error(Box::new(
                                    pgwire::error::ErrorInfo::new(
                                        "ERROR".to_string(),
                                        "XX000".to_string(),
                                        format!("Failed to commit transaction: {:?}", e),
                                    ),
                                ))]);
                            }
                        }
                    } else {
                        responses.push(Response::Execution(Tag::new(
                            "WARNING: There is no transaction in progress",
                        )));
                    }
                    continue;
                }
                Statement::Rollback { .. } => {
                    if let Some(txn) = session.transaction.take() {
                        match txn.rollback().await {
                            Ok(_) => responses.push(Response::Execution(Tag::new("ROLLBACK"))),
                            Err(e) => {
                                return Ok(vec![Response::Error(Box::new(
                                    pgwire::error::ErrorInfo::new(
                                        "ERROR".to_string(),
                                        "XX000".to_string(),
                                        format!("Failed to rollback transaction: {:?}", e),
                                    ),
                                ))]);
                            }
                        }
                    } else {
                        responses.push(Response::Execution(Tag::new(
                            "WARNING: There is no transaction in progress",
                        )));
                    }
                    continue;
                }
                _ => {}
            }

            // Execute Normal Statements
            let result = if let Some(txn) = session.transaction.as_mut() {
                // Execute in current transaction
                self.executor
                    .execute_in_transaction(&stmt, &mut **txn)
                    .await
            } else {
                // Execute in implicit transaction
                self.executor.execute(&stmt).await
            };

            match result {
                Ok(res) => match res {
                    QueryResult::Select { columns, rows } => {
                        let fields = Self::infer_text_fields(&columns, &rows);
                        let mut data_rows = Vec::with_capacity(rows.len());
                        for row in rows {
                            data_rows.push(Self::encode_row(fields.clone(), row)?);
                        }

                        responses.push(Response::Query(QueryResponse::new(
                            fields,
                            futures::stream::iter(data_rows.into_iter().map(Ok)),
                        )));
                    }
                    QueryResult::Success { message } => {
                        responses.push(Response::Execution(Tag::new(&message)));
                    }
                },
                Err(e) => {
                    return Ok(vec![Response::Error(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "XX000".to_string(),
                            format!("Execution Error: {:?}", e),
                        ),
                    ))]);
                }
            }
        }

        Ok(responses)
    }
}

#[async_trait::async_trait]
impl ExtendedQueryHandler for PgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Err(PgWireError::ApiError(Box::new(std::io::Error::other(
            "do_query not implemented",
        ))))
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        let username = Self::username_for_client(client);
        if let Err(e) = self.executor.authorize_sql(&username, &message.query).await {
            client
                .send(PgWireBackendMessage::ErrorResponse(
                    Self::auth_error(format!("Authorization Error: {:?}", e)).into(),
                ))
                .await
                .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
            return Ok(());
        }

        let mut session = self.session.lock().await;
        let name = message.name.clone().unwrap_or_default();
        let parameter_types =
            Self::infer_parameter_types_from_query(&message.query, &message.type_oids);
        session.statements.insert(
            name,
            StatementData {
                query: message.query.clone(),
                parameter_types,
            },
        );
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await
            .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        let mut session = self.session.lock().await;
        let statement_name = message.statement_name.clone().unwrap_or_default();

        let query = if let Some(q) = session.statements.get(&statement_name) {
            q.query.clone()
        } else if statement_name.is_empty() {
            "".to_string()
        } else {
            return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Statement {} not found", statement_name),
            ))));
        };

        if query.is_empty() && !statement_name.is_empty() {
            return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Empty query",
            ))));
        }

        let parameter_types = session
            .statements
            .get(&statement_name)
            .map(|statement| statement.parameter_types.clone())
            .unwrap_or_default();

        let mut params = Vec::with_capacity(message.parameters.len());
        for (idx, param_bytes) in message.parameters.iter().enumerate() {
            if let Some(bytes) = param_bytes {
                let param_type = parameter_types.get(idx).unwrap_or(&Type::TEXT);
                if message
                    .parameter_format_codes
                    .get(idx)
                    .copied()
                    .or_else(|| message.parameter_format_codes.first().copied())
                    .unwrap_or(0)
                    == 1
                {
                    params.push(Self::decode_binary_param(bytes, param_type));
                } else {
                    params.push(Self::decode_text_param(bytes, param_type));
                }
            } else {
                params.push(Value::Null);
            }
        }

        let portal_name = message.portal_name.clone().unwrap_or_default();
        session.portals.insert(
            portal_name,
            PortalData {
                statement_name,
                query,
                params,
            },
        );

        client
            .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await
            .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
        Ok(())
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        let portal_name = message.name.clone().unwrap_or_default();
        let (query, params) = {
            let session = self.session.lock().await;
            if let Some(portal) = session.portals.get(&portal_name) {
                (portal.query.clone(), portal.params.clone())
            } else {
                return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Portal {} not found", portal_name),
                ))));
            }
        };

        println!(
            "PG Execute Portal {}: {} params={:?}",
            portal_name, query, params
        );

        let username = Self::username_for_client(client);
        if let Err(e) = self.executor.authorize_sql(&username, &query).await {
            client
                .send(PgWireBackendMessage::ErrorResponse(
                    Self::auth_error(format!("Authorization Error: {:?}", e)).into(),
                ))
                .await
                .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
            return Ok(());
        }

        let result = self.execute_first_statement(&query, &params).await;

        match result {
            Ok(res) => match res {
                QueryResult::Select { columns, rows } => {
                    let described_fields = self.describe_query_fields(&query).await?;
                    let fields = if described_fields.is_empty() {
                        Self::infer_binary_fields(&columns, &rows)
                    } else {
                        Arc::new(
                            described_fields
                                .into_iter()
                                .map(|field| {
                                    FieldInfo::new(
                                        field.name().to_string(),
                                        None,
                                        None,
                                        field.datatype().clone(),
                                        FieldFormat::Binary,
                                    )
                                })
                                .collect::<Vec<_>>(),
                        )
                    };

                    for row in rows {
                        client
                            .send(PgWireBackendMessage::DataRow(Self::encode_binary_row(
                                fields.clone(),
                                row,
                            )))
                            .await
                            .map_err(|_| {
                                PgWireError::IoError(std::io::Error::other("Sink Error"))
                            })?;
                    }

                    client
                        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                            "SELECT".to_string(),
                        )))
                        .await
                        .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
                }
                QueryResult::Success { message } => {
                    client
                        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                            message,
                        )))
                        .await
                        .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
                }
            },
            Err(e) => {
                client
                    .send(PgWireBackendMessage::ErrorResponse(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "XX000".to_string(),
                            format!("Execution Error: {:?}", e),
                        )
                        .into(),
                    ))
                    .await
                    .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
            }
        }

        Ok(())
    }

    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        let transaction_status = {
            let session = self.session.lock().await;
            if session.transaction.is_some() {
                pgwire::messages::response::TransactionStatus::Transaction
            } else {
                pgwire::messages::response::TransactionStatus::Idle
            }
        };
        client
            .send(PgWireBackendMessage::ReadyForQuery(
                pgwire::messages::response::ReadyForQuery::new(transaction_status),
            ))
            .await
            .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
        Ok(())
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        let target_type = message.target_type;
        match target_type {
            b'S' => {
                let (parameter_types, query) = {
                    let session = self.session.lock().await;
                    let name = message.name.clone().unwrap_or_default();
                    if let Some(statement) = session.statements.get(&name) {
                        (statement.parameter_types.clone(), statement.query.clone())
                    } else {
                        (Vec::new(), String::new())
                    }
                };
                let fields = self.describe_query_fields(&query).await?;
                client
                    .send(PgWireBackendMessage::ParameterDescription(
                        ParameterDescription::new(
                            parameter_types.iter().map(Type::oid).collect::<Vec<_>>(),
                        ),
                    ))
                    .await
                    .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
                client
                    .send(PgWireBackendMessage::RowDescription(RowDescription::new(
                        fields.iter().map(Into::into).collect(),
                    )))
                    .await
                    .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
            }
            b'P' => {
                let query = {
                    let session = self.session.lock().await;
                    let name = message.name.clone().unwrap_or_default();
                    session
                        .portals
                        .get(&name)
                        .map(|portal| portal.query.clone())
                        .unwrap_or_default()
                };
                let fields = self.describe_query_fields(&query).await?;
                client
                    .send(PgWireBackendMessage::RowDescription(RowDescription::new(
                        fields.iter().map(Into::into).collect(),
                    )))
                    .await
                    .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn on_close<C>(&self, _client: &mut C, _message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        Ok(())
    }
}

pub async fn start_pg_server(
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    bind: &str,
    port: u16,
    password: &str,
    _tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
) {
    let addr = format!("{}:{}", bind, port);
    let listener = TcpListener::bind(&addr).await.unwrap();
    println!("FusionDB Postgres Server running on {}", addr);

    let password = password.to_string();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let executor = executor.clone();
        let storage = storage.clone();
        let password = password.clone();

        tokio::spawn(async move {
            let handler = Arc::new(PgHandler::new(executor, storage.clone()));
            let auth_source = Arc::new(FusionAuthSource { password, storage });
            let startup = Arc::new(FusionStartupHandler {
                auth_source,
                parameter_provider: DefaultServerParameterProvider::default(),
            });
            let factory = PgServerFactory { startup, handler };

            // pgwire 0.37 does not natively support TLS negotiation.
            // TLS for pgwire requires a TLS-terminating proxy (e.g., stunnel, HAProxy).
            let _ = pgwire::tokio::process_socket(stream, None, factory).await;
        });
    }
}

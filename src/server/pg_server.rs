use base64::Engine;
use bytes::{BufMut, BytesMut};
use chrono::Datelike;
use futures::{Sink, SinkExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore}; // Required for client.send

use pgwire::api::auth::{
    finish_authentication, protocol_negotiation, save_startup_parameters_to_metadata,
    DefaultServerParameterProvider, LoginInfo, StartupHandler,
};
use pgwire::api::copy::{self as pg_copy, CopyHandler};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{CopyResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::{ClientInfo, PgWireConnectionState, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::data::{NoData, ParameterDescription, RowDescription};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Close, Describe, Execute, Parse, ParseComplete, Sync as PgSync,
};
use pgwire::messages::response::{CommandComplete, ReadyForQuery, TransactionStatus};
use pgwire::messages::simplequery::Query as SimpleQuery;
use pgwire::messages::startup::Authentication;
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::PgWireFrontendMessage;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, Statement,
};

use crate::catalog::{Column, IndexType, TableSchema};
use crate::common::{FusionError, Value}; // Import FusionError
use crate::execution::{
    Executor, ForeignKeyMeta, QueryResult, SqlShardExtremum, SqlShardOwner, SqlShardRoutingDecision,
};
use crate::monitor;
use crate::parser::parse_sql;
use crate::storage::{Storage, Transaction};

const DEFAULT_MAX_CONNECTIONS: usize = 100;
const SHARD_OWNER_FORWARD_HEADER: &str = "x-fusiondb-forwarded";
const SHARD_OWNER_FORWARD_VALUE: &str = "shard-owner";

fn effective_max_connections(max_connections: usize) -> usize {
    max_connections.max(1)
}

#[derive(Clone)]
struct PgConnectionLimiter {
    max_connections: usize,
    semaphore: Arc<Semaphore>,
}

struct PgConnectionSlot {
    _permit: OwnedSemaphorePermit,
}

impl PgConnectionLimiter {
    fn new(max_connections: usize) -> Self {
        let max_connections = effective_max_connections(max_connections);
        Self {
            max_connections,
            semaphore: Arc::new(Semaphore::new(max_connections)),
        }
    }

    fn max_connections(&self) -> usize {
        self.max_connections
    }

    fn try_acquire(&self) -> Option<PgConnectionSlot> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                monitor::inc_pg_active_connection();
                Some(PgConnectionSlot { _permit: permit })
            }
            Err(_) => {
                monitor::inc_pg_connection_rejected();
                None
            }
        }
    }
}

impl Drop for PgConnectionSlot {
    fn drop(&mut self) {
        monitor::dec_pg_active_connection();
    }
}

struct Session {
    transaction: Option<Box<dyn Transaction>>,
    transaction_may_change_query_results: bool,
    statements: HashMap<String, StatementData>, // name -> statement data
    portals: HashMap<String, PortalData>,       // name -> portal data
    copy_in: Option<CopyInState>,
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
    result_format_codes: Vec<i16>,
}

struct CopyInState {
    statement: Statement,
    query: String,
    data: Vec<u8>,
    simple_query: bool,
}

#[derive(Clone, Copy)]
struct PgTypeInfo {
    oid: i64,
    name: &'static str,
    element_oid: Option<i64>,
    delimiter: char,
}

pub struct PgHandler {
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    query_parser: Arc<NoopQueryParser>,
    session: Arc<Mutex<Session>>,
    http_client: reqwest::Client,
}

enum PgShardWriteRouteAction {
    Local,
    Forward(SqlShardRoutingDecision),
    Conflict(String),
}

#[derive(Serialize)]
struct ForwardQueryRequest<'a> {
    sql: &'a str,
}

#[derive(Serialize)]
struct ForwardPrepareRequest<'a> {
    sql: &'a str,
}

#[derive(Serialize)]
struct ForwardExecuteRequest {
    statement_id: String,
    params: Vec<serde_json::Value>,
    return_results: Option<bool>,
}

#[derive(Serialize)]
struct ForwardCopyRequest<'a> {
    sql: &'a str,
    payload_base64: String,
}

#[derive(Deserialize)]
struct ForwardEnvelope<T> {
    data: Option<T>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct ForwardPreparedStatementInfo {
    statement_id: String,
}

#[derive(Deserialize)]
enum ForwardQueryResultJson {
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    },
    Success {
        message: String,
    },
}

#[derive(Clone, Copy)]
enum ForwardSum {
    Integer(i64),
    Float(f64),
}

impl PgHandler {
    const POSTGRES_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

    pub fn new(executor: Arc<Executor>, storage: Arc<dyn Storage>) -> Self {
        Self {
            executor,
            storage,
            query_parser: Arc::new(NoopQueryParser::new()),
            session: Arc::new(Mutex::new(Session {
                transaction: None,
                transaction_may_change_query_results: false,
                statements: HashMap::new(),
                portals: HashMap::new(),
                copy_in: None,
            })),
            http_client: reqwest::Client::new(),
        }
    }

    fn username_for_client<C: ClientInfo>(client: &C) -> String {
        LoginInfo::from_client_info(client)
            .user()
            .unwrap_or_default()
            .to_string()
    }

    fn is_postgresql_jdbc_client<C: ClientInfo>(client: &C) -> bool {
        client
            .metadata()
            .get("application_name")
            .map(|value| value.eq_ignore_ascii_case("PostgreSQL JDBC Driver"))
            .unwrap_or(false)
    }

    fn auth_error(message: impl Into<String>) -> pgwire::error::ErrorInfo {
        pgwire::error::ErrorInfo::new("ERROR".to_string(), "42501".to_string(), message.into())
    }

    fn execution_error(message: impl Into<String>) -> pgwire::error::ErrorInfo {
        pgwire::error::ErrorInfo::new("ERROR".to_string(), "XX000".to_string(), message.into())
    }

    fn shard_route_error(message: impl Into<String>) -> pgwire::error::ErrorInfo {
        pgwire::error::ErrorInfo::new("ERROR".to_string(), "0A000".to_string(), message.into())
    }

    fn fusion_error(prefix: &str, error: &FusionError) -> pgwire::error::ErrorInfo {
        pgwire::error::ErrorInfo::new(
            "ERROR".to_string(),
            Self::sqlstate_for_fusion_error(error).to_string(),
            format!("{}: {:?}", prefix, error),
        )
    }

    fn copy_error(error: FusionError) -> pgwire::error::ErrorInfo {
        match error {
            FusionError::ShardRouteConflict(message) => Self::shard_route_error(message),
            other => Self::execution_error(format!("COPY execution error: {:?}", other)),
        }
    }

    fn sqlstate_for_fusion_error(error: &FusionError) -> &'static str {
        match error {
            FusionError::ShardRouteConflict(_) => "0A000",
            FusionError::Storage(message) if message.starts_with("Write conflict:") => "40001",
            _ => "XX000",
        }
    }

    fn sink_error() -> PgWireError {
        PgWireError::IoError(std::io::Error::other("Sink Error"))
    }

    fn trace_enabled() -> bool {
        std::env::var("FUSIONDB_PGWIRE_TRACE")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false)
    }

    fn trace(message: impl AsRef<str>) {
        if Self::trace_enabled() {
            eprintln!("[pgwire-trace] {}", message.as_ref());
        }
    }

    fn trace_query(event: &str, query: &str) {
        if Self::trace_enabled() {
            let compact = query.split_whitespace().collect::<Vec<_>>().join(" ");
            let preview = if compact.len() > 240 {
                format!("{}...", &compact[..240])
            } else {
                compact
            };
            eprintln!("[pgwire-trace] {event}: {preview}");
        }
    }

    async fn shard_route_conflict_message_for_sql(
        &self,
        query: &str,
        params: &[Value],
    ) -> std::result::Result<Option<String>, FusionError> {
        let statements = parse_sql(query)
            .map_err(|e| FusionError::Execution(format!("Parse Error: {:?}", e)))?;
        let mut session = self.session.lock().await;
        if let Some(txn) = session.transaction.as_mut() {
            return self
                .shard_route_conflict_message_for_statements_in_transaction(
                    &statements,
                    &mut **txn,
                    params,
                )
                .await;
        }
        drop(session);
        self.shard_route_conflict_message_for_statements(&statements, params)
            .await
    }

    async fn shard_write_route_action_for_sql(
        &self,
        query: &str,
        params: &[Value],
    ) -> std::result::Result<PgShardWriteRouteAction, FusionError> {
        let statements = parse_sql(query)
            .map_err(|e| FusionError::Execution(format!("Parse Error: {:?}", e)))?;
        self.shard_write_route_action_for_statements(&statements, params)
            .await
    }

    async fn shard_read_route_decision_for_sql(
        &self,
        query: &str,
        params: &[Value],
    ) -> std::result::Result<Option<SqlShardRoutingDecision>, FusionError> {
        let statements = parse_sql(query)
            .map_err(|e| FusionError::Execution(format!("Parse Error: {:?}", e)))?;
        self.shard_read_route_decision_for_statements(&statements, params)
            .await
    }

    async fn shard_route_conflict_message_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> std::result::Result<Option<String>, FusionError> {
        let decisions = self
            .executor
            .shard_routing_decisions_for_statements(statements, params)
            .await?;
        Ok(Self::non_local_shard_route_message(decisions))
    }

    async fn shard_route_conflict_message_for_statements_in_transaction(
        &self,
        statements: &[Statement],
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> std::result::Result<Option<String>, FusionError> {
        let decisions = self
            .executor
            .shard_routing_decisions_for_statements_in_transaction(statements, txn, params)
            .await?;
        Ok(Self::non_local_shard_route_message(decisions))
    }

    async fn shard_read_route_conflict_message_for_statements_in_transaction(
        &self,
        statements: &[Statement],
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> std::result::Result<Option<String>, FusionError> {
        let decision = self
            .executor
            .shard_read_route_decision_for_statements_in_transaction(statements, txn, params)
            .await?;
        Ok(Self::non_local_shard_read_route_message(decision))
    }

    fn non_local_shard_route_message(decisions: Vec<SqlShardRoutingDecision>) -> Option<String> {
        decisions
            .into_iter()
            .find(|decision| !decision.is_local_owner())
            .map(|decision| Self::shard_route_conflict_message(&decision))
    }

    fn non_local_shard_read_route_message(
        decision: Option<SqlShardRoutingDecision>,
    ) -> Option<String> {
        decision
            .filter(|decision| !decision.is_local_owner())
            .map(|decision| Self::shard_route_conflict_message(&decision))
    }

    fn shard_route_conflict_message(decision: &SqlShardRoutingDecision) -> String {
        format!(
            "Shard route conflict: {} on table {} with shard key {} routes to shard {} owned by node {} at {}; local node {} is not the owner for this routed operation",
            decision.operation,
            decision.route.table,
            decision.route.shard_key,
            decision.route.shard_id,
            decision.route.owner_node_id,
            decision.route.owner_addr,
            decision.local_node_id
        )
    }

    async fn shard_write_route_action_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> std::result::Result<PgShardWriteRouteAction, FusionError> {
        let decisions = self
            .executor
            .shard_routing_decisions_for_statements(statements, params)
            .await?;
        Ok(Self::shard_write_route_action(decisions))
    }

    async fn shard_read_route_decision_for_statements(
        &self,
        statements: &[Statement],
        params: &[Value],
    ) -> std::result::Result<Option<SqlShardRoutingDecision>, FusionError> {
        self.executor
            .shard_read_route_decision_for_statements(statements, params)
            .await
    }

    fn shard_write_route_action(
        decisions: Vec<SqlShardRoutingDecision>,
    ) -> PgShardWriteRouteAction {
        let non_local: Vec<_> = decisions
            .iter()
            .filter(|decision| !decision.is_local_owner())
            .collect();
        if non_local.is_empty() {
            return PgShardWriteRouteAction::Local;
        }

        let first = non_local[0];
        if decisions
            .iter()
            .any(SqlShardRoutingDecision::is_local_owner)
        {
            return PgShardWriteRouteAction::Conflict(Self::multi_shard_route_conflict_message(
                first,
                "mixed local and non-local shard-owner writes require distributed transactions and are not yet supported",
            ));
        }

        if non_local.iter().any(|decision| {
            decision.route.owner_node_id != first.route.owner_node_id
                || decision.route.owner_addr != first.route.owner_addr
        }) {
            return PgShardWriteRouteAction::Conflict(Self::multi_shard_route_conflict_message(
                first,
                "multi-owner shard writes require distributed transactions and are not yet supported",
            ));
        }

        PgShardWriteRouteAction::Forward((*first).clone())
    }

    fn multi_shard_route_conflict_message(
        decision: &SqlShardRoutingDecision,
        reason: &str,
    ) -> String {
        format!(
            "{}; {}",
            Self::shard_route_conflict_message(decision),
            reason
        )
    }

    async fn forward_simple_query_to_shard_owner(
        &self,
        query: &str,
        username: &str,
        decision: &SqlShardRoutingDecision,
    ) -> PgWireResult<Vec<Response>> {
        let route_conflict = Self::shard_route_conflict_message(decision);
        let url = format!("http://{}/query", decision.route.owner_addr);
        let mut request = self
            .http_client
            .post(&url)
            .header(SHARD_OWNER_FORWARD_HEADER, SHARD_OWNER_FORWARD_VALUE)
            .json(&ForwardQueryRequest { sql: query });
        if !username.is_empty() {
            request = request.header("x-fusiondb-user", username);
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(e) => {
                return Ok(vec![Response::Error(Box::new(Self::shard_route_error(
                    format!(
                        "{}; shard owner forwarding to node {} at {} failed: {}",
                        route_conflict, decision.route.owner_node_id, decision.route.owner_addr, e
                    ),
                )))]);
            }
        };
        let status = response.status();
        let envelope = match response
            .json::<ForwardEnvelope<Vec<ForwardQueryResultJson>>>()
            .await
        {
            Ok(envelope) => envelope,
            Err(e) => {
                return Ok(vec![Response::Error(Box::new(Self::shard_route_error(
                    format!(
                        "{}; shard owner forwarding response from node {} at {} could not be decoded: {}",
                        route_conflict,
                        decision.route.owner_node_id,
                        decision.route.owner_addr,
                        e
                    ),
                )))]);
            }
        };

        if !status.is_success() {
            let message = envelope.error.unwrap_or_else(|| {
                format!(
                    "Shard owner forwarding error: node {} at {} returned HTTP {}",
                    decision.route.owner_node_id, decision.route.owner_addr, status
                )
            });
            let error = if message.contains("Shard route conflict") {
                Self::shard_route_error(message)
            } else {
                Self::execution_error(message)
            };
            return Ok(vec![Response::Error(Box::new(error))]);
        }

        let Some(results) = envelope.data else {
            return Ok(vec![Response::Error(Box::new(Self::execution_error(
                "Shard owner forwarding error: response did not include query results",
            )))]);
        };

        Self::responses_from_forwarded_query_results(results)
    }

    async fn forward_extended_query_to_shard_owner(
        &self,
        query: &str,
        params: &[Value],
        username: &str,
        decision: &SqlShardRoutingDecision,
    ) -> PgWireResult<std::result::Result<Vec<ForwardQueryResultJson>, pgwire::error::ErrorInfo>>
    {
        let prepare_url = format!("http://{}/prepare", decision.route.owner_addr);
        let prepare_response = match self
            .apply_forwarding_headers(self.http_client.post(&prepare_url), username)
            .json(&ForwardPrepareRequest { sql: query })
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                return Ok(Err(Self::shard_route_error(
                    Self::shard_owner_forwarding_transport_error(decision, "prepare", e),
                )));
            }
        };
        let prepare_status = prepare_response.status();
        let prepare_envelope = match prepare_response
            .json::<ForwardEnvelope<ForwardPreparedStatementInfo>>()
            .await
        {
            Ok(envelope) => envelope,
            Err(e) => {
                return Ok(Err(Self::shard_route_error(format!(
                    "{}; shard owner forwarding prepare response from node {} at {} could not be decoded: {}",
                    Self::shard_route_conflict_message(decision),
                    decision.route.owner_node_id,
                    decision.route.owner_addr,
                    e
                ))));
            }
        };
        if !prepare_status.is_success() {
            let message = prepare_envelope.error.unwrap_or_else(|| {
                format!(
                    "Shard owner forwarding prepare error: node {} at {} returned HTTP {}",
                    decision.route.owner_node_id, decision.route.owner_addr, prepare_status
                )
            });
            return Ok(Err(Self::forwarded_owner_error(message)));
        }
        let Some(prepared) = prepare_envelope.data else {
            return Ok(Err(Self::execution_error(
                "Shard owner forwarding prepare error: owner node returned no prepared statement",
            )));
        };

        let execute_url = format!("http://{}/execute", decision.route.owner_addr);
        let execute_payload = ForwardExecuteRequest {
            statement_id: prepared.statement_id.clone(),
            params: params.iter().map(Value::to_json).collect(),
            return_results: Some(true),
        };
        let execute_response = match self
            .apply_forwarding_headers(self.http_client.post(&execute_url), username)
            .json(&execute_payload)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                self.best_effort_deallocate_forwarded_statement(username, decision, &prepared)
                    .await;
                return Ok(Err(Self::shard_route_error(
                    Self::shard_owner_forwarding_transport_error(decision, "execute", e),
                )));
            }
        };
        let execute_status = execute_response.status();
        let execute_envelope = match execute_response
            .json::<ForwardEnvelope<Vec<ForwardQueryResultJson>>>()
            .await
        {
            Ok(envelope) => envelope,
            Err(e) => {
                self.best_effort_deallocate_forwarded_statement(username, decision, &prepared)
                    .await;
                return Ok(Err(Self::shard_route_error(format!(
                    "{}; shard owner forwarding execute response from node {} at {} could not be decoded: {}",
                    Self::shard_route_conflict_message(decision),
                    decision.route.owner_node_id,
                    decision.route.owner_addr,
                    e
                ))));
            }
        };
        self.best_effort_deallocate_forwarded_statement(username, decision, &prepared)
            .await;

        if !execute_status.is_success() {
            let message = execute_envelope.error.unwrap_or_else(|| {
                format!(
                    "Shard owner forwarding execute error: node {} at {} returned HTTP {}",
                    decision.route.owner_node_id, decision.route.owner_addr, execute_status
                )
            });
            return Ok(Err(Self::forwarded_owner_error(message)));
        }

        let Some(results) = execute_envelope.data else {
            return Ok(Err(Self::execution_error(
                "Shard owner forwarding execute error: response did not include query results",
            )));
        };
        Ok(Ok(results))
    }

    async fn forward_copy_to_shard_owner(
        &self,
        query: &str,
        payload: &[u8],
        username: &str,
        decision: &SqlShardRoutingDecision,
    ) -> PgWireResult<std::result::Result<usize, pgwire::error::ErrorInfo>> {
        let url = format!("http://{}/copy_stdin", decision.route.owner_addr);
        let request = ForwardCopyRequest {
            sql: query,
            payload_base64: base64::engine::general_purpose::STANDARD.encode(payload),
        };
        let response = match self
            .apply_forwarding_headers(self.http_client.post(&url), username)
            .json(&request)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                return Ok(Err(Self::shard_route_error(
                    Self::shard_owner_forwarding_transport_error(decision, "copy_stdin", e),
                )));
            }
        };
        let status = response.status();
        let envelope = match response
            .json::<ForwardEnvelope<Vec<ForwardQueryResultJson>>>()
            .await
        {
            Ok(envelope) => envelope,
            Err(e) => {
                return Ok(Err(Self::shard_route_error(format!(
                    "{}; shard owner forwarding copy_stdin response from node {} at {} could not be decoded: {}",
                    Self::shard_route_conflict_message(decision),
                    decision.route.owner_node_id,
                    decision.route.owner_addr,
                    e
                ))));
            }
        };

        if !status.is_success() {
            let message = envelope.error.unwrap_or_else(|| {
                format!(
                    "Shard owner forwarding copy_stdin error: node {} at {} returned HTTP {}",
                    decision.route.owner_node_id, decision.route.owner_addr, status
                )
            });
            return Ok(Err(Self::forwarded_owner_error(message)));
        }

        let Some(results) = envelope.data else {
            return Ok(Err(Self::execution_error(
                "Shard owner forwarding copy_stdin error: response did not include query results",
            )));
        };
        let Some(count) = results.into_iter().find_map(|result| match result {
            ForwardQueryResultJson::Success { message } => Self::first_i64_token(&message),
            ForwardQueryResultJson::Select { .. } => None,
        }) else {
            return Ok(Err(Self::execution_error(
                "Shard owner forwarding copy_stdin error: response did not include COPY count",
            )));
        };
        Ok(Ok(count as usize))
    }

    async fn fanout_simple_select_to_shard_owners(
        &self,
        query: &str,
        username: &str,
    ) -> PgWireResult<Option<Vec<Response>>> {
        let owners = match self
            .executor
            .shard_select_fanout_owners_for_sql(query, &[])
            .await
        {
            Ok(owners) if !owners.is_empty() => owners,
            Ok(_) => return Ok(None),
            Err(e) => {
                return Ok(Some(vec![Response::Error(Box::new(Self::fusion_error(
                    "Shard select fan-out planning error",
                    &e,
                )))]));
            }
        };

        let local_results = match self.executor.execute_sql(query).await {
            Ok(results) => Self::forward_results_from_query_results(results),
            Err(e) => {
                return Ok(Some(vec![Response::Error(Box::new(Self::fusion_error(
                    "Shard select fan-out local execution error",
                    &e,
                )))]));
            }
        };

        let mut columns = None;
        let mut rows = Vec::new();
        if let Err(error) =
            Self::append_forward_select_results(&mut columns, &mut rows, local_results)
        {
            return Ok(Some(vec![Response::Error(Box::new(error))]));
        }

        for owner in owners {
            let owner_results = match self
                .query_remote_shard_owner_results(query, username, &owner)
                .await?
            {
                Ok(results) => results,
                Err(error) => return Ok(Some(vec![Response::Error(Box::new(error))])),
            };
            if let Err(error) =
                Self::append_forward_select_results(&mut columns, &mut rows, owner_results)
            {
                return Ok(Some(vec![Response::Error(Box::new(error))]));
            }
        }

        Ok(Some(Self::responses_from_forwarded_query_results(vec![
            ForwardQueryResultJson::Select {
                columns: columns.unwrap_or_default(),
                rows,
            },
        ])?))
    }

    async fn fanout_count_select_to_shard_owners(
        &self,
        query: &str,
        username: &str,
    ) -> PgWireResult<Option<Vec<Response>>> {
        let owners = match self
            .executor
            .shard_count_select_fanout_owners_for_sql(query, &[])
            .await
        {
            Ok(owners) if !owners.is_empty() => owners,
            Ok(_) => return Ok(None),
            Err(e) => {
                return Ok(Some(vec![Response::Error(Box::new(Self::fusion_error(
                    "Shard count fan-out planning error",
                    &e,
                )))]));
            }
        };

        let local_results = match self.executor.execute_sql(query).await {
            Ok(results) => Self::forward_results_from_query_results(results),
            Err(e) => {
                return Ok(Some(vec![Response::Error(Box::new(Self::fusion_error(
                    "Shard count fan-out local execution error",
                    &e,
                )))]));
            }
        };
        let (columns, mut total) = match Self::count_from_forward_select_results(local_results) {
            Ok(count) => count,
            Err(error) => return Ok(Some(vec![Response::Error(Box::new(error))])),
        };

        for owner in owners {
            let owner_results = match self
                .query_remote_shard_owner_results(query, username, &owner)
                .await?
            {
                Ok(results) => results,
                Err(error) => return Ok(Some(vec![Response::Error(Box::new(error))])),
            };
            let (owner_columns, owner_count) =
                match Self::count_from_forward_select_results(owner_results) {
                    Ok(count) => count,
                    Err(error) => return Ok(Some(vec![Response::Error(Box::new(error))])),
                };
            if owner_columns != columns {
                return Ok(Some(vec![Response::Error(Box::new(
                    Self::execution_error(format!(
                        "Shard count fan-out column mismatch: expected {:?}, got {:?}",
                        columns, owner_columns
                    )),
                ))]));
            }
            total = match total.checked_add(owner_count) {
                Some(total) => total,
                None => {
                    return Ok(Some(vec![Response::Error(Box::new(
                        Self::execution_error("Shard count fan-out overflow"),
                    ))]));
                }
            };
        }

        Ok(Some(Self::responses_from_forwarded_query_results(vec![
            ForwardQueryResultJson::Select {
                columns,
                rows: vec![vec![serde_json::json!(total)]],
            },
        ])?))
    }

    async fn fanout_sum_select_to_shard_owners(
        &self,
        query: &str,
        username: &str,
    ) -> PgWireResult<Option<Vec<Response>>> {
        let owners = match self
            .executor
            .shard_sum_select_fanout_owners_for_sql(query, &[])
            .await
        {
            Ok(owners) if !owners.is_empty() => owners,
            Ok(_) => return Ok(None),
            Err(e) => {
                return Ok(Some(vec![Response::Error(Box::new(Self::fusion_error(
                    "Shard sum fan-out planning error",
                    &e,
                )))]));
            }
        };

        let local_results = match self.executor.execute_sql(query).await {
            Ok(results) => Self::forward_results_from_query_results(results),
            Err(e) => {
                return Ok(Some(vec![Response::Error(Box::new(Self::fusion_error(
                    "Shard sum fan-out local execution error",
                    &e,
                )))]));
            }
        };
        let (columns, mut total) = match Self::sum_from_forward_select_results(local_results) {
            Ok(sum) => sum,
            Err(error) => return Ok(Some(vec![Response::Error(Box::new(error))])),
        };

        for owner in owners {
            let owner_results = match self
                .query_remote_shard_owner_results(query, username, &owner)
                .await?
            {
                Ok(results) => results,
                Err(error) => return Ok(Some(vec![Response::Error(Box::new(error))])),
            };
            let (owner_columns, owner_sum) =
                match Self::sum_from_forward_select_results(owner_results) {
                    Ok(sum) => sum,
                    Err(error) => return Ok(Some(vec![Response::Error(Box::new(error))])),
                };
            if owner_columns != columns {
                return Ok(Some(vec![Response::Error(Box::new(
                    Self::execution_error(format!(
                        "Shard sum fan-out column mismatch: expected {:?}, got {:?}",
                        columns, owner_columns
                    )),
                ))]));
            }
            if let Err(error) = Self::add_forward_sum(&mut total, owner_sum) {
                return Ok(Some(vec![Response::Error(Box::new(error))]));
            }
        }

        Ok(Some(Self::responses_from_forwarded_query_results(vec![
            ForwardQueryResultJson::Select {
                columns,
                rows: vec![vec![Self::forward_sum_to_json(total)]],
            },
        ])?))
    }

    async fn fanout_min_max_select_to_shard_owners(
        &self,
        query: &str,
        username: &str,
    ) -> PgWireResult<Option<Vec<Response>>> {
        let kind = match self
            .executor
            .shard_min_max_select_fanout_kind_for_sql(query)
        {
            Ok(Some(kind)) => kind,
            Ok(None) => return Ok(None),
            Err(e) => {
                return Ok(Some(vec![Response::Error(Box::new(Self::fusion_error(
                    "Shard min/max fan-out planning error",
                    &e,
                )))]));
            }
        };
        let owners = match self
            .executor
            .shard_min_max_select_fanout_owners_for_sql(query, &[])
            .await
        {
            Ok(owners) if !owners.is_empty() => owners,
            Ok(_) => return Ok(None),
            Err(e) => {
                return Ok(Some(vec![Response::Error(Box::new(Self::fusion_error(
                    "Shard min/max fan-out planning error",
                    &e,
                )))]));
            }
        };

        let local_results = match self.executor.execute_sql(query).await {
            Ok(results) => Self::forward_results_from_query_results(results),
            Err(e) => {
                return Ok(Some(vec![Response::Error(Box::new(Self::fusion_error(
                    "Shard min/max fan-out local execution error",
                    &e,
                )))]));
            }
        };
        let (columns, mut total) = match Self::extremum_from_forward_select_results(local_results) {
            Ok(extremum) => extremum,
            Err(error) => return Ok(Some(vec![Response::Error(Box::new(error))])),
        };

        for owner in owners {
            let owner_results = match self
                .query_remote_shard_owner_results(query, username, &owner)
                .await?
            {
                Ok(results) => results,
                Err(error) => return Ok(Some(vec![Response::Error(Box::new(error))])),
            };
            let (owner_columns, owner_extremum) =
                match Self::extremum_from_forward_select_results(owner_results) {
                    Ok(extremum) => extremum,
                    Err(error) => return Ok(Some(vec![Response::Error(Box::new(error))])),
                };
            if owner_columns != columns {
                return Ok(Some(vec![Response::Error(Box::new(
                    Self::execution_error(format!(
                        "Shard min/max fan-out column mismatch: expected {:?}, got {:?}",
                        columns, owner_columns
                    )),
                ))]));
            }
            if let Err(error) = Self::merge_forward_extremum(&mut total, owner_extremum, kind) {
                return Ok(Some(vec![Response::Error(Box::new(error))]));
            }
        }

        Ok(Some(Self::responses_from_forwarded_query_results(vec![
            ForwardQueryResultJson::Select {
                columns,
                rows: vec![vec![total.unwrap_or(serde_json::Value::Null)]],
            },
        ])?))
    }

    async fn fanout_extended_select_to_shard_owners(
        &self,
        query: &str,
        params: &[Value],
        username: &str,
    ) -> PgWireResult<
        std::result::Result<Option<Vec<ForwardQueryResultJson>>, pgwire::error::ErrorInfo>,
    > {
        let owners = match self
            .executor
            .shard_select_fanout_owners_for_sql(query, params)
            .await
        {
            Ok(owners) if !owners.is_empty() => owners,
            Ok(_) => return Ok(Ok(None)),
            Err(e) => {
                return Ok(Err(Self::fusion_error(
                    "Shard select fan-out planning error",
                    &e,
                )));
            }
        };

        let local_results = match self.execute_first_statement(query, params).await {
            Ok(result) => Self::forward_results_from_query_results(vec![result]),
            Err(e) => {
                return Ok(Err(Self::fusion_error(
                    "Shard select fan-out local execution error",
                    &e,
                )));
            }
        };

        let mut columns = None;
        let mut rows = Vec::new();
        if let Err(error) =
            Self::append_forward_select_results(&mut columns, &mut rows, local_results)
        {
            return Ok(Err(error));
        }

        for owner in owners {
            let owner_results = match self
                .query_remote_prepared_shard_owner_results(query, params, username, &owner)
                .await?
            {
                Ok(results) => results,
                Err(error) => return Ok(Err(error)),
            };
            if let Err(error) =
                Self::append_forward_select_results(&mut columns, &mut rows, owner_results)
            {
                return Ok(Err(error));
            }
        }

        Ok(Ok(Some(vec![ForwardQueryResultJson::Select {
            columns: columns.unwrap_or_default(),
            rows,
        }])))
    }

    async fn fanout_extended_count_select_to_shard_owners(
        &self,
        query: &str,
        params: &[Value],
        username: &str,
    ) -> PgWireResult<
        std::result::Result<Option<Vec<ForwardQueryResultJson>>, pgwire::error::ErrorInfo>,
    > {
        let owners = match self
            .executor
            .shard_count_select_fanout_owners_for_sql(query, params)
            .await
        {
            Ok(owners) if !owners.is_empty() => owners,
            Ok(_) => return Ok(Ok(None)),
            Err(e) => {
                return Ok(Err(Self::fusion_error(
                    "Shard count fan-out planning error",
                    &e,
                )));
            }
        };

        let local_results = match self.execute_first_statement(query, params).await {
            Ok(result) => Self::forward_results_from_query_results(vec![result]),
            Err(e) => {
                return Ok(Err(Self::fusion_error(
                    "Shard count fan-out local execution error",
                    &e,
                )));
            }
        };
        let (columns, mut total) = match Self::count_from_forward_select_results(local_results) {
            Ok(count) => count,
            Err(error) => return Ok(Err(error)),
        };

        for owner in owners {
            let owner_results = match self
                .query_remote_prepared_shard_owner_results(query, params, username, &owner)
                .await?
            {
                Ok(results) => results,
                Err(error) => return Ok(Err(error)),
            };
            let (owner_columns, owner_count) =
                match Self::count_from_forward_select_results(owner_results) {
                    Ok(count) => count,
                    Err(error) => return Ok(Err(error)),
                };
            if owner_columns != columns {
                return Ok(Err(Self::execution_error(format!(
                    "Shard count fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ))));
            }
            total = match total.checked_add(owner_count) {
                Some(total) => total,
                None => return Ok(Err(Self::execution_error("Shard count fan-out overflow"))),
            };
        }

        Ok(Ok(Some(vec![ForwardQueryResultJson::Select {
            columns,
            rows: vec![vec![serde_json::json!(total)]],
        }])))
    }

    async fn fanout_extended_sum_select_to_shard_owners(
        &self,
        query: &str,
        params: &[Value],
        username: &str,
    ) -> PgWireResult<
        std::result::Result<Option<Vec<ForwardQueryResultJson>>, pgwire::error::ErrorInfo>,
    > {
        let owners = match self
            .executor
            .shard_sum_select_fanout_owners_for_sql(query, params)
            .await
        {
            Ok(owners) if !owners.is_empty() => owners,
            Ok(_) => return Ok(Ok(None)),
            Err(e) => {
                return Ok(Err(Self::fusion_error(
                    "Shard sum fan-out planning error",
                    &e,
                )));
            }
        };

        let local_results = match self.execute_first_statement(query, params).await {
            Ok(result) => Self::forward_results_from_query_results(vec![result]),
            Err(e) => {
                return Ok(Err(Self::fusion_error(
                    "Shard sum fan-out local execution error",
                    &e,
                )));
            }
        };
        let (columns, mut total) = match Self::sum_from_forward_select_results(local_results) {
            Ok(sum) => sum,
            Err(error) => return Ok(Err(error)),
        };

        for owner in owners {
            let owner_results = match self
                .query_remote_prepared_shard_owner_results(query, params, username, &owner)
                .await?
            {
                Ok(results) => results,
                Err(error) => return Ok(Err(error)),
            };
            let (owner_columns, owner_sum) =
                match Self::sum_from_forward_select_results(owner_results) {
                    Ok(sum) => sum,
                    Err(error) => return Ok(Err(error)),
                };
            if owner_columns != columns {
                return Ok(Err(Self::execution_error(format!(
                    "Shard sum fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ))));
            }
            if let Err(error) = Self::add_forward_sum(&mut total, owner_sum) {
                return Ok(Err(error));
            }
        }

        Ok(Ok(Some(vec![ForwardQueryResultJson::Select {
            columns,
            rows: vec![vec![Self::forward_sum_to_json(total)]],
        }])))
    }

    async fn fanout_extended_min_max_select_to_shard_owners(
        &self,
        query: &str,
        params: &[Value],
        username: &str,
    ) -> PgWireResult<
        std::result::Result<Option<Vec<ForwardQueryResultJson>>, pgwire::error::ErrorInfo>,
    > {
        let kind = match self
            .executor
            .shard_min_max_select_fanout_kind_for_sql(query)
        {
            Ok(Some(kind)) => kind,
            Ok(None) => return Ok(Ok(None)),
            Err(e) => {
                return Ok(Err(Self::fusion_error(
                    "Shard min/max fan-out planning error",
                    &e,
                )));
            }
        };
        let owners = match self
            .executor
            .shard_min_max_select_fanout_owners_for_sql(query, params)
            .await
        {
            Ok(owners) if !owners.is_empty() => owners,
            Ok(_) => return Ok(Ok(None)),
            Err(e) => {
                return Ok(Err(Self::fusion_error(
                    "Shard min/max fan-out planning error",
                    &e,
                )));
            }
        };

        let local_results = match self.execute_first_statement(query, params).await {
            Ok(result) => Self::forward_results_from_query_results(vec![result]),
            Err(e) => {
                return Ok(Err(Self::fusion_error(
                    "Shard min/max fan-out local execution error",
                    &e,
                )));
            }
        };
        let (columns, mut total) = match Self::extremum_from_forward_select_results(local_results) {
            Ok(extremum) => extremum,
            Err(error) => return Ok(Err(error)),
        };

        for owner in owners {
            let owner_results = match self
                .query_remote_prepared_shard_owner_results(query, params, username, &owner)
                .await?
            {
                Ok(results) => results,
                Err(error) => return Ok(Err(error)),
            };
            let (owner_columns, owner_extremum) =
                match Self::extremum_from_forward_select_results(owner_results) {
                    Ok(extremum) => extremum,
                    Err(error) => return Ok(Err(error)),
                };
            if owner_columns != columns {
                return Ok(Err(Self::execution_error(format!(
                    "Shard min/max fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ))));
            }
            if let Err(error) = Self::merge_forward_extremum(&mut total, owner_extremum, kind) {
                return Ok(Err(error));
            }
        }

        Ok(Ok(Some(vec![ForwardQueryResultJson::Select {
            columns,
            rows: vec![vec![total.unwrap_or(serde_json::Value::Null)]],
        }])))
    }

    async fn query_remote_shard_owner_results(
        &self,
        query: &str,
        username: &str,
        owner: &SqlShardOwner,
    ) -> PgWireResult<std::result::Result<Vec<ForwardQueryResultJson>, pgwire::error::ErrorInfo>>
    {
        let url = format!("http://{}/query", owner.addr);
        let response = match self
            .apply_forwarding_headers(self.http_client.post(&url), username)
            .json(&ForwardQueryRequest { sql: query })
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                return Ok(Err(Self::shard_route_error(format!(
                    "Shard select fan-out error: forwarding to node {} at {} failed: {}",
                    owner.node_id, owner.addr, e
                ))));
            }
        };
        let status = response.status();
        let envelope = match response
            .json::<ForwardEnvelope<Vec<ForwardQueryResultJson>>>()
            .await
        {
            Ok(envelope) => envelope,
            Err(e) => {
                return Ok(Err(Self::shard_route_error(format!(
                    "Shard select fan-out error: response from node {} at {} could not be decoded: {}",
                    owner.node_id, owner.addr, e
                ))));
            }
        };
        if !status.is_success() {
            return Ok(Err(Self::forwarded_owner_error(
                envelope.error.unwrap_or_else(|| {
                    format!(
                        "Shard select fan-out error: node {} at {} returned HTTP {}",
                        owner.node_id, owner.addr, status
                    )
                }),
            )));
        }
        let Some(results) = envelope.data else {
            return Ok(Err(Self::execution_error(format!(
                "Shard select fan-out error: node {} at {} returned no query results",
                owner.node_id, owner.addr
            ))));
        };
        Ok(Ok(results))
    }

    async fn query_remote_prepared_shard_owner_results(
        &self,
        query: &str,
        params: &[Value],
        username: &str,
        owner: &SqlShardOwner,
    ) -> PgWireResult<std::result::Result<Vec<ForwardQueryResultJson>, pgwire::error::ErrorInfo>>
    {
        let prepare_url = format!("http://{}/prepare", owner.addr);
        let prepare_response = match self
            .apply_forwarding_headers(self.http_client.post(&prepare_url), username)
            .json(&ForwardPrepareRequest { sql: query })
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                return Ok(Err(Self::shard_route_error(format!(
                    "Shard select fan-out prepare error: forwarding to node {} at {} failed: {}",
                    owner.node_id, owner.addr, e
                ))));
            }
        };
        let prepare_status = prepare_response.status();
        let prepare_envelope = match prepare_response
            .json::<ForwardEnvelope<ForwardPreparedStatementInfo>>()
            .await
        {
            Ok(envelope) => envelope,
            Err(e) => {
                return Ok(Err(Self::shard_route_error(format!(
                    "Shard select fan-out prepare error: response from node {} at {} could not be decoded: {}",
                    owner.node_id, owner.addr, e
                ))));
            }
        };
        if !prepare_status.is_success() {
            return Ok(Err(Self::forwarded_owner_error(
                prepare_envelope.error.unwrap_or_else(|| {
                    format!(
                        "Shard select fan-out prepare error: node {} at {} returned HTTP {}",
                        owner.node_id, owner.addr, prepare_status
                    )
                }),
            )));
        }
        let Some(prepared) = prepare_envelope.data else {
            return Ok(Err(Self::execution_error(format!(
                "Shard select fan-out prepare error: node {} at {} returned no prepared statement",
                owner.node_id, owner.addr
            ))));
        };

        let execute_url = format!("http://{}/execute", owner.addr);
        let execute_payload = ForwardExecuteRequest {
            statement_id: prepared.statement_id.clone(),
            params: params.iter().map(Value::to_json).collect(),
            return_results: Some(true),
        };
        let execute_response = match self
            .apply_forwarding_headers(self.http_client.post(&execute_url), username)
            .json(&execute_payload)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                self.best_effort_deallocate_prepared_on_owner(username, owner, &prepared)
                    .await;
                return Ok(Err(Self::shard_route_error(format!(
                    "Shard select fan-out execute error: forwarding to node {} at {} failed: {}",
                    owner.node_id, owner.addr, e
                ))));
            }
        };
        let execute_status = execute_response.status();
        let execute_envelope = match execute_response
            .json::<ForwardEnvelope<Vec<ForwardQueryResultJson>>>()
            .await
        {
            Ok(envelope) => envelope,
            Err(e) => {
                self.best_effort_deallocate_prepared_on_owner(username, owner, &prepared)
                    .await;
                return Ok(Err(Self::shard_route_error(format!(
                    "Shard select fan-out execute error: response from node {} at {} could not be decoded: {}",
                    owner.node_id, owner.addr, e
                ))));
            }
        };
        self.best_effort_deallocate_prepared_on_owner(username, owner, &prepared)
            .await;

        if !execute_status.is_success() {
            return Ok(Err(Self::forwarded_owner_error(
                execute_envelope.error.unwrap_or_else(|| {
                    format!(
                        "Shard select fan-out execute error: node {} at {} returned HTTP {}",
                        owner.node_id, owner.addr, execute_status
                    )
                }),
            )));
        }
        let Some(results) = execute_envelope.data else {
            return Ok(Err(Self::execution_error(format!(
                "Shard select fan-out execute error: node {} at {} returned no query results",
                owner.node_id, owner.addr
            ))));
        };
        Ok(Ok(results))
    }

    fn forward_results_from_query_results(
        results: Vec<QueryResult>,
    ) -> Vec<ForwardQueryResultJson> {
        results
            .into_iter()
            .map(|result| match result {
                QueryResult::Select { columns, rows } => ForwardQueryResultJson::Select {
                    columns,
                    rows: rows
                        .into_iter()
                        .map(|row| row.iter().map(Value::to_json).collect())
                        .collect(),
                },
                QueryResult::Success { message } => ForwardQueryResultJson::Success { message },
            })
            .collect()
    }

    fn append_forward_select_results(
        columns: &mut Option<Vec<String>>,
        rows: &mut Vec<Vec<serde_json::Value>>,
        results: Vec<ForwardQueryResultJson>,
    ) -> std::result::Result<(), pgwire::error::ErrorInfo> {
        let [result] = results.as_slice() else {
            return Err(Self::execution_error(
                "Shard select fan-out expected exactly one SELECT result",
            ));
        };
        let ForwardQueryResultJson::Select {
            columns: result_columns,
            rows: result_rows,
        } = result
        else {
            return Err(Self::execution_error(
                "Shard select fan-out received a non-SELECT result",
            ));
        };
        if let Some(columns) = columns.as_ref() {
            if columns != result_columns {
                return Err(Self::execution_error(format!(
                    "Shard select fan-out column mismatch: expected {:?}, got {:?}",
                    columns, result_columns
                )));
            }
        } else {
            *columns = Some(result_columns.clone());
        }
        rows.extend(result_rows.iter().cloned());
        Ok(())
    }

    fn count_from_forward_select_results(
        results: Vec<ForwardQueryResultJson>,
    ) -> std::result::Result<(Vec<String>, i64), pgwire::error::ErrorInfo> {
        let [result] = results.as_slice() else {
            return Err(Self::execution_error(
                "Shard count fan-out expected exactly one SELECT result",
            ));
        };
        let ForwardQueryResultJson::Select { columns, rows } = result else {
            return Err(Self::execution_error(
                "Shard count fan-out received a non-SELECT result",
            ));
        };
        if columns.len() != 1 || rows.len() != 1 || rows[0].len() != 1 {
            return Err(Self::execution_error(
                "Shard count fan-out expected one row with one count column",
            ));
        }
        let value = &rows[0][0];
        let count = value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|count| i64::try_from(count).ok()));
        let Some(count) = count else {
            return Err(Self::execution_error(format!(
                "Shard count fan-out received a non-integer count value: {}",
                value
            )));
        };
        Ok((columns.clone(), count))
    }

    fn sum_from_forward_select_results(
        results: Vec<ForwardQueryResultJson>,
    ) -> std::result::Result<(Vec<String>, Option<ForwardSum>), pgwire::error::ErrorInfo> {
        let [result] = results.as_slice() else {
            return Err(Self::execution_error(
                "Shard sum fan-out expected exactly one SELECT result",
            ));
        };
        let ForwardQueryResultJson::Select { columns, rows } = result else {
            return Err(Self::execution_error(
                "Shard sum fan-out received a non-SELECT result",
            ));
        };
        if columns.len() != 1 || rows.len() != 1 || rows[0].len() != 1 {
            return Err(Self::execution_error(
                "Shard sum fan-out expected one row with one sum column",
            ));
        }
        Ok((columns.clone(), Self::forward_sum_json_value(&rows[0][0])?))
    }

    fn forward_sum_json_value(
        value: &serde_json::Value,
    ) -> std::result::Result<Option<ForwardSum>, pgwire::error::ErrorInfo> {
        if value.is_null() {
            return Ok(None);
        }
        if let Some(value) = value.as_i64() {
            return Ok(Some(ForwardSum::Integer(value)));
        }
        if let Some(value) = value.as_u64() {
            let value = i64::try_from(value).map_err(|_| {
                Self::execution_error(format!(
                    "Shard sum fan-out received an out-of-range integer sum value: {}",
                    value
                ))
            })?;
            return Ok(Some(ForwardSum::Integer(value)));
        }
        if let Some(value) = value.as_f64() {
            if value.is_finite() {
                return Ok(Some(ForwardSum::Float(value)));
            }
        }
        Err(Self::execution_error(format!(
            "Shard sum fan-out received a non-numeric sum value: {}",
            value
        )))
    }

    fn add_forward_sum(
        total: &mut Option<ForwardSum>,
        value: Option<ForwardSum>,
    ) -> std::result::Result<(), pgwire::error::ErrorInfo> {
        let Some(value) = value else {
            return Ok(());
        };
        *total = Some(match (*total, value) {
            (None, value) => value,
            (Some(ForwardSum::Integer(left)), ForwardSum::Integer(right)) => ForwardSum::Integer(
                left.checked_add(right)
                    .ok_or_else(|| Self::execution_error("Shard sum fan-out overflow"))?,
            ),
            (Some(ForwardSum::Integer(left)), ForwardSum::Float(right)) => {
                ForwardSum::Float(left as f64 + right)
            }
            (Some(ForwardSum::Float(left)), ForwardSum::Integer(right)) => {
                ForwardSum::Float(left + right as f64)
            }
            (Some(ForwardSum::Float(left)), ForwardSum::Float(right)) => {
                ForwardSum::Float(left + right)
            }
        });
        Ok(())
    }

    fn forward_sum_to_json(total: Option<ForwardSum>) -> serde_json::Value {
        match total {
            None => serde_json::Value::Null,
            Some(ForwardSum::Integer(value)) => serde_json::json!(value),
            Some(ForwardSum::Float(value)) => serde_json::json!(value),
        }
    }

    fn extremum_from_forward_select_results(
        results: Vec<ForwardQueryResultJson>,
    ) -> std::result::Result<(Vec<String>, Option<serde_json::Value>), pgwire::error::ErrorInfo>
    {
        let [result] = results.as_slice() else {
            return Err(Self::execution_error(
                "Shard min/max fan-out expected exactly one SELECT result",
            ));
        };
        let ForwardQueryResultJson::Select { columns, rows } = result else {
            return Err(Self::execution_error(
                "Shard min/max fan-out received a non-SELECT result",
            ));
        };
        if columns.len() != 1 || rows.len() != 1 || rows[0].len() != 1 {
            return Err(Self::execution_error(
                "Shard min/max fan-out expected one row with one min/max column",
            ));
        }
        let value = &rows[0][0];
        if value.is_null() {
            return Ok((columns.clone(), None));
        }
        if value.as_f64().is_some_and(|value| value.is_finite()) {
            return Ok((columns.clone(), Some(value.clone())));
        }
        Err(Self::execution_error(format!(
            "Shard min/max fan-out received a non-numeric min/max value: {}",
            value
        )))
    }

    fn merge_forward_extremum(
        total: &mut Option<serde_json::Value>,
        value: Option<serde_json::Value>,
        kind: SqlShardExtremum,
    ) -> std::result::Result<(), pgwire::error::ErrorInfo> {
        let Some(value) = value else {
            return Ok(());
        };
        let Some(current) = total.as_ref() else {
            *total = Some(value);
            return Ok(());
        };
        let candidate_value = Value::from_json(&value);
        let current_value = Value::from_json(current);
        let should_replace = match kind {
            SqlShardExtremum::Min => candidate_value.compare(&current_value).is_lt(),
            SqlShardExtremum::Max => candidate_value.compare(&current_value).is_gt(),
        };
        if should_replace {
            *total = Some(value);
        }
        Ok(())
    }

    fn apply_forwarding_headers(
        &self,
        request: reqwest::RequestBuilder,
        username: &str,
    ) -> reqwest::RequestBuilder {
        let request = request.header(SHARD_OWNER_FORWARD_HEADER, SHARD_OWNER_FORWARD_VALUE);
        if username.is_empty() {
            request
        } else {
            request.header("x-fusiondb-user", username)
        }
    }

    async fn best_effort_deallocate_forwarded_statement(
        &self,
        username: &str,
        decision: &SqlShardRoutingDecision,
        prepared: &ForwardPreparedStatementInfo,
    ) {
        let url = format!(
            "http://{}/prepare/{}",
            decision.route.owner_addr, prepared.statement_id
        );
        let _ = self
            .apply_forwarding_headers(self.http_client.delete(url), username)
            .send()
            .await;
    }

    async fn best_effort_deallocate_prepared_on_owner(
        &self,
        username: &str,
        owner: &SqlShardOwner,
        prepared: &ForwardPreparedStatementInfo,
    ) {
        let url = format!("http://{}/prepare/{}", owner.addr, prepared.statement_id);
        let _ = self
            .apply_forwarding_headers(self.http_client.delete(url), username)
            .send()
            .await;
    }

    fn shard_owner_forwarding_transport_error(
        decision: &SqlShardRoutingDecision,
        phase: &str,
        error: reqwest::Error,
    ) -> String {
        format!(
            "{}; shard owner forwarding to node {} at {} failed during {}: {}",
            Self::shard_route_conflict_message(decision),
            decision.route.owner_node_id,
            decision.route.owner_addr,
            phase,
            error
        )
    }

    fn forwarded_owner_error(message: String) -> pgwire::error::ErrorInfo {
        if message.contains("Shard route conflict") {
            Self::shard_route_error(message)
        } else {
            Self::execution_error(message)
        }
    }

    async fn send_forwarded_extended_query_results<C>(
        &self,
        client: &mut C,
        query: &str,
        params: &[Value],
        result_format_codes: &[i16],
        jdbc_client: bool,
        results: Vec<ForwardQueryResultJson>,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        for result in results {
            match result {
                ForwardQueryResultJson::Select { columns, rows } => {
                    let rows = rows
                        .into_iter()
                        .map(|row| row.iter().map(Value::from_json).collect::<Vec<_>>())
                        .collect::<Vec<_>>();
                    let effective_result_format_codes =
                        if result_format_codes.is_empty() && !jdbc_client {
                            vec![1]
                        } else {
                            result_format_codes.to_vec()
                        };
                    let described_fields = self
                        .describe_query_fields(query, params, &effective_result_format_codes)
                        .await?;
                    let fields = if described_fields.is_empty() {
                        Self::fields_with_format(&columns, &rows, &effective_result_format_codes)
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
                                        field.format(),
                                    )
                                })
                                .collect::<Vec<_>>(),
                        )
                    };

                    for row in rows {
                        client
                            .send(PgWireBackendMessage::DataRow(Self::encode_row_for_fields(
                                fields.clone(),
                                row,
                            )?))
                            .await
                            .map_err(|_| Self::sink_error())?;
                    }
                    client
                        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                            "SELECT".to_string(),
                        )))
                        .await
                        .map_err(|_| Self::sink_error())?;
                }
                ForwardQueryResultJson::Success { message } => {
                    client
                        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                            Self::pg_command_tag(&message),
                        )))
                        .await
                        .map_err(|_| Self::sink_error())?;
                }
            }
        }
        Ok(())
    }

    fn responses_from_forwarded_query_results(
        results: Vec<ForwardQueryResultJson>,
    ) -> PgWireResult<Vec<Response>> {
        let mut responses = Vec::with_capacity(results.len());
        for result in results {
            match result {
                ForwardQueryResultJson::Select { columns, rows } => {
                    let rows = rows
                        .into_iter()
                        .map(|row| row.iter().map(Value::from_json).collect::<Vec<_>>())
                        .collect::<Vec<_>>();
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
                ForwardQueryResultJson::Success { message } => {
                    responses.push(Response::Execution(Tag::new(&Self::pg_command_tag(
                        &message,
                    ))));
                }
            }
        }
        Ok(responses)
    }

    fn is_pg_metadata_query(query: &str) -> bool {
        let upper = query.trim().trim_end_matches(';').to_ascii_uppercase();
        upper.contains("INFORMATION_SCHEMA.")
            || upper.contains("PG_CATALOG.")
            || upper.contains("FROM PG_DATABASE")
            || upper.contains("FROM PG_TABLES")
            || upper.contains("FROM PG_STAT_")
            || upper.contains("FROM PG_STATIO_")
            || upper.contains("CURRENT_SCHEMA()")
            || upper.contains("CURRENT_CATALOG")
            || upper.contains("VERSION()")
            || upper.contains("CURRENT_DATABASE()")
            || upper.contains("CURRENT_SETTING(")
            || upper == "SHOW ALL"
            || upper == "SHOW SERVER_VERSION"
            || upper == "SHOW SERVER_ENCODING"
            || upper == "SHOW TRANSACTION ISOLATION LEVEL"
    }

    async fn try_execute_pg_metadata_query(
        &self,
        query: &str,
        params: &[Value],
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        if !Self::is_pg_metadata_query(query) {
            return Ok(None);
        }

        if let Some(result) = Self::pg_metadata_show(query) {
            return Ok(Some(result));
        }

        if let Some(result) = Self::pg_jdbc_pg_type_metadata(query, params)? {
            return Ok(Some(result));
        }

        if let Some(result) = self.pg_jdbc_get_tables(query, params).await? {
            return Ok(Some(result));
        }
        if let Some(result) = self.pg_jdbc_get_columns(query, params).await? {
            return Ok(Some(result));
        }
        if let Some(result) = self.pg_jdbc_get_index_info(query, params).await? {
            return Ok(Some(result));
        }
        if let Some(result) = self.pg_jdbc_foreign_key_metadata(query, params).await? {
            return Ok(Some(result));
        }

        let statements = parse_sql(query)
            .map_err(|e| FusionError::Execution(format!("Parse Error: {:?}", e)))?;
        let Some(Statement::Query(parsed_query)) = statements.first() else {
            return Ok(None);
        };
        let SetExpr::Select(select) = parsed_query.body.as_ref() else {
            return Ok(None);
        };

        if select.from.is_empty() {
            return Self::pg_metadata_scalar_select(select);
        }

        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return Ok(None);
        }
        let sqlparser::ast::TableFactor::Table { name, .. } = &select.from[0].relation else {
            return Ok(None);
        };
        let relation = name.to_string().to_ascii_lowercase();
        match relation.as_str() {
            "information_schema.tables" => self.pg_information_schema_tables(select).await,
            "information_schema.columns" => self.pg_information_schema_columns(select).await,
            "pg_catalog.pg_type" => Self::pg_catalog_pg_type(select),
            "pg_catalog.pg_namespace" => Self::pg_catalog_pg_namespace(select),
            "pg_catalog.pg_class" => self.pg_catalog_pg_class(select).await,
            "pg_catalog.pg_attribute" => self.pg_catalog_pg_attribute(select).await,
            "pg_catalog.pg_database" | "pg_database" => {
                Self::pg_catalog_pg_database(select, params)
            }
            "pg_catalog.pg_settings" => Self::pg_catalog_pg_settings(select),
            "pg_catalog.pg_tables" | "pg_tables" => self.pg_catalog_pg_tables(select).await,
            "pg_catalog.pg_stat_archiver" | "pg_stat_archiver" => Self::pg_stat_archiver(select),
            "pg_catalog.pg_stat_bgwriter" | "pg_stat_bgwriter" => Self::pg_stat_bgwriter(select),
            "pg_catalog.pg_stat_database" | "pg_stat_database" => Self::pg_stat_database(select),
            "pg_catalog.pg_stat_database_conflicts" | "pg_stat_database_conflicts" => {
                Self::pg_stat_database_conflicts(select)
            }
            "pg_catalog.pg_stat_user_tables" | "pg_stat_user_tables" => {
                self.pg_stat_user_tables(select).await
            }
            "pg_catalog.pg_statio_user_tables" | "pg_statio_user_tables" => {
                self.pg_statio_user_tables(select).await
            }
            "pg_catalog.pg_stat_user_indexes" | "pg_stat_user_indexes" => {
                self.pg_stat_user_indexes(select).await
            }
            "pg_catalog.pg_statio_user_indexes" | "pg_statio_user_indexes" => {
                self.pg_statio_user_indexes(select).await
            }
            _ => Ok(None),
        }
    }

    async fn pg_jdbc_get_tables(
        &self,
        query: &str,
        params: &[Value],
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let upper = query.to_ascii_uppercase();
        if !upper.contains("FROM PG_CATALOG.PG_NAMESPACE N, PG_CATALOG.PG_CLASS C")
            || !upper.contains("TABLE_NAME")
            || !upper.contains("TABLE_TYPE")
        {
            return Ok(None);
        }

        let schema_pattern = params
            .first()
            .and_then(|value| Self::metadata_param_as_string(value))
            .unwrap_or_else(|| "public".to_string());
        let table_pattern = params
            .get(1)
            .and_then(|value| Self::metadata_param_as_string(value))
            .unwrap_or_else(|| "%".to_string());

        let columns = vec![
            "TABLE_CAT".to_string(),
            "TABLE_SCHEM".to_string(),
            "TABLE_NAME".to_string(),
            "TABLE_TYPE".to_string(),
            "REMARKS".to_string(),
            "TYPE_CAT".to_string(),
            "TYPE_SCHEM".to_string(),
            "TYPE_NAME".to_string(),
            "SELF_REFERENCING_COL_NAME".to_string(),
            "REF_GENERATION".to_string(),
        ];
        let rows = self
            .load_pg_table_schemas()
            .await?
            .into_iter()
            .filter(|schema| {
                Self::metadata_like_matches("public", &schema_pattern)
                    && Self::metadata_like_matches(&schema.name, &table_pattern)
            })
            .map(|schema| {
                vec![
                    Value::String("fusiondb".to_string()),
                    Value::String("public".to_string()),
                    Value::String(schema.name),
                    Value::String("TABLE".to_string()),
                    Value::Null,
                    Value::String(String::new()),
                    Value::String(String::new()),
                    Value::String(String::new()),
                    Value::String(String::new()),
                    Value::String(String::new()),
                ]
            })
            .collect();
        Ok(Some(QueryResult::Select { columns, rows }))
    }

    fn pg_jdbc_pg_type_metadata(
        query: &str,
        params: &[Value],
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let upper = query.to_ascii_uppercase();
        if !upper.contains("PG_CATALOG.PG_TYPE") {
            return Ok(None);
        }

        if upper.contains("T.TYPELEM = E.OID")
            && upper.contains("T.OID =")
            && upper.contains("E.TYPDELIM")
        {
            let array_oid = Self::first_metadata_i64_param_or_literal(query, params);
            let rows = array_oid
                .and_then(Self::pg_array_element_type_for_oid)
                .map(|element| vec![vec![Value::String(element.delimiter.to_string())]])
                .into_iter()
                .flatten()
                .collect();
            return Ok(Some(QueryResult::Select {
                columns: vec!["typdelim".to_string()],
                rows,
            }));
        }

        if upper.contains("T.TYPELEM = E.OID")
            && upper.contains("T.OID =")
            && upper.contains("E.OID")
            && upper.contains("E.TYPNAME")
        {
            let array_oid = Self::first_metadata_i64_param_or_literal(query, params);
            let rows = array_oid
                .and_then(Self::pg_array_element_type_for_oid)
                .map(|element| {
                    vec![vec![
                        Value::Integer(element.oid),
                        Value::Boolean(true),
                        Value::String("pg_catalog".to_string()),
                        Value::String(element.name.to_string()),
                    ]]
                })
                .unwrap_or_default();
            return Ok(Some(QueryResult::Select {
                columns: vec![
                    "oid".to_string(),
                    "?column?".to_string(),
                    "nspname".to_string(),
                    "typname".to_string(),
                ],
                rows,
            }));
        }

        if upper.contains("N.NSPNAME = ANY(CURRENT_SCHEMAS(TRUE))")
            && upper.contains("T.TYPNAME")
            && upper.contains("T.OID")
        {
            let oid = Self::first_metadata_i64_param_or_literal(query, params);
            let rows = oid
                .and_then(Self::pg_type_info_for_oid)
                .map(|ty| {
                    vec![vec![
                        Value::Boolean(true),
                        Value::String("pg_catalog".to_string()),
                        Value::String(ty.name.to_string()),
                    ]]
                })
                .unwrap_or_default();
            return Ok(Some(QueryResult::Select {
                columns: vec![
                    "?column?".to_string(),
                    "nspname".to_string(),
                    "typname".to_string(),
                ],
                rows,
            }));
        }

        Ok(None)
    }

    fn metadata_param_as_string(value: &Value) -> Option<String> {
        match value {
            Value::Null => None,
            Value::String(value) | Value::Decimal(value) => Some(value.clone()),
            Value::Integer(value) => Some(value.to_string()),
            Value::Float(value) => Some(value.to_string()),
            Value::Boolean(value) => Some(if *value { "true" } else { "false" }.to_string()),
            _ => None,
        }
    }

    async fn pg_jdbc_get_columns(
        &self,
        query: &str,
        params: &[Value],
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let upper = query.to_ascii_uppercase();
        if !upper.contains("JOIN PG_CATALOG.PG_ATTRIBUTE A")
            || !upper.contains("JOIN PG_CATALOG.PG_TYPE T")
            || !upper.contains("C.RELNAME LIKE")
        {
            return Ok(None);
        }

        let schema_pattern = params
            .first()
            .and_then(|value| Self::metadata_param_as_string(value))
            .unwrap_or_else(|| "public".to_string());
        let table_pattern = params
            .get(1)
            .and_then(|value| Self::metadata_param_as_string(value))
            .or_else(|| Self::metadata_literal_like_pattern(query, "c.relname"))
            .unwrap_or_else(|| "%".to_string());
        let column_pattern = params
            .get(2)
            .and_then(|value| Self::metadata_param_as_string(value))
            .or_else(|| Self::metadata_literal_like_pattern(query, "a.attname"))
            .unwrap_or_else(|| "%".to_string());

        let columns = vec![
            "current_database".to_string(),
            "nspname".to_string(),
            "relname".to_string(),
            "attname".to_string(),
            "atttypid".to_string(),
            "attnotnull".to_string(),
            "atttypmod".to_string(),
            "attlen".to_string(),
            "typtypmod".to_string(),
            "attnum".to_string(),
            "attidentity".to_string(),
            "attgenerated".to_string(),
            "adsrc".to_string(),
            "description".to_string(),
            "typbasetype".to_string(),
            "typtype".to_string(),
        ];
        let mut rows = Vec::new();
        for schema in self.load_pg_table_schemas().await? {
            if !Self::metadata_like_matches("public", &schema_pattern)
                || !Self::metadata_like_matches(&schema.name, &table_pattern)
            {
                continue;
            }
            for (idx, column) in schema.columns.iter().enumerate() {
                if !Self::metadata_like_matches(&column.name, &column_pattern) {
                    continue;
                }
                rows.push(vec![
                    Value::String("fusiondb".to_string()),
                    Value::String("public".to_string()),
                    Value::String(schema.name.clone()),
                    Value::String(column.name.clone()),
                    Value::Integer(Self::pg_type_oid_for_column_type(&column.data_type)),
                    Value::Boolean(!column.is_nullable),
                    Value::Integer(-1),
                    Value::Integer(Self::pg_type_len_for_column_type(&column.data_type)),
                    Value::Integer(-1),
                    Value::Integer((idx + 1) as i64),
                    Value::String(String::new()),
                    Value::String(String::new()),
                    column
                        .default_value
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                    Value::Null,
                    Value::Integer(0),
                    Value::String("b".to_string()),
                ]);
            }
        }

        Ok(Some(QueryResult::Select { columns, rows }))
    }

    async fn pg_jdbc_get_index_info(
        &self,
        query: &str,
        params: &[Value],
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let upper = query.to_ascii_uppercase();
        if !upper.contains("PG_CATALOG.PG_INDEX")
            || !upper.contains("PG_CATALOG.PG_AM")
            || !upper.contains("PG_GET_INDEXDEF")
            || !upper.contains("INDEX_NAME")
            || !upper.contains("ORDINAL_POSITION")
        {
            return Ok(None);
        }

        let schema_pattern = params
            .first()
            .and_then(|value| Self::metadata_param_as_string(value))
            .or_else(|| Self::metadata_literal_filter_pattern(query, "n.nspname"))
            .unwrap_or_else(|| "public".to_string());
        let table_pattern = params
            .get(1)
            .and_then(|value| Self::metadata_param_as_string(value))
            .or_else(|| Self::metadata_literal_filter_pattern(query, "ct.relname"))
            .or_else(|| Self::metadata_literal_filter_pattern(query, "c.relname"))
            .unwrap_or_else(|| "%".to_string());

        let columns = vec![
            "TABLE_CAT".to_string(),
            "TABLE_SCHEM".to_string(),
            "TABLE_NAME".to_string(),
            "NON_UNIQUE".to_string(),
            "INDEX_QUALIFIER".to_string(),
            "INDEX_NAME".to_string(),
            "TYPE".to_string(),
            "ORDINAL_POSITION".to_string(),
            "COLUMN_NAME".to_string(),
            "ASC_OR_DESC".to_string(),
            "CARDINALITY".to_string(),
            "PAGES".to_string(),
            "FILTER_CONDITION".to_string(),
        ];

        let mut rows = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let schemas = self.load_pg_table_schemas().await?;
        let index_metas = self.load_pg_index_metadata().await?;

        for schema in schemas {
            if !Self::metadata_like_matches("public", &schema_pattern)
                || !Self::metadata_like_matches(&schema.name, &table_pattern)
            {
                continue;
            }

            if let Some(pk_idx) = schema.get_primary_key_index() {
                let index_name = format!("{}_pkey", schema.name);
                let column_name = schema.columns[pk_idx].name.clone();
                Self::push_pg_index_info_row(
                    &mut rows,
                    &mut seen,
                    &schema.name,
                    &index_name,
                    false,
                    1,
                    &column_name,
                );
            }

            for (index_name, meta_str) in &index_metas {
                let Some(meta) = Executor::parse_index_meta(index_name, meta_str) else {
                    continue;
                };
                if !meta.table.eq_ignore_ascii_case(&schema.name) {
                    continue;
                }
                let non_unique = !meta_str.starts_with("u3:") && !index_name.ends_with("_pkey");
                for (idx, column_name) in meta.columns.iter().enumerate() {
                    if schema
                        .columns
                        .iter()
                        .all(|column| !column.name.eq_ignore_ascii_case(column_name))
                    {
                        continue;
                    }
                    Self::push_pg_index_info_row(
                        &mut rows,
                        &mut seen,
                        &schema.name,
                        index_name,
                        non_unique,
                        (idx + 1) as i64,
                        column_name,
                    );
                }
            }
        }

        rows.sort_by(|left, right| {
            let left_non_unique = matches!(left.get(3), Some(Value::Boolean(true)));
            let right_non_unique = matches!(right.get(3), Some(Value::Boolean(true)));
            left_non_unique
                .cmp(&right_non_unique)
                .then_with(|| {
                    Self::metadata_row_string(left, 5).cmp(&Self::metadata_row_string(right, 5))
                })
                .then_with(|| {
                    Self::metadata_row_integer(left, 7).cmp(&Self::metadata_row_integer(right, 7))
                })
        });

        Ok(Some(QueryResult::Select { columns, rows }))
    }

    async fn load_pg_index_metadata(
        &self,
    ) -> std::result::Result<Vec<(String, String)>, FusionError> {
        let txn = self.storage.begin_transaction().await?;
        let entries = txn.scan_prefix(b"index_meta:", None).await?;
        let mut metas = Vec::new();
        for (key, value) in entries {
            let Ok(key_str) = std::str::from_utf8(&key) else {
                continue;
            };
            let Some(index_name) = key_str.strip_prefix("index_meta:") else {
                continue;
            };
            let Ok(meta_str) = String::from_utf8(value) else {
                continue;
            };
            metas.push((index_name.to_string(), meta_str));
        }
        metas.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(metas)
    }

    fn push_pg_index_info_row(
        rows: &mut Vec<Vec<Value>>,
        seen: &mut std::collections::HashSet<(String, String, i64, String)>,
        table_name: &str,
        index_name: &str,
        non_unique: bool,
        ordinal_position: i64,
        column_name: &str,
    ) {
        let key = (
            table_name.to_ascii_lowercase(),
            index_name.to_ascii_lowercase(),
            ordinal_position,
            column_name.to_ascii_lowercase(),
        );
        if !seen.insert(key) {
            return;
        }
        rows.push(vec![
            Value::Null,
            Value::String("public".to_string()),
            Value::String(table_name.to_string()),
            Value::Boolean(non_unique),
            Value::Null,
            Value::String(index_name.to_string()),
            Value::Integer(3),
            Value::Integer(ordinal_position),
            Value::String(column_name.to_string()),
            Value::String("A".to_string()),
            Value::Integer(0),
            Value::Integer(0),
            Value::Null,
        ]);
    }

    fn metadata_row_string(row: &[Value], idx: usize) -> String {
        match row.get(idx) {
            Some(Value::String(value)) => value.clone(),
            Some(value) => value.to_string(),
            None => String::new(),
        }
    }

    fn metadata_row_integer(row: &[Value], idx: usize) -> i64 {
        match row.get(idx) {
            Some(Value::Integer(value)) => *value,
            _ => 0,
        }
    }

    async fn pg_jdbc_foreign_key_metadata(
        &self,
        query: &str,
        params: &[Value],
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let upper = query.to_ascii_uppercase();
        if !upper.contains("PG_CATALOG.PG_CONSTRAINT")
            || !upper.contains("CON.CONTYPE = 'F'")
            || !upper.contains("PKTABLE_NAME")
            || !upper.contains("FKTABLE_NAME")
            || !upper.contains("FKCOLUMN_NAME")
        {
            return Ok(None);
        }

        let child_schema_pattern = params
            .first()
            .and_then(|value| Self::metadata_param_as_string(value))
            .or_else(|| Self::metadata_literal_filter_pattern(query, "fkn.nspname"))
            .unwrap_or_else(|| "%".to_string());
        let child_table_pattern = params
            .get(1)
            .and_then(|value| Self::metadata_param_as_string(value))
            .or_else(|| Self::metadata_literal_filter_pattern(query, "fkc.relname"))
            .unwrap_or_else(|| "%".to_string());
        let parent_schema_pattern = params
            .get(2)
            .and_then(|value| Self::metadata_param_as_string(value))
            .or_else(|| Self::metadata_literal_filter_pattern(query, "pkn.nspname"))
            .unwrap_or_else(|| "%".to_string());
        let parent_table_pattern = params
            .get(3)
            .and_then(|value| Self::metadata_param_as_string(value))
            .or_else(|| Self::metadata_literal_filter_pattern(query, "pkc.relname"))
            .unwrap_or_else(|| "%".to_string());

        let columns = vec![
            "PKTABLE_CAT".to_string(),
            "PKTABLE_SCHEM".to_string(),
            "PKTABLE_NAME".to_string(),
            "PKCOLUMN_NAME".to_string(),
            "FKTABLE_CAT".to_string(),
            "FKTABLE_SCHEM".to_string(),
            "FKTABLE_NAME".to_string(),
            "FKCOLUMN_NAME".to_string(),
            "KEY_SEQ".to_string(),
            "UPDATE_RULE".to_string(),
            "DELETE_RULE".to_string(),
            "FK_NAME".to_string(),
            "PK_NAME".to_string(),
            "DEFERRABILITY".to_string(),
        ];

        let mut rows = Vec::new();
        for fk in self.load_pg_foreign_key_metadata().await? {
            if !Self::metadata_like_matches("public", &child_schema_pattern)
                || !Self::metadata_like_matches(&fk.child_table, &child_table_pattern)
                || !Self::metadata_like_matches("public", &parent_schema_pattern)
                || !Self::metadata_like_matches(&fk.parent_table, &parent_table_pattern)
            {
                continue;
            }

            let child_columns = fk.child_columns();
            let parent_columns = fk.parent_columns();
            for (idx, (child_column, parent_column)) in
                child_columns.iter().zip(parent_columns.iter()).enumerate()
            {
                rows.push(vec![
                    Value::Null,
                    Value::String("public".to_string()),
                    Value::String(fk.parent_table.clone()),
                    Value::String(parent_column.clone()),
                    Value::Null,
                    Value::String("public".to_string()),
                    Value::String(fk.child_table.clone()),
                    Value::String(child_column.clone()),
                    Value::Integer((idx + 1) as i64),
                    Value::Integer(3),
                    Value::Integer(0),
                    Value::String(fk.name.clone()),
                    Value::String(format!("{}_pkey", fk.parent_table)),
                    Value::Integer(7),
                ]);
            }
        }

        rows.sort_by(|left, right| {
            Self::metadata_row_string(left, 1)
                .cmp(&Self::metadata_row_string(right, 1))
                .then_with(|| {
                    Self::metadata_row_string(left, 2).cmp(&Self::metadata_row_string(right, 2))
                })
                .then_with(|| {
                    Self::metadata_row_string(left, 11).cmp(&Self::metadata_row_string(right, 11))
                })
                .then_with(|| {
                    Self::metadata_row_integer(left, 8).cmp(&Self::metadata_row_integer(right, 8))
                })
        });

        Ok(Some(QueryResult::Select { columns, rows }))
    }

    async fn load_pg_foreign_key_metadata(
        &self,
    ) -> std::result::Result<Vec<ForeignKeyMeta>, FusionError> {
        let txn = self.storage.begin_transaction().await?;
        let entries = txn.scan_prefix(b"fk_meta:child:", None).await?;
        let mut foreign_keys = Vec::new();
        for (_, value) in entries {
            let Ok(fk) = bincode::deserialize::<ForeignKeyMeta>(&value) else {
                continue;
            };
            foreign_keys.push(fk);
        }
        foreign_keys.sort_by(|left, right| {
            left.child_table
                .cmp(&right.child_table)
                .then_with(|| left.name.cmp(&right.name))
        });
        Ok(foreign_keys)
    }

    fn metadata_literal_like_pattern(query: &str, column: &str) -> Option<String> {
        let lower_query = query.to_ascii_lowercase();
        let lower_column = column.to_ascii_lowercase();
        let mut offset = 0;
        while let Some(pos) = lower_query[offset..].find(&lower_column) {
            let start = offset + pos + lower_column.len();
            let rest = query.get(start..)?.trim_start();
            if !rest
                .get(..4)
                .map(|head| head.eq_ignore_ascii_case("LIKE"))
                .unwrap_or(false)
            {
                offset = start;
                continue;
            }
            let after_like = rest.get(4..)?.trim_start();
            if !after_like.starts_with('\'') {
                return None;
            }
            let mut value = String::new();
            let mut chars = after_like[1..].chars().peekable();
            while let Some(ch) = chars.next() {
                if ch == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        value.push('\'');
                        continue;
                    }
                    return Some(value);
                }
                value.push(ch);
            }
            return None;
        }
        None
    }

    fn metadata_literal_filter_pattern(query: &str, column: &str) -> Option<String> {
        let lower_query = query.to_ascii_lowercase();
        let lower_column = column.to_ascii_lowercase();
        let mut offset = 0;
        while let Some(pos) = lower_query[offset..].find(&lower_column) {
            let start = offset + pos + lower_column.len();
            let rest = query.get(start..)?.trim_start();
            let after_operator = if rest
                .get(..4)
                .map(|head| head.eq_ignore_ascii_case("LIKE"))
                .unwrap_or(false)
            {
                rest.get(4..)?.trim_start()
            } else if rest.starts_with('=') {
                rest.get(1..)?.trim_start()
            } else {
                offset = start;
                continue;
            };
            if !after_operator.starts_with('\'') {
                return None;
            }
            let mut value = String::new();
            let mut chars = after_operator[1..].chars().peekable();
            while let Some(ch) = chars.next() {
                if ch == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        value.push('\'');
                        continue;
                    }
                    return Some(value);
                }
                value.push(ch);
            }
            return None;
        }
        None
    }

    fn metadata_like_matches(value: &str, pattern: &str) -> bool {
        fn matches_at(value: &[u8], pattern: &[u8]) -> bool {
            if pattern.is_empty() {
                return value.is_empty();
            }
            match pattern[0] {
                b'%' => {
                    matches_at(value, &pattern[1..])
                        || (!value.is_empty() && matches_at(&value[1..], pattern))
                }
                b'_' => !value.is_empty() && matches_at(&value[1..], &pattern[1..]),
                ch => {
                    !value.is_empty()
                        && value[0].eq_ignore_ascii_case(&ch)
                        && matches_at(&value[1..], &pattern[1..])
                }
            }
        }

        matches_at(value.as_bytes(), pattern.as_bytes())
    }

    fn pg_metadata_show(query: &str) -> Option<QueryResult> {
        let upper = query.trim().trim_end_matches(';').to_ascii_uppercase();
        let (column, value) = match upper.as_str() {
            "SHOW SERVER_VERSION" => ("server_version", Self::pg_server_version()),
            "SHOW SERVER_ENCODING" => ("server_encoding", "UTF8".to_string()),
            "SHOW TRANSACTION ISOLATION LEVEL" => {
                ("transaction_isolation", "read committed".to_string())
            }
            "SHOW ALL" => return Some(Executor::show_all_settings_result()),
            _ => return None,
        };
        Some(QueryResult::Select {
            columns: vec![column.to_string()],
            rows: vec![vec![Value::String(value)]],
        })
    }

    fn parse_set_application_name(query: &str) -> Option<String> {
        let trimmed = query.trim().trim_end_matches(';').trim();
        let prefix = "SET application_name";
        if !trimmed
            .get(..prefix.len())
            .map(|head| head.eq_ignore_ascii_case(prefix))
            .unwrap_or(false)
        {
            return None;
        }
        let value = trimmed[prefix.len()..].trim();
        let value = value.strip_prefix('=').unwrap_or(value).trim();
        if value.is_empty() {
            return None;
        }
        Some(Self::unquote_sql_string(value))
    }

    fn unquote_sql_string(value: &str) -> String {
        let trimmed = value.trim();
        if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            trimmed[1..trimmed.len() - 1].replace("''", "'")
        } else {
            trimmed.to_string()
        }
    }

    fn pg_server_version() -> String {
        "15.0".to_string()
    }

    fn pg_version_string() -> String {
        format!(
            "PostgreSQL {}-compatible FusionDB 0.1.0",
            Self::pg_server_version()
        )
    }

    async fn load_pg_table_schemas(&self) -> std::result::Result<Vec<TableSchema>, FusionError> {
        let txn = self.storage.begin_transaction().await?;
        let pairs = txn.scan_prefix(b"schema:", None).await?;
        let mut schemas = Vec::new();
        for (_, value) in pairs {
            if let Ok(schema) = bincode::deserialize::<TableSchema>(&value) {
                schemas.push(schema);
            }
        }
        schemas.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(schemas)
    }

    fn pg_metadata_scalar_select(
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let mut columns = Vec::new();
        let mut row = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if let Some((name, value)) = Self::pg_metadata_scalar_expr(expr, None) {
                        columns.push(name);
                        row.push(value);
                    } else {
                        return Ok(None);
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Some((_, value)) =
                        Self::pg_metadata_scalar_expr(expr, Some(&alias.value))
                    {
                        columns.push(alias.value.clone());
                        row.push(value);
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            }
        }
        Ok(Some(QueryResult::Select {
            columns,
            rows: vec![row],
        }))
    }

    fn pg_metadata_scalar_expr(expr: &Expr, alias: Option<&str>) -> Option<(String, Value)> {
        if let Expr::Identifier(ident) = expr {
            if ident.value.eq_ignore_ascii_case("current_catalog") {
                let column = "current_catalog".to_string();
                return Some((
                    alias.unwrap_or(&column).to_string(),
                    Value::String("fusiondb".to_string()),
                ));
            }
        };

        let Expr::Function(func) = expr else {
            return None;
        };
        let name = func.name.to_string().to_ascii_lowercase();
        let (column, value) = match name.as_str() {
            "current_schema" => (
                "current_schema".to_string(),
                Value::String("public".to_string()),
            ),
            "current_catalog" => (
                "current_catalog".to_string(),
                Value::String("fusiondb".to_string()),
            ),
            "current_database" => (
                "current_database".to_string(),
                Value::String("fusiondb".to_string()),
            ),
            "version" => (
                "version".to_string(),
                Value::String(Self::pg_version_string()),
            ),
            "current_setting" => {
                let setting = Self::pg_function_first_string_arg(expr)?;
                let value = match setting.to_ascii_lowercase().as_str() {
                    "server_version" => Self::pg_server_version(),
                    "server_encoding" | "client_encoding" => "UTF8".to_string(),
                    "search_path" => "public".to_string(),
                    "application_name" => String::new(),
                    _ => return None,
                };
                ("current_setting".to_string(), Value::String(value))
            }
            _ => return None,
        };
        Some((alias.unwrap_or(&column).to_string(), value))
    }

    fn pg_function_first_string_arg(expr: &Expr) -> Option<String> {
        let Expr::Function(func) = expr else {
            return None;
        };
        match &func.args {
            FunctionArguments::List(list) => {
                let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))) = list.args.first()
                else {
                    return None;
                };
                Self::pg_metadata_string_value(arg)
            }
            _ => None,
        }
    }

    async fn pg_information_schema_tables(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let schemas = self.load_pg_table_schemas().await?;
        let columns = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "table_type".to_string(),
        ];
        let rows = schemas
            .into_iter()
            .filter(|schema| {
                Self::pg_schema_row_matches(select.selection.as_ref(), &schema.name, None)
            })
            .map(|schema| {
                vec![
                    Value::String("fusiondb".to_string()),
                    Value::String("public".to_string()),
                    Value::String(schema.name),
                    Value::String("BASE TABLE".to_string()),
                ]
            })
            .collect();
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    async fn pg_information_schema_columns(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let schemas = self.load_pg_table_schemas().await?;
        let columns = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "column_name".to_string(),
            "ordinal_position".to_string(),
            "is_nullable".to_string(),
            "column_default".to_string(),
            "data_type".to_string(),
        ];
        let mut rows = Vec::new();
        for schema in schemas {
            if !Self::pg_schema_row_matches(select.selection.as_ref(), &schema.name, None) {
                continue;
            }
            for (idx, column) in schema.columns.iter().enumerate() {
                if !Self::pg_schema_row_matches(
                    select.selection.as_ref(),
                    &schema.name,
                    Some(&column.name),
                ) {
                    continue;
                }
                rows.push(vec![
                    Value::String("fusiondb".to_string()),
                    Value::String("public".to_string()),
                    Value::String(schema.name.clone()),
                    Value::String(column.name.clone()),
                    Value::Integer((idx + 1) as i64),
                    Value::String(if column.is_nullable { "YES" } else { "NO" }.to_string()),
                    column
                        .default_value
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                    Value::String(Self::pg_information_schema_data_type(&column.data_type)),
                ]);
            }
        }
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    fn pg_information_schema_data_type(data_type: &str) -> String {
        let upper = data_type.trim().to_ascii_uppercase();
        if Self::pg_type_for_column_type(&upper) == Type::INT2
            || Self::pg_type_for_column_type(&upper) == Type::INT4
            || Self::pg_type_for_column_type(&upper) == Type::INT8
        {
            "integer".to_string()
        } else if Self::pg_type_for_column_type(&upper) == Type::FLOAT4
            || Self::pg_type_for_column_type(&upper) == Type::FLOAT8
        {
            "double precision".to_string()
        } else if Self::pg_type_for_column_type(&upper) == Type::NUMERIC {
            "numeric".to_string()
        } else if Self::pg_type_for_column_type(&upper) == Type::DATE {
            "date".to_string()
        } else if matches!(
            Self::pg_type_for_column_type(&upper),
            Type::TIMESTAMP | Type::TIMESTAMPTZ
        ) {
            "timestamp without time zone".to_string()
        } else if Self::pg_type_for_column_type(&upper) == Type::INTERVAL {
            "interval".to_string()
        } else if Self::pg_type_for_column_type(&upper) == Type::BOOL {
            "boolean".to_string()
        } else {
            "text".to_string()
        }
    }

    fn pg_catalog_pg_type(
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "oid".to_string(),
            "typname".to_string(),
            "typnamespace".to_string(),
            "typelem".to_string(),
            "typdelim".to_string(),
            "typarray".to_string(),
        ];
        let rows = Self::pg_builtin_type_infos()
            .iter()
            .filter(|info| {
                Self::pg_metadata_name_matches(select.selection.as_ref(), "typname", info.name)
            })
            .map(|info| {
                vec![
                    Value::Integer(info.oid),
                    Value::String(info.name.to_string()),
                    Value::Integer(11),
                    info.element_oid.map(Value::Integer).unwrap_or(Value::Null),
                    Value::String(info.delimiter.to_string()),
                    Self::pg_array_oid_for_element_oid(info.oid)
                        .map(Value::Integer)
                        .unwrap_or(Value::Integer(0)),
                ]
            })
            .collect();
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    fn pg_catalog_pg_namespace(
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "oid".to_string(),
            "nspname".to_string(),
            "nspowner".to_string(),
            "nspacl".to_string(),
        ];
        let rows = [
            (11, "pg_catalog"),
            (2200, "public"),
            (13207, "information_schema"),
        ]
        .into_iter()
        .filter(|(_, name)| {
            Self::pg_metadata_name_matches(select.selection.as_ref(), "nspname", name)
        })
        .map(|(oid, name)| {
            vec![
                Value::Integer(oid),
                Value::String(name.to_string()),
                Value::Integer(10),
                Value::Null,
            ]
        })
        .collect();
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    async fn pg_catalog_pg_class(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let schemas = self.load_pg_table_schemas().await?;
        let columns = vec![
            "oid".to_string(),
            "relname".to_string(),
            "relnamespace".to_string(),
            "relkind".to_string(),
            "relowner".to_string(),
            "relhasindex".to_string(),
            "relpersistence".to_string(),
        ];
        let rows = schemas
            .into_iter()
            .filter(|schema| {
                Self::pg_metadata_name_matches(select.selection.as_ref(), "relname", &schema.name)
            })
            .enumerate()
            .map(|(idx, schema)| {
                vec![
                    Value::Integer(10_000 + idx as i64),
                    Value::String(schema.name),
                    Value::Integer(2200),
                    Value::String("r".to_string()),
                    Value::Integer(10),
                    Value::Boolean(true),
                    Value::String("p".to_string()),
                ]
            })
            .collect();
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    async fn pg_catalog_pg_attribute(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let schemas = self.load_pg_table_schemas().await?;
        let columns = vec![
            "attrelid".to_string(),
            "attname".to_string(),
            "atttypid".to_string(),
            "attnum".to_string(),
            "attnotnull".to_string(),
            "attisdropped".to_string(),
            "attlen".to_string(),
            "atttypmod".to_string(),
        ];
        let mut rows = Vec::new();
        for (table_idx, schema) in schemas.into_iter().enumerate() {
            let relid = 10_000 + table_idx as i64;
            for (column_idx, column) in schema.columns.iter().enumerate() {
                if !Self::pg_attribute_row_matches(select.selection.as_ref(), relid, &column.name) {
                    continue;
                }
                rows.push(vec![
                    Value::Integer(relid),
                    Value::String(column.name.clone()),
                    Value::Integer(Self::pg_type_oid_for_column_type(&column.data_type)),
                    Value::Integer((column_idx + 1) as i64),
                    Value::Boolean(!column.is_nullable),
                    Value::Boolean(false),
                    Value::Integer(Self::pg_type_len_for_column_type(&column.data_type)),
                    Value::Integer(-1),
                ]);
            }
        }
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    async fn pg_catalog_pg_tables(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "schemaname".to_string(),
            "tablename".to_string(),
            "tableowner".to_string(),
            "tablespace".to_string(),
            "hasindexes".to_string(),
            "hasrules".to_string(),
            "hastriggers".to_string(),
            "rowsecurity".to_string(),
        ];
        let rows = self
            .load_pg_table_schemas()
            .await?
            .into_iter()
            .filter(|schema| {
                Self::pg_metadata_name_matches(select.selection.as_ref(), "tablename", &schema.name)
            })
            .map(|schema| {
                vec![
                    Value::String("public".to_string()),
                    Value::String(schema.name),
                    Value::String("postgres".to_string()),
                    Value::Null,
                    Value::Boolean(true),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::Boolean(false),
                ]
            })
            .collect();
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    fn pg_catalog_pg_database(
        select: &sqlparser::ast::Select,
        params: &[Value],
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "oid".to_string(),
            "datname".to_string(),
            "datdba".to_string(),
            "encoding".to_string(),
            "datistemplate".to_string(),
            "datallowconn".to_string(),
        ];
        let rows = [(5, "fusiondb")]
            .into_iter()
            .filter(|(_, name)| {
                Self::pg_metadata_name_matches_with_params(
                    select.selection.as_ref(),
                    "datname",
                    name,
                    params,
                )
            })
            .map(|(oid, name)| {
                vec![
                    Value::Integer(oid),
                    Value::String(name.to_string()),
                    Value::Integer(10),
                    Value::Integer(6),
                    Value::Boolean(false),
                    Value::Boolean(true),
                ]
            })
            .collect();
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    fn pg_catalog_pg_settings(
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "name".to_string(),
            "setting".to_string(),
            "unit".to_string(),
            "category".to_string(),
            "short_desc".to_string(),
            "extra_desc".to_string(),
            "context".to_string(),
            "vartype".to_string(),
            "source".to_string(),
            "min_val".to_string(),
            "max_val".to_string(),
        ];
        let rows = [
            (
                "max_index_keys",
                "32",
                "",
                "Preset Options",
                "Shows the maximum number of index columns.",
                "FusionDB reports PostgreSQL-compatible metadata for JDBC clients.",
                "internal",
                "integer",
                "default",
                "1",
                "32",
            ),
            (
                "server_version",
                "15.0",
                "",
                "Preset Options",
                "Shows the server version.",
                "",
                "internal",
                "string",
                "default",
                "",
                "",
            ),
            (
                "server_encoding",
                "UTF8",
                "",
                "Client Connection Defaults / Locale and Formatting",
                "Sets the server character set encoding.",
                "",
                "internal",
                "string",
                "default",
                "",
                "",
            ),
        ]
        .into_iter()
        .filter(|(name, ..)| {
            Self::pg_metadata_name_matches(select.selection.as_ref(), "name", name)
        })
        .map(
            |(
                name,
                setting,
                unit,
                category,
                short_desc,
                extra_desc,
                context,
                vartype,
                source,
                min_val,
                max_val,
            )| {
                vec![
                    Value::String(name.to_string()),
                    Value::String(setting.to_string()),
                    Value::String(unit.to_string()),
                    Value::String(category.to_string()),
                    Value::String(short_desc.to_string()),
                    Value::String(extra_desc.to_string()),
                    Value::String(context.to_string()),
                    Value::String(vartype.to_string()),
                    Value::String(source.to_string()),
                    Value::String(min_val.to_string()),
                    Value::String(max_val.to_string()),
                ]
            },
        )
        .collect();
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    fn pg_stat_archiver(
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "archived_count".to_string(),
            "last_archived_wal".to_string(),
            "last_archived_time".to_string(),
            "failed_count".to_string(),
            "last_failed_wal".to_string(),
            "last_failed_time".to_string(),
            "stats_reset".to_string(),
        ];
        let rows = vec![vec![
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Null,
        ]];
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    fn pg_stat_bgwriter(
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "checkpoints_timed".to_string(),
            "checkpoints_req".to_string(),
            "checkpoint_write_time".to_string(),
            "checkpoint_sync_time".to_string(),
            "buffers_checkpoint".to_string(),
            "buffers_clean".to_string(),
            "maxwritten_clean".to_string(),
            "buffers_backend".to_string(),
            "buffers_backend_fsync".to_string(),
            "buffers_alloc".to_string(),
            "stats_reset".to_string(),
        ];
        let rows = vec![vec![
            Value::Integer(0),
            Value::Integer(0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Null,
        ]];
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    fn pg_stat_database(
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "datid".to_string(),
            "datname".to_string(),
            "numbackends".to_string(),
            "xact_commit".to_string(),
            "xact_rollback".to_string(),
            "blks_read".to_string(),
            "blks_hit".to_string(),
            "tup_returned".to_string(),
            "tup_fetched".to_string(),
            "tup_inserted".to_string(),
            "tup_updated".to_string(),
            "tup_deleted".to_string(),
            "conflicts".to_string(),
            "temp_files".to_string(),
            "temp_bytes".to_string(),
            "deadlocks".to_string(),
            "checksum_failures".to_string(),
            "checksum_last_failure".to_string(),
            "blk_read_time".to_string(),
            "blk_write_time".to_string(),
            "session_time".to_string(),
            "active_time".to_string(),
            "idle_in_transaction_time".to_string(),
            "sessions".to_string(),
            "sessions_abandoned".to_string(),
            "sessions_fatal".to_string(),
            "sessions_killed".to_string(),
            "stats_reset".to_string(),
        ];
        let rows = vec![vec![
            Value::Integer(5),
            Value::String("fusiondb".to_string()),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Null,
        ]];
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    fn pg_stat_database_conflicts(
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "datid".to_string(),
            "datname".to_string(),
            "confl_tablespace".to_string(),
            "confl_lock".to_string(),
            "confl_snapshot".to_string(),
            "confl_bufferpin".to_string(),
            "confl_deadlock".to_string(),
        ];
        let rows = vec![vec![
            Value::Integer(5),
            Value::String("fusiondb".to_string()),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
        ]];
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    async fn pg_stat_user_tables(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "relid".to_string(),
            "schemaname".to_string(),
            "relname".to_string(),
            "seq_scan".to_string(),
            "seq_tup_read".to_string(),
            "idx_scan".to_string(),
            "idx_tup_fetch".to_string(),
            "n_tup_ins".to_string(),
            "n_tup_upd".to_string(),
            "n_tup_del".to_string(),
            "n_tup_hot_upd".to_string(),
            "n_tup_newpage_upd".to_string(),
            "n_live_tup".to_string(),
            "n_dead_tup".to_string(),
            "n_mod_since_analyze".to_string(),
            "n_ins_since_vacuum".to_string(),
            "last_vacuum".to_string(),
            "last_autovacuum".to_string(),
            "last_analyze".to_string(),
            "last_autoanalyze".to_string(),
            "vacuum_count".to_string(),
            "autovacuum_count".to_string(),
            "analyze_count".to_string(),
            "autoanalyze_count".to_string(),
        ];
        let rows = self
            .load_pg_table_schemas()
            .await?
            .into_iter()
            .enumerate()
            .filter(|(_, schema)| {
                Self::pg_metadata_name_matches(select.selection.as_ref(), "relname", &schema.name)
            })
            .map(|(idx, schema)| {
                vec![
                    Value::Integer(10_000 + idx as i64),
                    Value::String("public".to_string()),
                    Value::String(schema.name),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                ]
            })
            .collect();
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    async fn pg_statio_user_tables(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "relid".to_string(),
            "schemaname".to_string(),
            "relname".to_string(),
            "heap_blks_read".to_string(),
            "heap_blks_hit".to_string(),
            "idx_blks_read".to_string(),
            "idx_blks_hit".to_string(),
            "toast_blks_read".to_string(),
            "toast_blks_hit".to_string(),
            "tidx_blks_read".to_string(),
            "tidx_blks_hit".to_string(),
        ];
        let rows = self
            .load_pg_table_schemas()
            .await?
            .into_iter()
            .enumerate()
            .filter(|(_, schema)| {
                Self::pg_metadata_name_matches(select.selection.as_ref(), "relname", &schema.name)
            })
            .map(|(idx, schema)| {
                vec![
                    Value::Integer(10_000 + idx as i64),
                    Value::String("public".to_string()),
                    Value::String(schema.name),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                ]
            })
            .collect();
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    async fn pg_stat_user_indexes(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "relid".to_string(),
            "indexrelid".to_string(),
            "schemaname".to_string(),
            "relname".to_string(),
            "indexrelname".to_string(),
            "idx_scan".to_string(),
            "idx_tup_read".to_string(),
            "idx_tup_fetch".to_string(),
        ];
        let rows = self.pg_stat_index_rows(select).await?;
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    async fn pg_statio_user_indexes(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Option<QueryResult>, FusionError> {
        let columns = vec![
            "relid".to_string(),
            "indexrelid".to_string(),
            "schemaname".to_string(),
            "relname".to_string(),
            "indexrelname".to_string(),
            "idx_blks_read".to_string(),
            "idx_blks_hit".to_string(),
        ];
        let rows = self.pg_statio_index_rows(select).await?;
        Ok(Some(Self::project_pg_metadata_result(
            select, columns, rows,
        )?))
    }

    async fn pg_stat_index_rows(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Vec<Vec<Value>>, FusionError> {
        let mut rows = Vec::new();
        for (relid, indexrelid, relname, indexrelname) in self.pg_user_index_entries().await? {
            if !Self::pg_index_stat_row_matches(select.selection.as_ref(), &relname, &indexrelname)
            {
                continue;
            }
            rows.push(vec![
                Value::Integer(relid),
                Value::Integer(indexrelid),
                Value::String("public".to_string()),
                Value::String(relname),
                Value::String(indexrelname),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(0),
            ]);
        }
        Ok(rows)
    }

    async fn pg_statio_index_rows(
        &self,
        select: &sqlparser::ast::Select,
    ) -> std::result::Result<Vec<Vec<Value>>, FusionError> {
        let mut rows = Vec::new();
        for (relid, indexrelid, relname, indexrelname) in self.pg_user_index_entries().await? {
            if !Self::pg_index_stat_row_matches(select.selection.as_ref(), &relname, &indexrelname)
            {
                continue;
            }
            rows.push(vec![
                Value::Integer(relid),
                Value::Integer(indexrelid),
                Value::String("public".to_string()),
                Value::String(relname),
                Value::String(indexrelname),
                Value::Integer(0),
                Value::Integer(0),
            ]);
        }
        Ok(rows)
    }

    async fn pg_user_index_entries(
        &self,
    ) -> std::result::Result<Vec<(i64, i64, String, String)>, FusionError> {
        let schemas = self.load_pg_table_schemas().await?;
        let index_metas = self.load_pg_index_metadata().await?;
        let mut rows = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for (table_idx, schema) in schemas.into_iter().enumerate() {
            let relid = 10_000 + table_idx as i64;
            let mut ordinal = 0i64;

            if schema.get_primary_key_index().is_some() {
                ordinal += 1;
                let index_name = format!("{}_pkey", schema.name);
                if seen.insert((schema.name.to_ascii_lowercase(), index_name.clone())) {
                    rows.push((
                        relid,
                        20_000 + table_idx as i64 * 100 + ordinal,
                        schema.name.clone(),
                        index_name,
                    ));
                }
            }

            for (index_name, meta_str) in &index_metas {
                let Some(meta) = Executor::parse_index_meta(index_name, meta_str) else {
                    continue;
                };
                if !meta.table.eq_ignore_ascii_case(&schema.name) {
                    continue;
                }
                if !seen.insert((schema.name.to_ascii_lowercase(), index_name.clone())) {
                    continue;
                }
                ordinal += 1;
                rows.push((
                    relid,
                    20_000 + table_idx as i64 * 100 + ordinal,
                    schema.name.clone(),
                    index_name.clone(),
                ));
            }
        }

        rows.sort_by(|left, right| left.2.cmp(&right.2).then_with(|| left.3.cmp(&right.3)));
        Ok(rows)
    }

    fn project_pg_metadata_result(
        select: &sqlparser::ast::Select,
        base_columns: Vec<String>,
        base_rows: Vec<Vec<Value>>,
    ) -> std::result::Result<QueryResult, FusionError> {
        let wildcard = select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        });
        if wildcard {
            return Ok(QueryResult::Select {
                columns: base_columns,
                rows: base_rows,
            });
        }

        enum MetadataProjection {
            Column(usize),
            Constant(Value),
        }

        let mut projected_columns = Vec::with_capacity(select.projection.len());
        let mut projections = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            let (projection, output_name) = match item {
                SelectItem::UnnamedExpr(expr) => {
                    if let Some(value) = Self::pg_metadata_constant_value(expr) {
                        (MetadataProjection::Constant(value), expr.to_string())
                    } else {
                        let Some(name) = Self::pg_metadata_column_name(expr) else {
                            return Err(FusionError::Execution(format!(
                                "Unsupported metadata projection: {}",
                                expr
                            )));
                        };
                        let Some(index) = base_columns
                            .iter()
                            .position(|column| column.eq_ignore_ascii_case(name))
                        else {
                            return Err(FusionError::Execution(format!(
                                "Unknown metadata column: {}",
                                name
                            )));
                        };
                        (MetadataProjection::Column(index), name.to_string())
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Some(value) = Self::pg_metadata_constant_value(expr) {
                        (MetadataProjection::Constant(value), alias.value.clone())
                    } else {
                        let Some(name) = Self::pg_metadata_column_name(expr) else {
                            return Err(FusionError::Execution(format!(
                                "Unsupported metadata projection: {}",
                                expr
                            )));
                        };
                        let Some(index) = base_columns
                            .iter()
                            .position(|column| column.eq_ignore_ascii_case(name))
                        else {
                            return Err(FusionError::Execution(format!(
                                "Unknown metadata column: {}",
                                name
                            )));
                        };
                        (MetadataProjection::Column(index), alias.value.clone())
                    }
                }
                _ => {
                    return Err(FusionError::Execution(format!(
                        "Unsupported metadata projection: {}",
                        item
                    )))
                }
            };
            projections.push(projection);
            projected_columns.push(output_name);
        }

        let rows = base_rows
            .into_iter()
            .map(|row| {
                projections
                    .iter()
                    .map(|projection| match projection {
                        MetadataProjection::Column(index) => {
                            row.get(*index).cloned().unwrap_or(Value::Null)
                        }
                        MetadataProjection::Constant(value) => value.clone(),
                    })
                    .collect()
            })
            .collect();
        Ok(QueryResult::Select {
            columns: projected_columns,
            rows,
        })
    }

    fn pg_metadata_constant_value(expr: &Expr) -> Option<Value> {
        match expr {
            Expr::Value(value) => match &value.value {
                sqlparser::ast::Value::Number(s, _) => s
                    .parse::<i64>()
                    .map(Value::Integer)
                    .or_else(|_| s.parse::<f64>().map(Value::Float))
                    .ok(),
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s)
                | sqlparser::ast::Value::EscapedStringLiteral(s)
                | sqlparser::ast::Value::NationalStringLiteral(s) => Some(Value::String(s.clone())),
                sqlparser::ast::Value::Boolean(value) => Some(Value::Boolean(*value)),
                sqlparser::ast::Value::Null => Some(Value::Null),
                _ => None,
            },
            _ => None,
        }
    }

    fn pg_schema_row_matches(
        selection: Option<&Expr>,
        table_name: &str,
        column_name: Option<&str>,
    ) -> bool {
        let Some(expr) = selection else {
            return true;
        };
        match expr {
            Expr::BinaryOp { left, op, right } => {
                use sqlparser::ast::BinaryOperator;
                match op {
                    BinaryOperator::And => {
                        Self::pg_schema_row_matches(Some(left), table_name, column_name)
                            && Self::pg_schema_row_matches(Some(right), table_name, column_name)
                    }
                    BinaryOperator::Or => {
                        Self::pg_schema_row_matches(Some(left), table_name, column_name)
                            || Self::pg_schema_row_matches(Some(right), table_name, column_name)
                    }
                    BinaryOperator::Eq => {
                        let Some(column) = Self::pg_metadata_column_name(left)
                            .or_else(|| Self::pg_metadata_column_name(right))
                        else {
                            return true;
                        };
                        let Some(value) = Self::pg_metadata_string_value(left)
                            .or_else(|| Self::pg_metadata_string_value(right))
                        else {
                            return true;
                        };
                        match column {
                            "table_catalog" => value.eq_ignore_ascii_case("fusiondb"),
                            "table_schema" => value.eq_ignore_ascii_case("public"),
                            "table_name" => value.eq_ignore_ascii_case(table_name),
                            "column_name" => column_name
                                .map(|name| value.eq_ignore_ascii_case(name))
                                .unwrap_or(true),
                            _ => true,
                        }
                    }
                    _ => true,
                }
            }
            _ => true,
        }
    }

    fn pg_metadata_name_matches(selection: Option<&Expr>, column_name: &str, value: &str) -> bool {
        Self::pg_metadata_name_matches_with_params(selection, column_name, value, &[])
    }

    fn pg_metadata_name_matches_with_params(
        selection: Option<&Expr>,
        column_name: &str,
        value: &str,
        params: &[Value],
    ) -> bool {
        let Some(expr) = selection else {
            return true;
        };
        match expr {
            Expr::BinaryOp { left, op, right } => {
                use sqlparser::ast::BinaryOperator;
                match op {
                    BinaryOperator::And => {
                        Self::pg_metadata_name_matches_with_params(
                            Some(left),
                            column_name,
                            value,
                            params,
                        ) && Self::pg_metadata_name_matches_with_params(
                            Some(right),
                            column_name,
                            value,
                            params,
                        )
                    }
                    BinaryOperator::Or => {
                        Self::pg_metadata_name_matches_with_params(
                            Some(left),
                            column_name,
                            value,
                            params,
                        ) || Self::pg_metadata_name_matches_with_params(
                            Some(right),
                            column_name,
                            value,
                            params,
                        )
                    }
                    BinaryOperator::Eq => {
                        let Some(column) = Self::pg_metadata_column_name(left)
                            .or_else(|| Self::pg_metadata_column_name(right))
                        else {
                            return true;
                        };
                        if !column.eq_ignore_ascii_case(column_name) {
                            return true;
                        }
                        let Some(expected) = Self::pg_metadata_string_value_with_params(
                            left, params,
                        )
                        .or_else(|| Self::pg_metadata_string_value_with_params(right, params)) else {
                            return true;
                        };
                        expected.eq_ignore_ascii_case(value)
                    }
                    _ => true,
                }
            }
            _ => true,
        }
    }

    fn pg_attribute_row_matches(selection: Option<&Expr>, relid: i64, attname: &str) -> bool {
        let Some(expr) = selection else {
            return true;
        };
        match expr {
            Expr::BinaryOp { left, op, right } => {
                use sqlparser::ast::BinaryOperator;
                match op {
                    BinaryOperator::And => {
                        Self::pg_attribute_row_matches(Some(left), relid, attname)
                            && Self::pg_attribute_row_matches(Some(right), relid, attname)
                    }
                    BinaryOperator::Or => {
                        Self::pg_attribute_row_matches(Some(left), relid, attname)
                            || Self::pg_attribute_row_matches(Some(right), relid, attname)
                    }
                    BinaryOperator::Eq => {
                        let Some(column) = Self::pg_metadata_column_name(left)
                            .or_else(|| Self::pg_metadata_column_name(right))
                        else {
                            return true;
                        };
                        if column.eq_ignore_ascii_case("attname") {
                            let Some(expected) = Self::pg_metadata_string_value(left)
                                .or_else(|| Self::pg_metadata_string_value(right))
                            else {
                                return true;
                            };
                            return expected.eq_ignore_ascii_case(attname);
                        }
                        if column.eq_ignore_ascii_case("attrelid") {
                            let Some(expected) = Self::pg_metadata_i64_value(left)
                                .or_else(|| Self::pg_metadata_i64_value(right))
                            else {
                                return true;
                            };
                            return expected == relid;
                        }
                        true
                    }
                    _ => true,
                }
            }
            _ => true,
        }
    }

    fn pg_index_stat_row_matches(
        selection: Option<&Expr>,
        relname: &str,
        indexrelname: &str,
    ) -> bool {
        let Some(expr) = selection else {
            return true;
        };
        match expr {
            Expr::BinaryOp { left, op, right } => {
                use sqlparser::ast::BinaryOperator;
                match op {
                    BinaryOperator::And => {
                        Self::pg_index_stat_row_matches(Some(left), relname, indexrelname)
                            && Self::pg_index_stat_row_matches(Some(right), relname, indexrelname)
                    }
                    BinaryOperator::Or => {
                        Self::pg_index_stat_row_matches(Some(left), relname, indexrelname)
                            || Self::pg_index_stat_row_matches(Some(right), relname, indexrelname)
                    }
                    BinaryOperator::Eq => {
                        let Some(column) = Self::pg_metadata_column_name(left)
                            .or_else(|| Self::pg_metadata_column_name(right))
                        else {
                            return true;
                        };
                        let Some(expected) = Self::pg_metadata_string_value(left)
                            .or_else(|| Self::pg_metadata_string_value(right))
                        else {
                            return true;
                        };
                        match column {
                            name if name.eq_ignore_ascii_case("relname") => {
                                expected.eq_ignore_ascii_case(relname)
                            }
                            name if name.eq_ignore_ascii_case("indexrelname") => {
                                expected.eq_ignore_ascii_case(indexrelname)
                            }
                            name if name.eq_ignore_ascii_case("schemaname") => {
                                expected.eq_ignore_ascii_case("public")
                            }
                            _ => true,
                        }
                    }
                    _ => true,
                }
            }
            _ => true,
        }
    }

    fn pg_metadata_column_name(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.as_str()),
            Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.as_str()),
            _ => None,
        }
    }

    fn pg_metadata_string_value(expr: &Expr) -> Option<String> {
        Self::pg_metadata_string_value_with_params(expr, &[])
    }

    fn pg_metadata_string_value_with_params(expr: &Expr, params: &[Value]) -> Option<String> {
        match expr {
            Expr::Value(value) => match &value.value {
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s)
                | sqlparser::ast::Value::EscapedStringLiteral(s)
                | sqlparser::ast::Value::NationalStringLiteral(s) => Some(s.clone()),
                sqlparser::ast::Value::Placeholder(placeholder) => placeholder
                    .strip_prefix('$')
                    .and_then(|idx| idx.parse::<usize>().ok())
                    .and_then(|idx| params.get(idx.saturating_sub(1)))
                    .and_then(Self::metadata_param_as_string),
                _ => None,
            },
            _ => None,
        }
    }

    fn pg_metadata_i64_value(expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Value(value) => match &value.value {
                sqlparser::ast::Value::Number(s, _) => s.parse::<i64>().ok(),
                _ => None,
            },
            _ => None,
        }
    }

    fn first_metadata_i64_param_or_literal(query: &str, params: &[Value]) -> Option<i64> {
        params
            .iter()
            .find_map(|param| match param {
                Value::Integer(value) => Some(*value),
                Value::String(value) => value.parse::<i64>().ok(),
                _ => None,
            })
            .or_else(|| Self::first_i64_literal_after_equals(query))
    }

    fn first_i64_literal_after_equals(query: &str) -> Option<i64> {
        for part in query.split('=') {
            let token = part
                .trim_start()
                .chars()
                .take_while(|ch| ch.is_ascii_digit())
                .collect::<String>();
            if let Ok(value) = token.parse::<i64>() {
                return Some(value);
            }
        }
        None
    }

    fn pg_builtin_type_infos() -> &'static [PgTypeInfo] {
        &[
            PgTypeInfo {
                oid: 16,
                name: "bool",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1000,
                name: "_bool",
                element_oid: Some(16),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 20,
                name: "int8",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1016,
                name: "_int8",
                element_oid: Some(20),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 21,
                name: "int2",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1005,
                name: "_int2",
                element_oid: Some(21),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 23,
                name: "int4",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1007,
                name: "_int4",
                element_oid: Some(23),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 25,
                name: "text",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1009,
                name: "_text",
                element_oid: Some(25),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 700,
                name: "float4",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1021,
                name: "_float4",
                element_oid: Some(700),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 701,
                name: "float8",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1022,
                name: "_float8",
                element_oid: Some(701),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1082,
                name: "date",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1182,
                name: "_date",
                element_oid: Some(1082),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1114,
                name: "timestamp",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1115,
                name: "_timestamp",
                element_oid: Some(1114),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1184,
                name: "timestamptz",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1185,
                name: "_timestamptz",
                element_oid: Some(1184),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1186,
                name: "interval",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1187,
                name: "_interval",
                element_oid: Some(1186),
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1700,
                name: "numeric",
                element_oid: None,
                delimiter: ',',
            },
            PgTypeInfo {
                oid: 1231,
                name: "_numeric",
                element_oid: Some(1700),
                delimiter: ',',
            },
        ]
    }

    fn pg_type_info_for_oid(oid: i64) -> Option<PgTypeInfo> {
        Self::pg_builtin_type_infos()
            .iter()
            .copied()
            .find(|info| info.oid == oid)
    }

    fn pg_array_element_type_for_oid(oid: i64) -> Option<PgTypeInfo> {
        let array_type = Self::pg_type_info_for_oid(oid)?;
        let element_oid = array_type.element_oid?;
        Self::pg_type_info_for_oid(element_oid)
    }

    fn pg_array_oid_for_element_oid(oid: i64) -> Option<i64> {
        Self::pg_builtin_type_infos()
            .iter()
            .find(|info| info.element_oid == Some(oid))
            .map(|info| info.oid)
    }

    fn pg_type_oid_for_column_type(data_type: &str) -> i64 {
        match Self::pg_type_for_column_type(data_type) {
            Type::BOOL => 16,
            Type::INT8 => 20,
            Type::INT2 => 21,
            Type::INT4 => 23,
            Type::TEXT => 25,
            Type::FLOAT4 => 700,
            Type::FLOAT8 => 701,
            Type::DATE => 1082,
            Type::TIMESTAMP => 1114,
            Type::TIMESTAMPTZ => 1184,
            Type::INTERVAL => 1186,
            Type::NUMERIC => 1700,
            Type::BOOL_ARRAY => 1000,
            Type::INT8_ARRAY => 1016,
            Type::INT2_ARRAY => 1005,
            Type::INT4_ARRAY => 1007,
            Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => 1009,
            Type::FLOAT4_ARRAY => 1021,
            Type::FLOAT8_ARRAY => 1022,
            Type::DATE_ARRAY => 1182,
            Type::TIMESTAMP_ARRAY => 1115,
            Type::TIMESTAMPTZ_ARRAY => 1185,
            Type::INTERVAL_ARRAY => 1187,
            Type::NUMERIC_ARRAY => 1231,
            _ => 25,
        }
    }

    fn pg_type_len_for_column_type(data_type: &str) -> i64 {
        match Self::pg_type_for_column_type(data_type) {
            Type::BOOL => 1,
            Type::INT2 => 2,
            Type::INT4 => 4,
            Type::INT8 | Type::FLOAT8 | Type::TIMESTAMP | Type::TIMESTAMPTZ => 8,
            Type::FLOAT4 | Type::DATE => 4,
            _ => -1,
        }
    }

    fn pg_command_tag(message: &str) -> String {
        let trimmed = message.trim();
        let lower = trimmed.to_ascii_lowercase();

        if lower.starts_with("inserted ") {
            let rows = Self::first_i64_token(trimmed).unwrap_or(0);
            return format!("INSERT 0 {}", rows);
        }
        if lower.starts_with("updated ") {
            let rows = Self::first_i64_token(trimmed).unwrap_or(0);
            return format!("UPDATE {}", rows);
        }
        if lower.starts_with("deleted ") {
            let rows = Self::first_i64_token(trimmed).unwrap_or(0);
            return format!("DELETE {}", rows);
        }
        if lower.starts_with("copied ") || lower.starts_with("copy ") {
            let rows = Self::first_i64_token(trimmed).unwrap_or(0);
            return format!("COPY {}", rows);
        }
        if lower.starts_with("table ") && lower.contains(" created") {
            return "CREATE TABLE".to_string();
        }
        if lower.starts_with("index ") && lower.contains(" created") {
            return "CREATE INDEX".to_string();
        }
        if lower.starts_with("view ") && lower.contains(" created") {
            return "CREATE VIEW".to_string();
        }
        if lower.starts_with("dropped ") {
            return "DROP".to_string();
        }
        if lower.starts_with("truncated ") {
            return "TRUNCATE TABLE".to_string();
        }
        if lower.starts_with("vacuum ") || lower == "vacuum" {
            return "VACUUM".to_string();
        }
        if lower.contains(" column ") || lower.starts_with("renamed column ") {
            return "ALTER TABLE".to_string();
        }
        if matches!(trimmed, "BEGIN" | "COMMIT" | "ROLLBACK") {
            return trimmed.to_string();
        }
        trimmed.to_string()
    }

    fn first_i64_token(message: &str) -> Option<i64> {
        message
            .split(|ch: char| !ch.is_ascii_digit())
            .find(|part| !part.is_empty())
            .and_then(|part| part.parse::<i64>().ok())
    }

    async fn send_row_description_or_nodata<C>(
        &self,
        client: &mut C,
        fields: Vec<FieldInfo>,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        if fields.is_empty() {
            client
                .send(PgWireBackendMessage::NoData(NoData::new()))
                .await
                .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
        } else {
            client
                .send(PgWireBackendMessage::RowDescription(RowDescription::new(
                    fields.iter().map(Into::into).collect(),
                )))
                .await
                .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
        }
        Ok(())
    }

    fn is_copy_from_stdin_statement(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Copy {
                to: false,
                target: sqlparser::ast::CopyTarget::Stdin,
                ..
            }
        )
    }

    fn parse_copy_from_stdin_query(
        query: &str,
    ) -> std::result::Result<Option<Statement>, FusionError> {
        let trimmed = query.trim();
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("COPY ") || !upper.contains("STDIN") {
            return Ok(None);
        }

        let mut normalized = trimmed.to_string();
        if !normalized.ends_with(';') {
            normalized.push(';');
        }

        let statements = parse_sql(&normalized)
            .map_err(|e| FusionError::Execution(format!("Parse Error: {:?}", e)))?;
        let Some(stmt) = statements.first() else {
            return Ok(None);
        };

        if Self::is_copy_from_stdin_statement(stmt) {
            Ok(Some(stmt.clone()))
        } else {
            Ok(None)
        }
    }

    fn copy_column_count(stmt: &Statement) -> usize {
        let Statement::Copy { source, .. } = stmt else {
            return 0;
        };
        match source {
            sqlparser::ast::CopySource::Table { columns, .. } if !columns.is_empty() => {
                columns.len()
            }
            _ => 0,
        }
    }

    fn copy_in_response_for_statement(stmt: &Statement) -> CopyResponse {
        let columns = Self::copy_column_count(stmt);
        CopyResponse::new(0, columns, vec![0; columns])
    }

    async fn begin_copy_from_stdin(
        &self,
        stmt: Statement,
        query: String,
    ) -> PgWireResult<Response> {
        let response = Self::copy_in_response_for_statement(&stmt);
        let mut session = self.session.lock().await;
        session.copy_in = Some(CopyInState {
            statement: stmt,
            query,
            data: Vec::new(),
            simple_query: true,
        });
        Ok(Response::CopyIn(response))
    }

    fn pg_type_for_value(value: &Value) -> Type {
        match value {
            Value::Boolean(_) => Type::BOOL,
            Value::Integer(_) => Type::INT8,
            Value::Float(_) => Type::FLOAT8,
            Value::Decimal(_) => Type::NUMERIC,
            Value::Date(_) => Type::DATE,
            Value::Timestamp(_) => Type::TIMESTAMP,
            Value::Interval(_) => Type::INTERVAL,
            Value::Blob(_) => Type::BYTEA,
            Value::Array(values) => Self::pg_array_type_for_values(values),
            Value::String(_) | Value::Vector(_) | Value::Object(_) | Value::Null => Type::TEXT,
        }
    }

    fn pg_array_type_for_values(values: &[Value]) -> Type {
        values
            .iter()
            .find_map(Self::pg_array_type_for_value_member)
            .unwrap_or(Type::TEXT_ARRAY)
    }

    fn pg_array_type_for_value_member(value: &Value) -> Option<Type> {
        match value {
            Value::Null => None,
            Value::Boolean(_) => Some(Type::BOOL_ARRAY),
            Value::Integer(_) => Some(Type::INT8_ARRAY),
            Value::Float(_) => Some(Type::FLOAT8_ARRAY),
            Value::Decimal(_) => Some(Type::NUMERIC_ARRAY),
            Value::String(_) => Some(Type::TEXT_ARRAY),
            Value::Date(_) => Some(Type::DATE_ARRAY),
            Value::Timestamp(_) => Some(Type::TIMESTAMP_ARRAY),
            Value::Interval(_) => Some(Type::INTERVAL_ARRAY),
            Value::Blob(_) => Some(Type::BYTEA_ARRAY),
            Value::Array(values) => Some(Self::pg_array_type_for_values(values)),
            Value::Vector(_) | Value::Object(_) => Some(Type::TEXT_ARRAY),
        }
    }

    fn pg_type_for_column_type(data_type: &str) -> Type {
        let upper = data_type.trim().to_uppercase();
        if let Some(scalar) = Self::strip_array_suffixes(&upper) {
            let scalar_type = Self::pg_type_for_column_type(scalar);
            return Self::pg_array_type_for_scalar_type(&scalar_type);
        }
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

    fn strip_array_suffixes(data_type: &str) -> Option<&str> {
        let mut scalar = data_type.trim();
        let mut saw_array = false;
        while let Some(stripped) = scalar.strip_suffix("[]") {
            scalar = stripped.trim_end();
            saw_array = true;
        }
        saw_array.then_some(scalar)
    }

    fn pg_type_for_sql_type(data_type: &sqlparser::ast::DataType) -> Type {
        use sqlparser::ast::{ArrayElemTypeDef, DataType};
        match data_type {
            DataType::Array(array_type) => match array_type {
                ArrayElemTypeDef::AngleBracket(inner)
                | ArrayElemTypeDef::SquareBracket(inner, _)
                | ArrayElemTypeDef::Parenthesis(inner) => Self::pg_array_type_for_sql_type(inner),
                ArrayElemTypeDef::None => Type::TEXT_ARRAY,
            },
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

    fn pg_array_type_for_sql_type(data_type: &sqlparser::ast::DataType) -> Type {
        let scalar_type = Self::pg_type_for_sql_type(data_type);
        if Self::is_pg_array_type(&scalar_type) {
            scalar_type
        } else {
            Self::pg_array_type_for_scalar_type(&scalar_type)
        }
    }

    fn pg_array_type_for_scalar_type(data_type: &Type) -> Type {
        if Self::is_pg_array_type(data_type) {
            return data_type.clone();
        }
        match *data_type {
            Type::BOOL => Type::BOOL_ARRAY,
            Type::INT2 => Type::INT2_ARRAY,
            Type::INT4 => Type::INT4_ARRAY,
            Type::INT8 => Type::INT8_ARRAY,
            Type::FLOAT4 => Type::FLOAT4_ARRAY,
            Type::FLOAT8 => Type::FLOAT8_ARRAY,
            Type::NUMERIC => Type::NUMERIC_ARRAY,
            Type::DATE => Type::DATE_ARRAY,
            Type::TIMESTAMP => Type::TIMESTAMP_ARRAY,
            Type::TIMESTAMPTZ => Type::TIMESTAMPTZ_ARRAY,
            Type::TIME => Type::TIME_ARRAY,
            Type::INTERVAL => Type::INTERVAL_ARRAY,
            Type::BYTEA => Type::BYTEA_ARRAY,
            Type::JSON => Type::JSON_ARRAY,
            Type::JSONB => Type::JSONB_ARRAY,
            Type::TEXT | Type::VARCHAR => Type::TEXT_ARRAY,
            _ => Type::TEXT_ARRAY,
        }
    }

    fn pg_scalar_type_for_array_type(data_type: &Type) -> Type {
        match *data_type {
            Type::BOOL_ARRAY => Type::BOOL,
            Type::BYTEA_ARRAY => Type::BYTEA,
            Type::INT2_ARRAY => Type::INT2,
            Type::INT4_ARRAY => Type::INT4,
            Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => Type::TEXT,
            Type::INT8_ARRAY => Type::INT8,
            Type::FLOAT4_ARRAY => Type::FLOAT4,
            Type::FLOAT8_ARRAY => Type::FLOAT8,
            Type::TIMESTAMP_ARRAY => Type::TIMESTAMP,
            Type::DATE_ARRAY => Type::DATE,
            Type::TIME_ARRAY => Type::TIME,
            Type::TIMESTAMPTZ_ARRAY => Type::TIMESTAMPTZ,
            Type::INTERVAL_ARRAY => Type::INTERVAL,
            Type::NUMERIC_ARRAY => Type::NUMERIC,
            Type::JSON_ARRAY => Type::JSON,
            Type::JSONB_ARRAY => Type::JSONB,
            _ => Type::TEXT,
        }
    }

    fn value_as_pg_text(value: Value) -> Option<String> {
        match value {
            Value::Null => None,
            Value::Boolean(b) => Some(if b { "t".to_string() } else { "f".to_string() }),
            Value::Integer(i) => Some(i.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::Decimal(d) => Some(d),
            Value::String(s) => Some(s),
            Value::Date(days) => Some(Value::format_date(days)),
            Value::Timestamp(micros) => Some(Value::format_timestamp(micros)),
            Value::Interval(micros) => Some(Value::format_interval(micros)),
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
            Value::Array(v) => Some(Self::pg_array_text(&v)),
            Value::Object(v) => Some(format!("{:?}", v)),
        }
    }

    fn pg_array_text(values: &[Value]) -> String {
        let mut out = String::from("{");
        for (idx, value) in values.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            out.push_str(&Self::pg_array_element_text(value));
        }
        out.push('}');
        out
    }

    fn pg_array_element_text(value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Array(values) => Self::pg_array_text(values),
            Value::Boolean(value) => {
                if *value {
                    "t".to_string()
                } else {
                    "f".to_string()
                }
            }
            Value::Integer(value) => value.to_string(),
            Value::Float(value) => value.to_string(),
            Value::Decimal(value) => value.clone(),
            Value::Date(days) => Value::format_date(*days),
            Value::Timestamp(micros) => Value::format_timestamp(*micros),
            Value::Interval(micros) => Value::format_interval(*micros),
            Value::Blob(bytes) => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let mut out = String::with_capacity(2 + bytes.len() * 2);
                out.push_str("\\x");
                for byte in bytes {
                    out.push(HEX[(byte >> 4) as usize] as char);
                    out.push(HEX[(byte & 0x0f) as usize] as char);
                }
                Self::quote_pg_array_element(&out)
            }
            Value::String(value) => Self::quote_pg_array_element(value),
            Value::Vector(value) => Self::quote_pg_array_element(&format!("{:?}", value)),
            Value::Object(value) => Self::quote_pg_array_element(&format!("{:?}", value)),
        }
    }

    fn quote_pg_array_element(value: &str) -> String {
        let mut out = String::with_capacity(value.len() + 2);
        out.push('"');
        for ch in value.chars() {
            if matches!(ch, '"' | '\\') {
                out.push('\\');
            }
            out.push(ch);
        }
        out.push('"');
        out
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

    fn fields_with_format(
        columns: &[String],
        rows: &[Vec<Value>],
        result_format_codes: &[i16],
    ) -> Arc<Vec<FieldInfo>> {
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

                    FieldInfo::new(
                        name.clone(),
                        None,
                        None,
                        datatype.clone(),
                        Self::result_field_format_for_type(&datatype, result_format_codes, idx),
                    )
                })
                .collect::<Vec<_>>(),
        )
    }

    fn result_field_format(result_format_codes: &[i16], idx: usize) -> FieldFormat {
        match result_format_codes {
            [] => FieldFormat::Text,
            [format] => FieldFormat::from(*format),
            formats => FieldFormat::from(formats.get(idx).copied().unwrap_or(0)),
        }
    }

    fn result_field_format_for_type(
        data_type: &Type,
        result_format_codes: &[i16],
        idx: usize,
    ) -> FieldFormat {
        if Self::is_pg_array_type(data_type) {
            FieldFormat::Text
        } else {
            Self::result_field_format(result_format_codes, idx)
        }
    }

    fn is_pg_array_type(data_type: &Type) -> bool {
        matches!(
            *data_type,
            Type::BOOL_ARRAY
                | Type::BYTEA_ARRAY
                | Type::INT2_ARRAY
                | Type::INT4_ARRAY
                | Type::TEXT_ARRAY
                | Type::VARCHAR_ARRAY
                | Type::INT8_ARRAY
                | Type::FLOAT4_ARRAY
                | Type::FLOAT8_ARRAY
                | Type::TIMESTAMP_ARRAY
                | Type::DATE_ARRAY
                | Type::TIME_ARRAY
                | Type::TIMESTAMPTZ_ARRAY
                | Type::INTERVAL_ARRAY
                | Type::NUMERIC_ARRAY
                | Type::JSON_ARRAY
                | Type::JSONB_ARRAY
        )
    }

    fn apply_result_formats(fields: Vec<FieldInfo>, result_format_codes: &[i16]) -> Vec<FieldInfo> {
        fields
            .into_iter()
            .enumerate()
            .map(|(idx, field)| {
                FieldInfo::new(
                    field.name().to_string(),
                    None,
                    None,
                    field.datatype().clone(),
                    Self::result_field_format_for_type(field.datatype(), result_format_codes, idx),
                )
            })
            .collect()
    }

    fn encode_row_for_fields(
        fields: Arc<Vec<FieldInfo>>,
        row: Vec<Value>,
    ) -> PgWireResult<pgwire::messages::data::DataRow> {
        Self::encode_row_with_formats(fields, row)
    }

    fn encode_row_with_formats(
        fields: Arc<Vec<FieldInfo>>,
        row: Vec<Value>,
    ) -> PgWireResult<pgwire::messages::data::DataRow> {
        let mut out = BytesMut::with_capacity(row.len() * 16);
        let mut field_count = 0i16;

        for (idx, value) in row.into_iter().enumerate() {
            field_count += 1;
            let Some(field) = fields.get(idx) else {
                out.put_i32(-1);
                continue;
            };

            if matches!(field.format(), FieldFormat::Binary) {
                let mut value_buf = BytesMut::with_capacity(16);
                if Self::put_binary_value(&mut value_buf, field.datatype(), value) {
                    out.put_i32(value_buf.len() as i32);
                    out.extend_from_slice(&value_buf);
                } else {
                    out.put_i32(-1);
                }
                continue;
            }

            if let Some(text) = Self::value_as_pg_text(value) {
                out.put_i32(text.len() as i32);
                out.extend_from_slice(text.as_bytes());
            } else {
                out.put_i32(-1);
            }
        }

        Ok(pgwire::messages::data::DataRow::new(out, field_count))
    }

    fn encode_row(
        fields: Arc<Vec<FieldInfo>>,
        row: Vec<Value>,
    ) -> PgWireResult<pgwire::messages::data::DataRow> {
        Self::encode_row_with_formats(fields, row)
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
            Value::Decimal(s) => {
                if *data_type == Type::NUMERIC {
                    Self::put_pg_numeric(out, &s)
                } else {
                    out.extend_from_slice(s.as_bytes());
                    true
                }
            }
            Value::String(s) => {
                out.extend_from_slice(s.as_bytes());
                true
            }
            Value::Date(days) => {
                if *data_type == Type::DATE {
                    out.put_i32(days - Self::postgres_epoch_days_from_ce());
                } else {
                    out.extend_from_slice(Value::format_date(days).as_bytes());
                }
                true
            }
            Value::Timestamp(micros) => {
                if matches!(*data_type, Type::TIMESTAMP | Type::TIMESTAMPTZ) {
                    out.put_i64(micros - Self::POSTGRES_EPOCH_UNIX_MICROS);
                } else {
                    out.extend_from_slice(Value::format_timestamp(micros).as_bytes());
                }
                true
            }
            Value::Interval(micros) => {
                if *data_type == Type::INTERVAL {
                    out.put_i64(micros);
                    out.put_i32(0);
                    out.put_i32(0);
                } else {
                    out.extend_from_slice(Value::format_interval(micros).as_bytes());
                }
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
                out.extend_from_slice(Self::pg_array_text(&v).as_bytes());
                true
            }
            Value::Object(v) => {
                out.extend_from_slice(format!("{:?}", v).as_bytes());
                true
            }
        }
    }

    fn postgres_epoch_days_from_ce() -> i32 {
        chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .num_days_from_ce()
    }

    fn put_pg_numeric(out: &mut BytesMut, value: &str) -> bool {
        let Some(normalized) = Value::normalize_decimal(value) else {
            return false;
        };
        let negative = normalized.starts_with('-');
        let body = normalized.trim_start_matches('-');
        let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
        let dscale = frac_part.len() as i16;

        if int_part == "0" && frac_part.is_empty() {
            out.put_i16(0);
            out.put_i16(0);
            out.put_i16(if negative { 0x4000 } else { 0 });
            out.put_i16(0);
            return true;
        }

        let mut int_groups = Vec::new();
        let mut start = int_part.len();
        while start > 0 {
            let group_start = start.saturating_sub(4);
            int_groups.push(int_part[group_start..start].parse::<i16>().unwrap_or(0));
            start = group_start;
        }
        int_groups.reverse();
        while int_groups.first() == Some(&0) {
            int_groups.remove(0);
        }

        let mut frac_groups = Vec::new();
        if !frac_part.is_empty() {
            let mut padded = frac_part.to_string();
            while padded.len() % 4 != 0 {
                padded.push('0');
            }
            for chunk in padded.as_bytes().chunks(4) {
                let group = std::str::from_utf8(chunk)
                    .ok()
                    .and_then(|s| s.parse::<i16>().ok())
                    .unwrap_or(0);
                frac_groups.push(group);
            }
        }
        while frac_groups.last() == Some(&0) {
            frac_groups.pop();
        }

        let weight = if !int_groups.is_empty() {
            int_groups.len() as i16 - 1
        } else {
            -1
        };
        let ndigits = int_groups.len() + frac_groups.len();
        out.put_i16(ndigits as i16);
        out.put_i16(weight);
        out.put_i16(if negative { 0x4000 } else { 0 });
        out.put_i16(dscale);
        for digit in int_groups.into_iter().chain(frac_groups) {
            out.put_i16(digit);
        }
        true
    }

    async fn infer_parameter_types_from_query(
        &self,
        query: &str,
        provided_oids: &[u32],
    ) -> Vec<Type> {
        self.infer_parameter_types_from_query_with_schema_loader(query, provided_oids, None)
            .await
    }

    async fn infer_parameter_types_from_query_in_transaction(
        &self,
        query: &str,
        provided_oids: &[u32],
        txn: &dyn Transaction,
    ) -> Vec<Type> {
        self.infer_parameter_types_from_query_with_schema_loader(query, provided_oids, Some(txn))
            .await
    }

    async fn infer_parameter_types_from_query_with_schema_loader(
        &self,
        query: &str,
        provided_oids: &[u32],
        txn: Option<&dyn Transaction>,
    ) -> Vec<Type> {
        Self::trace_query("infer-params start", query);
        let text_placeholder_count = Self::max_placeholder_in_text(query);
        let ast_placeholder_count = parse_sql(query)
            .ok()
            .map(|statements| {
                statements
                    .iter()
                    .map(Self::max_placeholder_in_statement)
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        let placeholder_count = ast_placeholder_count.max(text_placeholder_count);
        let count = placeholder_count.max(provided_oids.len());
        let mut types = vec![Type::TEXT; count];
        let mut client_provided = vec![false; count];
        for (idx, oid) in provided_oids.iter().enumerate() {
            if let Some(ty) = Type::from_oid(*oid) {
                if idx < types.len() {
                    types[idx] = ty;
                    client_provided[idx] = true;
                }
            }
        }

        if let Ok(statements) = parse_sql(query) {
            let schemas = if let Some(txn) = txn {
                Self::load_parameter_inference_schemas_in_transaction(&statements, txn).await
            } else {
                self.load_parameter_inference_schemas(&statements).await
            };
            for stmt in &statements {
                Self::infer_parameter_types_from_statement(
                    stmt,
                    &schemas,
                    &client_provided,
                    &mut types,
                );
            }
        }

        if Self::trace_enabled() {
            Self::trace(format!(
                "infer-params done: placeholders={} provided_oids={} inferred_oids={:?}",
                placeholder_count,
                provided_oids.len(),
                types.iter().map(Type::oid).collect::<Vec<_>>()
            ));
        }
        types
    }

    async fn load_parameter_inference_schemas(
        &self,
        statements: &[Statement],
    ) -> HashMap<String, TableSchema> {
        let Ok(txn) = self.storage.begin_transaction().await else {
            return HashMap::new();
        };
        Self::load_parameter_inference_schemas_in_transaction(statements, &*txn).await
    }

    async fn load_parameter_inference_schemas_in_transaction(
        statements: &[Statement],
        txn: &dyn Transaction,
    ) -> HashMap<String, TableSchema> {
        let mut table_names = Vec::new();
        for stmt in statements {
            Self::collect_inference_table_names_from_statement(stmt, &mut table_names);
        }
        table_names.sort();
        table_names.dedup();

        let mut schemas = HashMap::new();
        for table_name in table_names {
            let schema_key = format!("schema:{}", table_name);
            let Some(schema_bytes) = (match txn.get(schema_key.as_bytes()).await {
                Ok(bytes) => bytes,
                Err(_) => None,
            }) else {
                continue;
            };
            let Ok(schema) = bincode::deserialize::<TableSchema>(&schema_bytes) else {
                continue;
            };
            schemas.insert(table_name.to_ascii_lowercase(), schema);
        }
        schemas
    }

    fn collect_inference_table_names_from_statement(
        stmt: &Statement,
        table_names: &mut Vec<String>,
    ) {
        match stmt {
            Statement::Query(query) => {
                Self::collect_inference_table_names_from_query(query, table_names)
            }
            Statement::Insert(insert) => table_names.push(insert.table.to_string()),
            Statement::Update(update) => {
                Self::collect_inference_table_names_from_table_with_joins(
                    &update.table,
                    table_names,
                );
            }
            Statement::Delete(delete) => {
                Self::collect_inference_table_names_from_delete(delete, table_names)
            }
            Statement::Explain { statement, .. } => {
                Self::collect_inference_table_names_from_statement(statement, table_names)
            }
            _ => {}
        }
    }

    fn collect_inference_table_names_from_query(
        query: &sqlparser::ast::Query,
        table_names: &mut Vec<String>,
    ) {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                Self::collect_inference_table_names_from_query(&cte.query, table_names);
            }
        }
        Self::collect_inference_table_names_from_set_expr(query.body.as_ref(), table_names);
    }

    fn collect_inference_table_names_from_set_expr(
        set_expr: &SetExpr,
        table_names: &mut Vec<String>,
    ) {
        match set_expr {
            SetExpr::Select(select) => {
                for table in &select.from {
                    Self::collect_inference_table_names_from_table_with_joins(table, table_names);
                }
            }
            SetExpr::Query(query) => {
                Self::collect_inference_table_names_from_query(query, table_names)
            }
            SetExpr::SetOperation { left, right, .. } => {
                Self::collect_inference_table_names_from_set_expr(left, table_names);
                Self::collect_inference_table_names_from_set_expr(right, table_names);
            }
            _ => {}
        }
    }

    fn collect_inference_table_names_from_table_with_joins(
        table: &sqlparser::ast::TableWithJoins,
        table_names: &mut Vec<String>,
    ) {
        if let Some(table_name) = Self::inference_table_name_from_factor(&table.relation) {
            table_names.push(table_name);
        }
        for join in &table.joins {
            if let Some(table_name) = Self::inference_table_name_from_factor(&join.relation) {
                table_names.push(table_name);
            }
        }
    }

    fn collect_inference_table_names_from_delete(
        delete: &sqlparser::ast::Delete,
        table_names: &mut Vec<String>,
    ) {
        let tables = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables)
            | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
        };
        for table in tables {
            Self::collect_inference_table_names_from_table_with_joins(table, table_names);
        }
    }

    fn inference_table_name_from_factor(factor: &sqlparser::ast::TableFactor) -> Option<String> {
        match factor {
            sqlparser::ast::TableFactor::Table { name, .. } => Some(name.to_string()),
            _ => None,
        }
    }

    fn infer_parameter_types_from_statement(
        stmt: &Statement,
        schemas: &HashMap<String, TableSchema>,
        client_provided: &[bool],
        types: &mut [Type],
    ) {
        match stmt {
            Statement::Query(query) => {
                Self::infer_parameter_types_from_query_ast(query, schemas, client_provided, types)
            }
            Statement::Update(update) => {
                let schema = Self::single_schema_for_table_with_joins(&update.table, schemas);
                if let Some(selection) = &update.selection {
                    Self::infer_parameter_types_from_expr(
                        selection,
                        schema.as_ref(),
                        client_provided,
                        types,
                    );
                }
                for assignment in &update.assignments {
                    Self::infer_parameter_types_from_expr(
                        &assignment.value,
                        schema.as_ref(),
                        client_provided,
                        types,
                    );
                }
            }
            Statement::Delete(delete) => {
                let schema = Self::single_schema_for_delete(delete, schemas);
                if let Some(selection) = &delete.selection {
                    Self::infer_parameter_types_from_expr(
                        selection,
                        schema.as_ref(),
                        client_provided,
                        types,
                    );
                }
            }
            Statement::Insert(insert) => {
                Self::infer_parameter_types_from_insert(insert, schemas, client_provided, types);
                if let Some(source) = &insert.source {
                    Self::infer_parameter_types_from_query_ast(
                        source,
                        schemas,
                        client_provided,
                        types,
                    );
                }
            }
            Statement::Explain { statement, .. } => Self::infer_parameter_types_from_statement(
                statement,
                schemas,
                client_provided,
                types,
            ),
            _ => {}
        }
    }

    fn infer_parameter_types_from_query_ast(
        query: &sqlparser::ast::Query,
        schemas: &HashMap<String, TableSchema>,
        client_provided: &[bool],
        types: &mut [Type],
    ) {
        if let SetExpr::Select(select) = query.body.as_ref() {
            let schema = select
                .from
                .first()
                .and_then(|table| Self::single_schema_for_table_with_joins(table, schemas));

            if let Some(selection) = &select.selection {
                Self::infer_parameter_types_from_expr(
                    selection,
                    schema.as_ref(),
                    client_provided,
                    types,
                );
            }
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item
                {
                    Self::infer_parameter_types_from_expr(
                        expr,
                        schema.as_ref(),
                        client_provided,
                        types,
                    );
                }
            }
        }
    }

    fn single_schema_for_table_with_joins(
        table: &sqlparser::ast::TableWithJoins,
        schemas: &HashMap<String, TableSchema>,
    ) -> Option<TableSchema> {
        if !table.joins.is_empty() {
            return None;
        }
        let table_name = Self::inference_table_name_from_factor(&table.relation)?;
        schemas.get(&table_name.to_ascii_lowercase()).cloned()
    }

    fn single_schema_for_delete(
        delete: &sqlparser::ast::Delete,
        schemas: &HashMap<String, TableSchema>,
    ) -> Option<TableSchema> {
        let tables = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables)
            | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
        };
        let table = tables.first()?;
        if tables.len() == 1 {
            Self::single_schema_for_table_with_joins(table, schemas)
        } else {
            None
        }
    }

    fn infer_parameter_types_from_insert(
        insert: &sqlparser::ast::Insert,
        schemas: &HashMap<String, TableSchema>,
        client_provided: &[bool],
        types: &mut [Type],
    ) {
        let Some(schema) = schemas.get(&insert.table.to_string().to_ascii_lowercase()) else {
            return;
        };
        let Some(source) = &insert.source else {
            return;
        };
        let SetExpr::Values(values) = source.body.as_ref() else {
            return;
        };

        let column_indices: Vec<usize> = if insert.columns.is_empty() {
            (0..schema.columns.len()).collect()
        } else {
            insert
                .columns
                .iter()
                .filter_map(|ident| {
                    schema
                        .columns
                        .iter()
                        .position(|column| column.name.eq_ignore_ascii_case(&ident.value))
                })
                .collect()
        };
        for row in &values.rows {
            for (expr, column_idx) in row.iter().zip(column_indices.iter().copied()) {
                let ty = Self::pg_type_for_column_type(&schema.columns[column_idx].data_type);
                Self::assign_placeholder_type(expr, ty, client_provided, types);
            }
        }
    }

    fn infer_parameter_types_from_expr(
        expr: &Expr,
        schema_hint: Option<&TableSchema>,
        client_provided: &[bool],
        types: &mut [Type],
    ) {
        match expr {
            Expr::BinaryOp { left, op, right }
                if matches!(op, sqlparser::ast::BinaryOperator::Eq) =>
            {
                if let Some((idx, ty)) =
                    Self::placeholder_column_type_pair(left, right, schema_hint)
                {
                    Self::assign_parameter_type(idx, ty, client_provided, types);
                }
                if let Some((idx, ty)) =
                    Self::placeholder_column_type_pair(right, left, schema_hint)
                {
                    Self::assign_parameter_type(idx, ty, client_provided, types);
                }
                Self::infer_parameter_types_from_expr(left, schema_hint, client_provided, types);
                Self::infer_parameter_types_from_expr(right, schema_hint, client_provided, types);
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::infer_parameter_types_from_expr(left, schema_hint, client_provided, types);
                Self::infer_parameter_types_from_expr(right, schema_hint, client_provided, types);
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Nested(expr)
            | Expr::Cast { expr, .. }
            | Expr::Ceil { expr, .. }
            | Expr::Floor { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::InSubquery { expr, .. } => {
                Self::infer_parameter_types_from_expr(expr, schema_hint, client_provided, types)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::infer_parameter_types_from_expr(expr, schema_hint, client_provided, types);
                Self::infer_parameter_types_from_expr(low, schema_hint, client_provided, types);
                Self::infer_parameter_types_from_expr(high, schema_hint, client_provided, types);
            }
            Expr::InList { expr, list, .. } => {
                Self::infer_parameter_types_from_expr(expr, schema_hint, client_provided, types);
                for item in list {
                    Self::infer_parameter_types_from_expr(
                        item,
                        schema_hint,
                        client_provided,
                        types,
                    );
                }
            }
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                Self::infer_parameter_types_from_expr(expr, schema_hint, client_provided, types);
                Self::infer_parameter_types_from_expr(pattern, schema_hint, client_provided, types);
            }
            Expr::Function(func) => {
                if let FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                            Self::infer_parameter_types_from_expr(
                                expr,
                                schema_hint,
                                client_provided,
                                types,
                            );
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
                    Self::infer_parameter_types_from_expr(
                        expr,
                        schema_hint,
                        client_provided,
                        types,
                    );
                }
                for when in conditions {
                    Self::infer_parameter_types_from_expr(
                        &when.condition,
                        schema_hint,
                        client_provided,
                        types,
                    );
                    Self::infer_parameter_types_from_expr(
                        &when.result,
                        schema_hint,
                        client_provided,
                        types,
                    );
                }
                if let Some(expr) = else_result {
                    Self::infer_parameter_types_from_expr(
                        expr,
                        schema_hint,
                        client_provided,
                        types,
                    );
                }
            }
            Expr::Array(array) => {
                for expr in &array.elem {
                    Self::infer_parameter_types_from_expr(
                        expr,
                        schema_hint,
                        client_provided,
                        types,
                    );
                }
            }
            _ => {}
        }
    }

    fn assign_placeholder_type(
        expr: &Expr,
        ty: Type,
        client_provided: &[bool],
        types: &mut [Type],
    ) {
        if let Some(idx) = Self::placeholder_index(expr) {
            Self::assign_parameter_type(idx, ty, client_provided, types);
        }
    }

    fn assign_parameter_type(idx: usize, ty: Type, client_provided: &[bool], types: &mut [Type]) {
        let slot_idx = idx.saturating_sub(1);
        if client_provided.get(slot_idx).copied().unwrap_or(false) {
            return;
        }
        if let Some(slot) = types.get_mut(slot_idx) {
            *slot = ty;
        }
    }

    fn placeholder_index(expr: &Expr) -> Option<usize> {
        let Expr::Value(value) = expr else {
            return None;
        };
        let sqlparser::ast::Value::Placeholder(p) = &value.value else {
            return None;
        };
        p.strip_prefix('$')
            .unwrap_or(p)
            .parse::<usize>()
            .ok()
            .filter(|idx| *idx > 0)
    }

    fn placeholder_column_type_pair(
        placeholder_expr: &Expr,
        column_expr: &Expr,
        schema_hint: Option<&TableSchema>,
    ) -> Option<(usize, Type)> {
        let idx = Self::placeholder_index(placeholder_expr)?;

        let column_name = match column_expr {
            Expr::Identifier(ident) => ident.value.as_str(),
            Expr::CompoundIdentifier(idents) => idents.last()?.value.as_str(),
            _ => return None,
        };
        if let Some(schema) = schema_hint {
            if let Some(column) = schema
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(column_name))
            {
                return Some((idx, Self::pg_type_for_column_type(&column.data_type)));
            }
        }
        let upper = column_name.to_ascii_uppercase();
        let ty = if upper.ends_with("ID") || upper == "ID" || upper.ends_with("_ID") {
            Type::INT4
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
                .map(|value| {
                    if *param_type == Type::NUMERIC {
                        Value::decimal_from_f64(value).unwrap_or(Value::Float(value))
                    } else {
                        Value::Float(value)
                    }
                })
                .unwrap_or(Value::String(s)),
            Type::BOOL => match s.trim().to_ascii_lowercase().as_str() {
                "t" | "true" | "1" | "yes" | "on" => Value::Boolean(true),
                "f" | "false" | "0" | "no" | "off" => Value::Boolean(false),
                _ => Value::String(s),
            },
            Type::BYTEA => Value::Blob(bytes.to_vec()),
            Type::DATE => Value::date_from_str(&s).unwrap_or(Value::String(s)),
            Type::TIMESTAMP | Type::TIMESTAMPTZ => {
                Value::timestamp_from_str(&s).unwrap_or(Value::String(s))
            }
            Type::INTERVAL => Value::interval_from_str(&s).unwrap_or(Value::String(s)),
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
            Type::DATE if bytes.len() == 4 => {
                let pg_days = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Value::Date(Self::postgres_epoch_days_from_ce() + pg_days)
            }
            Type::TIMESTAMP | Type::TIMESTAMPTZ if bytes.len() == 8 => {
                let pg_micros = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Value::Timestamp(Self::POSTGRES_EPOCH_UNIX_MICROS + pg_micros)
            }
            Type::INTERVAL if bytes.len() == 16 => {
                let micros = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                let days = i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
                let months = i32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
                let total = micros
                    .saturating_add(days as i64 * 86_400_000_000)
                    .saturating_add(months as i64 * 30 * 86_400_000_000);
                Value::Interval(total)
            }
            Type::NUMERIC => Self::decode_pg_numeric(bytes)
                .map(Value::Decimal)
                .unwrap_or_else(|| Value::String(String::from_utf8_lossy(bytes).to_string())),
            Type::BYTEA => Value::Blob(bytes.to_vec()),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::UNKNOWN => {
                Value::String(String::from_utf8_lossy(bytes).to_string())
            }
            _ => String::from_utf8(bytes.to_vec())
                .map(|s| Self::decode_text_param(s.as_bytes(), param_type))
                .unwrap_or_else(|_| Value::Blob(bytes.to_vec())),
        }
    }

    fn decode_pg_numeric(bytes: &[u8]) -> Option<String> {
        if bytes.len() < 8 || bytes.len() % 2 != 0 {
            return None;
        }
        let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]) as usize;
        let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
        let sign = i16::from_be_bytes([bytes[4], bytes[5]]);
        let dscale = i16::from_be_bytes([bytes[6], bytes[7]]).max(0) as usize;
        if bytes.len() < 8 + ndigits * 2 {
            return None;
        }
        if sign == -0x4000 {
            return None;
        }

        let mut digits = Vec::with_capacity(ndigits);
        for idx in 0..ndigits {
            let offset = 8 + idx * 2;
            digits.push(i16::from_be_bytes([bytes[offset], bytes[offset + 1]]).max(0) as u16);
        }

        let mut int_part = String::new();
        if weight >= 0 {
            for pos in (0..=weight as usize).rev() {
                let digit_idx = weight as isize - pos as isize;
                let digit = digit_idx
                    .try_into()
                    .ok()
                    .and_then(|idx: usize| digits.get(idx))
                    .copied()
                    .unwrap_or(0);
                if int_part.is_empty() {
                    int_part.push_str(&digit.to_string());
                } else {
                    int_part.push_str(&format!("{:04}", digit));
                }
            }
        }
        if int_part.is_empty() {
            int_part.push('0');
        }

        let mut frac_part = String::new();
        let mut pos = -1isize;
        while frac_part.len() < dscale {
            let digit_idx = weight as isize - pos;
            let digit = digit_idx
                .try_into()
                .ok()
                .and_then(|idx: usize| digits.get(idx))
                .copied()
                .unwrap_or(0);
            frac_part.push_str(&format!("{:04}", digit));
            pos -= 1;
        }
        frac_part.truncate(dscale);

        let mut out = String::new();
        if sign == 0x4000 {
            out.push('-');
        }
        out.push_str(&int_part);
        if !frac_part.is_empty() {
            out.push('.');
            out.push_str(&frac_part);
        }
        Value::normalize_decimal(&out)
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

    async fn describe_query_fields(
        &self,
        query: &str,
        params: &[Value],
        result_format_codes: &[i16],
    ) -> PgWireResult<Vec<FieldInfo>> {
        Self::trace_query("describe start", query);
        match self.try_execute_pg_metadata_query(query, params).await {
            Ok(Some(QueryResult::Select { columns, rows })) => {
                Self::trace(format!(
                    "describe metadata result: columns={} rows={}",
                    columns.len(),
                    rows.len()
                ));
                return Ok(
                    Self::fields_with_format(&columns, &rows, result_format_codes)
                        .iter()
                        .map(|field| {
                            FieldInfo::new(
                                field.name().to_string(),
                                None,
                                None,
                                field.datatype().clone(),
                                field.format(),
                            )
                        })
                        .collect(),
                );
            }
            Ok(Some(QueryResult::Success { .. })) | Ok(None) => {}
            Err(e) => {
                return Err(PgWireError::ApiError(Box::new(std::io::Error::other(
                    format!("Metadata describe error: {:?}", e),
                ))));
            }
        }

        let statements = parse_sql(query).map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                "Parse Error: {:?}",
                e
            ))))
        })?;
        let Some(stmt) = statements.first() else {
            Self::trace("describe done: empty statement");
            return Ok(Vec::new());
        };
        let fields = self.describe_statement_fields(stmt).await?;
        Self::trace(format!("describe done: fields={}", fields.len()));
        Ok(Self::apply_result_formats(fields, result_format_codes))
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
        if let Some(fields) = self.describe_query_fields_from_ctes(query).await? {
            return Ok(fields);
        }

        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(Vec::new());
        };
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return Ok(self.describe_projection_fallback(&select.projection));
        }
        if let sqlparser::ast::TableFactor::Derived { subquery, .. } = &select.from[0].relation {
            let wildcard = select.projection.iter().any(|item| {
                matches!(
                    item,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                )
            });
            if wildcard {
                return Box::pin(self.describe_select_query_fields(subquery)).await;
            }
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
        let empty_schema = TableSchema::new("__describe_fallback".to_string(), Vec::new());
        projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::UnnamedExpr(expr) => Some(FieldInfo::new(
                    expr.to_string(),
                    None,
                    None,
                    Self::pg_type_for_literal_expr(expr)
                        .unwrap_or_else(|| self.pg_type_for_projection_expr(expr, &empty_schema)),
                    FieldFormat::Text,
                )),
                SelectItem::ExprWithAlias { expr, alias } => Some(FieldInfo::new(
                    alias.value.clone(),
                    None,
                    None,
                    Self::pg_type_for_literal_expr(expr)
                        .unwrap_or_else(|| self.pg_type_for_projection_expr(expr, &empty_schema)),
                    FieldFormat::Text,
                )),
                _ => None,
            })
            .collect()
    }

    async fn describe_query_fields_from_ctes(
        &self,
        query: &sqlparser::ast::Query,
    ) -> PgWireResult<Option<Vec<FieldInfo>>> {
        let Some(with) = &query.with else {
            return Ok(None);
        };

        let mut schemas: HashMap<String, TableSchema> = HashMap::new();
        for cte in &with.cte_tables {
            let Some(schema) = self.describe_cte_schema(cte, &schemas).await? else {
                continue;
            };
            schemas.insert(cte.alias.name.value.to_ascii_lowercase(), schema);
        }

        let Some(fields) = self
            .describe_query_fields_from_schema_map(query, &schemas)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(fields))
    }

    async fn describe_cte_schema(
        &self,
        cte: &sqlparser::ast::Cte,
        schemas: &HashMap<String, TableSchema>,
    ) -> PgWireResult<Option<TableSchema>> {
        let Some(fields) =
            Box::pin(self.describe_query_fields_from_schema_map(&cte.query, schemas)).await?
        else {
            return Ok(None);
        };

        let columns = fields
            .into_iter()
            .enumerate()
            .map(|(idx, field)| {
                let name = cte
                    .alias
                    .columns
                    .get(idx)
                    .map(|column| column.name.value.clone())
                    .unwrap_or_else(|| field.name().to_string());
                Self::catalog_column_from_pg_type(name, field.datatype())
            })
            .collect();

        Ok(Some(TableSchema::new(
            cte.alias.name.value.clone(),
            columns,
        )))
    }

    async fn describe_query_fields_from_schema_map(
        &self,
        query: &sqlparser::ast::Query,
        schemas: &HashMap<String, TableSchema>,
    ) -> PgWireResult<Option<Vec<FieldInfo>>> {
        if let Some(with) = &query.with {
            let mut local_schemas = schemas.clone();
            for cte in &with.cte_tables {
                let Some(schema) = self.describe_cte_schema(cte, &local_schemas).await? else {
                    continue;
                };
                local_schemas.insert(cte.alias.name.value.to_ascii_lowercase(), schema);
            }
            let body_query = sqlparser::ast::Query {
                with: None,
                body: query.body.clone(),
                order_by: query.order_by.clone(),
                limit_clause: query.limit_clause.clone(),
                fetch: query.fetch.clone(),
                locks: query.locks.clone(),
                for_clause: query.for_clause.clone(),
                settings: query.settings.clone(),
                format_clause: query.format_clause.clone(),
                pipe_operators: query.pipe_operators.clone(),
            };
            return Box::pin(
                self.describe_query_fields_from_schema_map(&body_query, &local_schemas),
            )
            .await;
        }

        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            SetExpr::Query(query) => {
                return Box::pin(self.describe_query_fields_from_schema_map(query, schemas)).await;
            }
            SetExpr::SetOperation { left, .. } => {
                let query = sqlparser::ast::Query {
                    with: None,
                    body: left.clone(),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: Vec::new(),
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: Vec::new(),
                };
                return Box::pin(self.describe_query_fields_from_schema_map(&query, schemas)).await;
            }
            _ => return Ok(None),
        };

        let relation_schemas = self.describe_relation_schemas(select, schemas).await?;
        if relation_schemas.is_empty() {
            return Ok(Some(self.describe_projection_fallback(&select.projection)));
        }

        let fields =
            self.describe_projection_with_relation_schemas(&select.projection, &relation_schemas);
        Ok(Some(fields))
    }

    async fn describe_relation_schemas(
        &self,
        select: &sqlparser::ast::Select,
        cte_schemas: &HashMap<String, TableSchema>,
    ) -> PgWireResult<Vec<TableSchema>> {
        let mut schemas = Vec::new();
        for table in &select.from {
            if let Some(schema) = self
                .describe_table_factor_schema(&table.relation, cte_schemas)
                .await?
            {
                schemas.push(schema);
            }
            for join in &table.joins {
                if let Some(schema) = self
                    .describe_table_factor_schema(&join.relation, cte_schemas)
                    .await?
                {
                    schemas.push(schema);
                }
            }
        }
        Ok(schemas)
    }

    async fn describe_table_factor_schema(
        &self,
        relation: &sqlparser::ast::TableFactor,
        cte_schemas: &HashMap<String, TableSchema>,
    ) -> PgWireResult<Option<TableSchema>> {
        match relation {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                if let Some(schema) = cte_schemas.get(&table_name.to_ascii_lowercase()) {
                    return Ok(Some(Self::schema_with_table_alias(schema, alias.as_ref())));
                }
                self.load_schema_for_describe(&table_name).await
            }
            sqlparser::ast::TableFactor::Derived {
                subquery, alias, ..
            } => {
                let Some(fields) =
                    Box::pin(self.describe_query_fields_from_schema_map(subquery, cte_schemas))
                        .await?
                else {
                    return Ok(None);
                };
                let table_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| "derived".to_string());
                let columns = fields
                    .into_iter()
                    .enumerate()
                    .map(|(idx, field)| {
                        let name = alias
                            .as_ref()
                            .and_then(|alias| alias.columns.get(idx))
                            .map(|column| column.name.value.clone())
                            .unwrap_or_else(|| field.name().to_string());
                        Self::catalog_column_from_pg_type(name, field.datatype())
                    })
                    .collect();
                Ok(Some(TableSchema::new(table_name, columns)))
            }
            _ => Ok(None),
        }
    }

    async fn load_schema_for_describe(
        &self,
        table_name: &str,
    ) -> PgWireResult<Option<TableSchema>> {
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
            return Ok(None);
        };
        let schema = bincode::deserialize::<TableSchema>(&schema_bytes).map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                "Schema deserialization error: {}",
                e
            ))))
        })?;
        Ok(Some(schema))
    }

    fn schema_with_table_alias(
        schema: &TableSchema,
        alias: Option<&sqlparser::ast::TableAlias>,
    ) -> TableSchema {
        let mut schema = schema.clone();
        if let Some(alias) = alias {
            schema.name = alias.name.value.clone();
            for (idx, alias_column) in alias.columns.iter().enumerate() {
                if let Some(column) = schema.columns.get_mut(idx) {
                    column.name = alias_column.name.value.clone();
                    if let Some(data_type) = &alias_column.data_type {
                        column.data_type = Self::pg_type_name_for_sql_type(data_type);
                    }
                }
            }
        }
        schema
    }

    fn catalog_column_from_pg_type(name: String, data_type: &Type) -> Column {
        Column {
            name,
            data_type: Self::pg_type_name_for_type(data_type),
            is_primary: false,
            is_indexed: false,
            index_type: IndexType::None,
            default_value: None,
            is_nullable: true,
            is_unique: false,
            check_expr: None,
        }
    }

    fn describe_projection_with_relation_schemas(
        &self,
        projection: &[SelectItem],
        relation_schemas: &[TableSchema],
    ) -> Vec<FieldInfo> {
        let wildcard = projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        });
        if wildcard {
            return relation_schemas
                .iter()
                .flat_map(|schema| {
                    schema.columns.iter().map(|column| {
                        FieldInfo::new(
                            column.name.clone(),
                            None,
                            None,
                            Self::pg_type_for_column_type(&column.data_type),
                            FieldFormat::Text,
                        )
                    })
                })
                .collect();
        }

        projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::UnnamedExpr(expr) => Some(FieldInfo::new(
                    Self::projection_output_name(expr),
                    None,
                    None,
                    self.pg_type_for_projection_expr_in_relations(expr, relation_schemas),
                    FieldFormat::Text,
                )),
                SelectItem::ExprWithAlias { expr, alias } => Some(FieldInfo::new(
                    alias.value.clone(),
                    None,
                    None,
                    self.pg_type_for_projection_expr_in_relations(expr, relation_schemas),
                    FieldFormat::Text,
                )),
                _ => None,
            })
            .collect()
    }

    fn projection_output_name(expr: &Expr) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(idents) => idents
                .last()
                .map(|ident| ident.value.clone())
                .unwrap_or_else(|| expr.to_string()),
            _ => expr.to_string(),
        }
    }

    fn pg_type_for_projection_expr_in_relations(
        &self,
        expr: &Expr,
        relation_schemas: &[TableSchema],
    ) -> Type {
        match expr {
            Expr::Identifier(ident) => relation_schemas
                .iter()
                .find_map(|schema| {
                    schema
                        .columns
                        .iter()
                        .find(|column| column.name.eq_ignore_ascii_case(&ident.value))
                        .map(|column| Self::pg_type_for_column_type(&column.data_type))
                })
                .unwrap_or_else(|| Self::pg_type_for_literal_expr(expr).unwrap_or(Type::TEXT)),
            Expr::CompoundIdentifier(idents) => {
                let Some(column_name) = idents.last().map(|ident| ident.value.as_str()) else {
                    return Type::TEXT;
                };
                let qualifier = if idents.len() > 1 {
                    idents
                        .get(idents.len() - 2)
                        .map(|ident| ident.value.as_str())
                } else {
                    None
                };
                relation_schemas
                    .iter()
                    .filter(|schema| {
                        qualifier
                            .is_none_or(|qualifier| schema.name.eq_ignore_ascii_case(qualifier))
                    })
                    .find_map(|schema| {
                        schema
                            .columns
                            .iter()
                            .find(|column| column.name.eq_ignore_ascii_case(column_name))
                            .map(|column| Self::pg_type_for_column_type(&column.data_type))
                    })
                    .unwrap_or(Type::TEXT)
            }
            _ => {
                let merged_schema = Self::merged_relation_schema(relation_schemas);
                self.pg_type_for_projection_expr(expr, &merged_schema)
            }
        }
    }

    fn merged_relation_schema(relation_schemas: &[TableSchema]) -> TableSchema {
        let columns = relation_schemas
            .iter()
            .flat_map(|schema| schema.columns.clone())
            .collect();
        TableSchema::new("__describe".to_string(), columns)
    }

    fn pg_type_name_for_sql_type(data_type: &sqlparser::ast::DataType) -> String {
        Self::pg_type_name_for_type(&Self::pg_type_for_sql_type(data_type))
    }

    fn pg_type_name_for_type(data_type: &Type) -> String {
        match *data_type {
            Type::BOOL => "BOOLEAN",
            Type::BOOL_ARRAY => "BOOLEAN[]",
            Type::INT2 => "SMALLINT",
            Type::INT2_ARRAY => "SMALLINT[]",
            Type::INT4 => "INTEGER",
            Type::INT4_ARRAY => "INTEGER[]",
            Type::INT8 => "BIGINT",
            Type::INT8_ARRAY => "BIGINT[]",
            Type::FLOAT4 => "REAL",
            Type::FLOAT4_ARRAY => "REAL[]",
            Type::FLOAT8 => "DOUBLE PRECISION",
            Type::FLOAT8_ARRAY => "DOUBLE PRECISION[]",
            Type::NUMERIC => "NUMERIC",
            Type::NUMERIC_ARRAY => "NUMERIC[]",
            Type::DATE => "DATE",
            Type::DATE_ARRAY => "DATE[]",
            Type::TIMESTAMP => "TIMESTAMP",
            Type::TIMESTAMP_ARRAY => "TIMESTAMP[]",
            Type::TIMESTAMPTZ => "TIMESTAMPTZ",
            Type::TIMESTAMPTZ_ARRAY => "TIMESTAMPTZ[]",
            Type::TIME => "TIME",
            Type::TIME_ARRAY => "TIME[]",
            Type::INTERVAL => "INTERVAL",
            Type::INTERVAL_ARRAY => "INTERVAL[]",
            Type::BYTEA => "BYTEA",
            Type::BYTEA_ARRAY => "BYTEA[]",
            Type::JSON => "JSON",
            Type::JSON_ARRAY => "JSON[]",
            Type::JSONB => "JSONB",
            Type::JSONB_ARRAY => "JSONB[]",
            Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => "TEXT[]",
            _ => "TEXT",
        }
        .to_string()
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
            Expr::CompoundFieldAccess { root, access_chain } => {
                let mut current_type = self.pg_type_for_projection_expr(root, schema);
                for access in access_chain {
                    if matches!(access, sqlparser::ast::AccessExpr::Subscript(_))
                        && Self::is_pg_array_type(&current_type)
                    {
                        current_type = Self::pg_scalar_type_for_array_type(&current_type);
                    }
                }
                current_type
            }
            Expr::Cast { data_type, .. } => Self::pg_type_for_sql_type(data_type),
            Expr::Array(array) => array
                .elem
                .iter()
                .find_map(|expr| {
                    let elem_type = self.pg_type_for_projection_expr(expr, schema);
                    (elem_type != Type::TEXT
                        || matches!(expr, Expr::Value(_) | Expr::Cast { .. } | Expr::Array(_)))
                    .then_some(Self::pg_array_type_for_scalar_type(&elem_type))
                })
                .unwrap_or(Type::TEXT_ARRAY),
            Expr::BinaryOp { left, op, right } => match op {
                sqlparser::ast::BinaryOperator::Eq
                | sqlparser::ast::BinaryOperator::NotEq
                | sqlparser::ast::BinaryOperator::Gt
                | sqlparser::ast::BinaryOperator::GtEq
                | sqlparser::ast::BinaryOperator::Lt
                | sqlparser::ast::BinaryOperator::LtEq
                | sqlparser::ast::BinaryOperator::And
                | sqlparser::ast::BinaryOperator::Or => Type::BOOL,
                sqlparser::ast::BinaryOperator::StringConcat => {
                    let left_type = self.pg_type_for_projection_expr(left, schema);
                    let right_type = self.pg_type_for_projection_expr(right, schema);
                    if Self::is_pg_array_type(&left_type) {
                        left_type
                    } else if Self::is_pg_array_type(&right_type) {
                        right_type
                    } else {
                        Type::TEXT
                    }
                }
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
            Expr::Subquery(subquery) => self.pg_type_for_scalar_subquery(subquery, schema),
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                match name.as_str() {
                    "COUNT" | "ROW_NUMBER" | "RANK" | "DENSE_RANK" => Type::INT8,
                    "ARRAY_AGG" => {
                        if let FunctionArguments::List(args) = &func.args {
                            args.args
                                .iter()
                                .find_map(|arg| {
                                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                                        Some(Self::pg_array_type_for_scalar_type(
                                            &self.pg_type_for_projection_expr(expr, schema),
                                        ))
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(Type::TEXT_ARRAY)
                        } else {
                            Type::TEXT_ARRAY
                        }
                    }
                    "SUM" => {
                        match self
                            .pg_type_for_first_function_arg(func, schema)
                            .unwrap_or(Type::INT8)
                        {
                            Type::FLOAT4 | Type::FLOAT8 => Type::FLOAT8,
                            Type::NUMERIC => Type::NUMERIC,
                            _ => Type::INT8,
                        }
                    }
                    "AVG" => Type::FLOAT8,
                    "MIN" | "MAX" => self
                        .pg_type_for_first_function_arg(func, schema)
                        .unwrap_or(Type::FLOAT8),
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

    fn pg_type_for_first_function_arg(
        &self,
        func: &sqlparser::ast::Function,
        schema: &crate::catalog::TableSchema,
    ) -> Option<Type> {
        let FunctionArguments::List(args) = &func.args else {
            return None;
        };
        args.args.iter().find_map(|arg| {
            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                Some(self.pg_type_for_projection_expr(expr, schema))
            } else {
                None
            }
        })
    }

    fn pg_type_for_scalar_subquery(
        &self,
        query: &sqlparser::ast::Query,
        outer_schema: &crate::catalog::TableSchema,
    ) -> Type {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Type::TEXT;
        };
        let Some(item) = select.projection.first() else {
            return Type::TEXT;
        };
        let expr = match item {
            SelectItem::UnnamedExpr(expr) => expr,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return Type::TEXT,
        };
        self.pg_type_for_projection_expr(expr, outer_schema)
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

        match stmt {
            Statement::StartTransaction { .. } => {
                let mut session = self.session.lock().await;
                if session.transaction.is_none() {
                    session.transaction = Some(self.storage.begin_transaction().await?);
                    session.transaction_may_change_query_results = false;
                }
                return Ok(QueryResult::Success {
                    message: "BEGIN".to_string(),
                });
            }
            Statement::Commit { .. } => {
                let txn = {
                    let mut session = self.session.lock().await;
                    session.transaction_may_change_query_results = false;
                    session.transaction.take()
                };
                if let Some(txn) = txn {
                    txn.commit().await?;
                    self.executor.invalidate_query_result_cache();
                }
                return Ok(QueryResult::Success {
                    message: "COMMIT".to_string(),
                });
            }
            Statement::Rollback { .. } => {
                let txn = {
                    let mut session = self.session.lock().await;
                    session.transaction_may_change_query_results = false;
                    session.transaction.take()
                };
                if let Some(txn) = txn {
                    txn.rollback().await?;
                }
                return Ok(QueryResult::Success {
                    message: "ROLLBACK".to_string(),
                });
            }
            _ => {}
        }

        let mut session = self.session.lock().await;
        if session.transaction.is_some() {
            if Executor::statement_may_change_query_results(stmt) {
                session.transaction_may_change_query_results = true;
            }
            let txn = session.transaction.as_mut().expect("transaction checked");
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
                if Executor::statement_may_change_query_results(stmt) {
                    self.executor.invalidate_query_result_cache();
                }
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

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.handler.clone()
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for PgHandler {
    async fn on_query<C>(&self, client: &mut C, query: SimpleQuery) -> PgWireResult<()>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query_string = query.query.clone();
        match Self::parse_copy_from_stdin_query(&query_string) {
            Ok(Some(stmt)) => {
                if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
                    return Err(PgWireError::NotReadyForQuery);
                }

                let username = Self::username_for_client(client);
                if let Err(e) = self.executor.authorize_statement(&username, &stmt).await {
                    client
                        .send(PgWireBackendMessage::ErrorResponse(
                            Self::auth_error(format!("Authorization Error: {:?}", e)).into(),
                        ))
                        .await
                        .map_err(|_| Self::sink_error())?;
                    client.set_state(PgWireConnectionState::ReadyForQuery);
                    client
                        .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                            client.transaction_status().to_error_state(),
                        )))
                        .await
                        .map_err(|_| Self::sink_error())?;
                    return Ok(());
                }

                let response = Self::copy_in_response_for_statement(&stmt);
                {
                    let mut session = self.session.lock().await;
                    session.copy_in = Some(CopyInState {
                        statement: stmt,
                        query: query_string.clone(),
                        data: Vec::new(),
                        simple_query: true,
                    });
                }
                client.set_state(PgWireConnectionState::CopyInProgress(true));
                pg_copy::send_copy_in_response(client, response).await?;
                Ok(())
            }
            Ok(None) => self._on_query(client, query).await,
            Err(e) => {
                client
                    .send(PgWireBackendMessage::ErrorResponse(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Parse Error: {:?}", e),
                        )
                        .into(),
                    ))
                    .await
                    .map_err(|_| Self::sink_error())?;
                client.set_state(PgWireConnectionState::ReadyForQuery);
                client
                    .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                        client.transaction_status().to_error_state(),
                    )))
                    .await
                    .map_err(|_| Self::sink_error())?;
                Ok(())
            }
        }
    }

    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        eprintln!("PG Simple Query called: {}", query);

        if let Some(application_name) = Self::parse_set_application_name(query) {
            client
                .metadata_mut()
                .insert("application_name".to_string(), application_name);
            return Ok(vec![Response::Execution(Tag::new("SET"))]);
        }

        let username = Self::username_for_client(client);
        match Self::parse_copy_from_stdin_query(query) {
            Ok(Some(stmt)) => {
                if let Err(e) = self.executor.authorize_statement(&username, &stmt).await {
                    return Ok(vec![Response::Error(Box::new(Self::auth_error(format!(
                        "Authorization Error: {:?}",
                        e
                    ))))]);
                }
                return Ok(vec![
                    self.begin_copy_from_stdin(stmt, query.to_string()).await?,
                ]);
            }
            Ok(None) => {}
            Err(e) => {
                return Ok(vec![Response::Error(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_string(),
                        "42000".to_string(),
                        format!("Parse Error: {:?}", e),
                    ),
                ))])
            }
        }

        if let Err(e) = self.executor.authorize_sql(&username, query).await {
            return Ok(vec![Response::Error(Box::new(Self::auth_error(format!(
                "Authorization Error: {:?}",
                e
            ))))]);
        }

        match self.try_execute_pg_metadata_query(query, &[]).await {
            Ok(Some(QueryResult::Select { columns, rows })) => {
                let fields = Self::infer_text_fields(&columns, &rows);
                let mut data_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    data_rows.push(Self::encode_row(fields.clone(), row)?);
                }
                return Ok(vec![Response::Query(QueryResponse::new(
                    fields,
                    futures::stream::iter(data_rows.into_iter().map(Ok)),
                ))]);
            }
            Ok(Some(QueryResult::Success { message })) => {
                return Ok(vec![Response::Execution(Tag::new(&Self::pg_command_tag(
                    &message,
                )))]);
            }
            Ok(None) => {}
            Err(e) => {
                return Ok(vec![Response::Error(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_string(),
                        "XX000".to_string(),
                        format!("Metadata Error: {:?}", e),
                    ),
                ))]);
            }
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
                                session.transaction_may_change_query_results = false;
                                responses.push(Response::TransactionStart(Tag::new("BEGIN")));
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
                            Ok(_) => {
                                if session.transaction_may_change_query_results {
                                    self.executor.invalidate_query_result_cache();
                                }
                                session.transaction_may_change_query_results = false;
                                responses.push(Response::TransactionEnd(Tag::new("COMMIT")));
                            }
                            Err(e) => {
                                return Ok(vec![Response::Error(Box::new(Self::fusion_error(
                                    "Failed to commit transaction",
                                    &e,
                                )))]);
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
                            Ok(_) => {
                                session.transaction_may_change_query_results = false;
                                responses.push(Response::TransactionEnd(Tag::new("ROLLBACK")));
                            }
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

            if let Some(txn) = session.transaction.as_mut() {
                match self
                    .shard_route_conflict_message_for_statements_in_transaction(
                        std::slice::from_ref(&stmt),
                        &mut **txn,
                        &[],
                    )
                    .await
                {
                    Ok(Some(message)) => {
                        return Ok(vec![Response::Error(Box::new(Self::shard_route_error(
                            message,
                        )))]);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        return Ok(vec![Response::Error(Box::new(Self::fusion_error(
                            "Shard routing error",
                            &e,
                        )))]);
                    }
                }
                match self
                    .shard_read_route_conflict_message_for_statements_in_transaction(
                        std::slice::from_ref(&stmt),
                        &mut **txn,
                        &[],
                    )
                    .await
                {
                    Ok(Some(message)) => {
                        return Ok(vec![Response::Error(Box::new(Self::shard_route_error(
                            message,
                        )))]);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        return Ok(vec![Response::Error(Box::new(Self::fusion_error(
                            "Shard read routing error",
                            &e,
                        )))]);
                    }
                }
            } else {
                match self
                    .shard_write_route_action_for_statements(std::slice::from_ref(&stmt), &[])
                    .await
                {
                    Ok(PgShardWriteRouteAction::Local) => {}
                    Ok(PgShardWriteRouteAction::Forward(decision)) => {
                        let forwarded_query = stmt.to_string();
                        drop(session);
                        let mut forwarded_responses = self
                            .forward_simple_query_to_shard_owner(
                                &forwarded_query,
                                &username,
                                &decision,
                            )
                            .await?;
                        responses.append(&mut forwarded_responses);
                        return Ok(responses);
                    }
                    Ok(PgShardWriteRouteAction::Conflict(message)) => {
                        return Ok(vec![Response::Error(Box::new(Self::shard_route_error(
                            message,
                        )))]);
                    }
                    Err(e) => {
                        return Ok(vec![Response::Error(Box::new(Self::fusion_error(
                            "Shard routing error",
                            &e,
                        )))]);
                    }
                }
                match self
                    .shard_read_route_decision_for_statements(std::slice::from_ref(&stmt), &[])
                    .await
                {
                    Ok(Some(decision)) if !decision.is_local_owner() => {
                        let forwarded_query = stmt.to_string();
                        drop(session);
                        let mut forwarded_responses = self
                            .forward_simple_query_to_shard_owner(
                                &forwarded_query,
                                &username,
                                &decision,
                            )
                            .await?;
                        responses.append(&mut forwarded_responses);
                        return Ok(responses);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        return Ok(vec![Response::Error(Box::new(Self::fusion_error(
                            "Shard read routing error",
                            &e,
                        )))]);
                    }
                }
                let fanout_query = stmt.to_string();
                drop(session);
                if let Some(mut fanout_responses) = self
                    .fanout_count_select_to_shard_owners(&fanout_query, &username)
                    .await?
                {
                    responses.append(&mut fanout_responses);
                    return Ok(responses);
                }
                if let Some(mut fanout_responses) = self
                    .fanout_sum_select_to_shard_owners(&fanout_query, &username)
                    .await?
                {
                    responses.append(&mut fanout_responses);
                    return Ok(responses);
                }
                if let Some(mut fanout_responses) = self
                    .fanout_min_max_select_to_shard_owners(&fanout_query, &username)
                    .await?
                {
                    responses.append(&mut fanout_responses);
                    return Ok(responses);
                }
                if let Some(mut fanout_responses) = self
                    .fanout_simple_select_to_shard_owners(&fanout_query, &username)
                    .await?
                {
                    responses.append(&mut fanout_responses);
                    return Ok(responses);
                }
                session = self.session.lock().await;
            }
            // Execute Normal Statements
            let result = if session.transaction.is_some() {
                // Execute in current transaction
                if Executor::statement_may_change_query_results(&stmt) {
                    session.transaction_may_change_query_results = true;
                }
                let txn = session.transaction.as_mut().expect("transaction checked");
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
                        responses.push(Response::Execution(Tag::new(&Self::pg_command_tag(
                            &message,
                        ))));
                    }
                },
                Err(e) => {
                    return Ok(vec![Response::Error(Box::new(Self::fusion_error(
                        "Execution Error",
                        &e,
                    )))]);
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
        Self::trace_query(
            &format!(
                "parse start name={} oids={:?}",
                message.name.clone().unwrap_or_default(),
                message.type_oids
            ),
            &message.query,
        );
        let username = Self::username_for_client(client);
        let auth_result = match Self::parse_copy_from_stdin_query(&message.query) {
            Ok(Some(stmt)) => self.executor.authorize_statement(&username, &stmt).await,
            Ok(None) => self.executor.authorize_sql(&username, &message.query).await,
            Err(e) => Err(e),
        };
        if let Err(e) = auth_result {
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
        let parameter_types = if let Some(txn) = session.transaction.as_ref() {
            self.infer_parameter_types_from_query_in_transaction(
                &message.query,
                &message.type_oids,
                &**txn,
            )
            .await
        } else {
            self.infer_parameter_types_from_query(&message.query, &message.type_oids)
                .await
        };
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
        Self::trace("parse done");
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        Self::trace(format!(
            "bind start portal={} statement={} params={}",
            message.portal_name.clone().unwrap_or_default(),
            message.statement_name.clone().unwrap_or_default(),
            message.parameters.len()
        ));
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
                result_format_codes: message.result_column_format_codes.clone(),
            },
        );

        client
            .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await
            .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
        Self::trace("bind done");
        Ok(())
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        Self::trace(format!(
            "execute start portal={} max_rows={}",
            message.name.clone().unwrap_or_default(),
            message.max_rows
        ));
        let portal_name = message.name.clone().unwrap_or_default();
        let (query, params, result_format_codes) = {
            let session = self.session.lock().await;
            if let Some(portal) = session.portals.get(&portal_name) {
                (
                    portal.query.clone(),
                    portal.params.clone(),
                    portal.result_format_codes.clone(),
                )
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
        Self::trace_query(
            &format!(
                "execute query portal={} params={}",
                portal_name,
                params.len()
            ),
            &query,
        );

        let username = Self::username_for_client(client);
        if let Some(stmt) = Self::parse_copy_from_stdin_query(&query).map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                "Parse Error: {:?}",
                e
            ))))
        })? {
            if let Err(e) = self.executor.authorize_statement(&username, &stmt).await {
                client
                    .send(PgWireBackendMessage::ErrorResponse(
                        Self::auth_error(format!("Authorization Error: {:?}", e)).into(),
                    ))
                    .await
                    .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
                return Ok(());
            }

            let columns = Self::copy_column_count(&stmt);
            {
                let mut session = self.session.lock().await;
                session.copy_in = Some(CopyInState {
                    statement: stmt,
                    query: query.clone(),
                    data: Vec::new(),
                    simple_query: false,
                });
            }
            client.set_state(PgWireConnectionState::CopyInProgress(true));
            client
                .send(PgWireBackendMessage::CopyInResponse(
                    pgwire::messages::copy::CopyInResponse::new(
                        0,
                        columns as i16,
                        vec![0; columns],
                    ),
                ))
                .await
                .map_err(|_| Self::sink_error())?;
            return Ok(());
        }

        if let Err(e) = self.executor.authorize_sql(&username, &query).await {
            client
                .send(PgWireBackendMessage::ErrorResponse(
                    Self::auth_error(format!("Authorization Error: {:?}", e)).into(),
                ))
                .await
                .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
            return Ok(());
        }

        let jdbc_client = Self::is_postgresql_jdbc_client(client);
        let (result, is_metadata_result) = match self
            .try_execute_pg_metadata_query(&query, &params)
            .await
        {
            Ok(Some(result)) => (Ok(result), true),
            Ok(None) => {
                let in_transaction = {
                    let session = self.session.lock().await;
                    session.transaction.is_some()
                };
                if in_transaction {
                    match self
                        .shard_route_conflict_message_for_sql(&query, &params)
                        .await
                    {
                        Ok(Some(message)) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(
                                    Self::shard_route_error(message).into(),
                                ))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                        Ok(None) => {}
                        Err(e) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(
                                    Self::fusion_error("Shard routing error", &e).into(),
                                ))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                    }
                    let statements = match parse_sql(&query) {
                        Ok(statements) => statements,
                        Err(e) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(
                                    Self::fusion_error(
                                        "Shard read routing error",
                                        &FusionError::Execution(format!("Parse Error: {:?}", e)),
                                    )
                                    .into(),
                                ))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                    };
                    let mut session = self.session.lock().await;
                    let Some(txn) = session.transaction.as_mut() else {
                        drop(session);
                        return Ok(());
                    };
                    match self
                        .shard_read_route_conflict_message_for_statements_in_transaction(
                            &statements,
                            &mut **txn,
                            &params,
                        )
                        .await
                    {
                        Ok(Some(message)) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(
                                    Self::shard_route_error(message).into(),
                                ))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                        Ok(None) => {}
                        Err(e) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(
                                    Self::fusion_error("Shard read routing error", &e).into(),
                                ))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                    }
                } else {
                    match self.shard_write_route_action_for_sql(&query, &params).await {
                        Ok(PgShardWriteRouteAction::Local) => {}
                        Ok(PgShardWriteRouteAction::Forward(decision)) => {
                            match self
                                .forward_extended_query_to_shard_owner(
                                    &query, &params, &username, &decision,
                                )
                                .await?
                            {
                                Ok(results) => {
                                    self.send_forwarded_extended_query_results(
                                        client,
                                        &query,
                                        &params,
                                        &result_format_codes,
                                        jdbc_client,
                                        results,
                                    )
                                    .await?;
                                }
                                Err(error) => {
                                    client
                                        .send(PgWireBackendMessage::ErrorResponse(error.into()))
                                        .await
                                        .map_err(|_| Self::sink_error())?;
                                }
                            }
                            return Ok(());
                        }
                        Ok(PgShardWriteRouteAction::Conflict(message)) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(
                                    Self::shard_route_error(message).into(),
                                ))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                        Err(e) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(
                                    Self::fusion_error("Shard routing error", &e).into(),
                                ))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                    }
                    match self
                        .shard_read_route_decision_for_sql(&query, &params)
                        .await
                    {
                        Ok(Some(decision)) if !decision.is_local_owner() => {
                            match self
                                .forward_extended_query_to_shard_owner(
                                    &query, &params, &username, &decision,
                                )
                                .await?
                            {
                                Ok(results) => {
                                    self.send_forwarded_extended_query_results(
                                        client,
                                        &query,
                                        &params,
                                        &result_format_codes,
                                        jdbc_client,
                                        results,
                                    )
                                    .await?;
                                }
                                Err(error) => {
                                    client
                                        .send(PgWireBackendMessage::ErrorResponse(error.into()))
                                        .await
                                        .map_err(|_| Self::sink_error())?;
                                }
                            }
                            return Ok(());
                        }
                        Ok(_) => {}
                        Err(e) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(
                                    Self::fusion_error("Shard read routing error", &e).into(),
                                ))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                    }
                    match self
                        .fanout_extended_count_select_to_shard_owners(&query, &params, &username)
                        .await?
                    {
                        Ok(Some(results)) => {
                            self.send_forwarded_extended_query_results(
                                client,
                                &query,
                                &params,
                                &result_format_codes,
                                jdbc_client,
                                results,
                            )
                            .await?;
                            return Ok(());
                        }
                        Ok(None) => {}
                        Err(error) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(error.into()))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                    }
                    match self
                        .fanout_extended_sum_select_to_shard_owners(&query, &params, &username)
                        .await?
                    {
                        Ok(Some(results)) => {
                            self.send_forwarded_extended_query_results(
                                client,
                                &query,
                                &params,
                                &result_format_codes,
                                jdbc_client,
                                results,
                            )
                            .await?;
                            return Ok(());
                        }
                        Ok(None) => {}
                        Err(error) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(error.into()))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                    }
                    match self
                        .fanout_extended_min_max_select_to_shard_owners(&query, &params, &username)
                        .await?
                    {
                        Ok(Some(results)) => {
                            self.send_forwarded_extended_query_results(
                                client,
                                &query,
                                &params,
                                &result_format_codes,
                                jdbc_client,
                                results,
                            )
                            .await?;
                            return Ok(());
                        }
                        Ok(None) => {}
                        Err(error) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(error.into()))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                    }
                    match self
                        .fanout_extended_select_to_shard_owners(&query, &params, &username)
                        .await?
                    {
                        Ok(Some(results)) => {
                            self.send_forwarded_extended_query_results(
                                client,
                                &query,
                                &params,
                                &result_format_codes,
                                jdbc_client,
                                results,
                            )
                            .await?;
                            return Ok(());
                        }
                        Ok(None) => {}
                        Err(error) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse(error.into()))
                                .await
                                .map_err(|_| Self::sink_error())?;
                            return Ok(());
                        }
                    }
                }
                (self.execute_first_statement(&query, &params).await, false)
            }
            Err(e) => (Err(e), false),
        };

        match result {
            Ok(res) => match res {
                QueryResult::Select { columns, rows } => {
                    let effective_result_format_codes =
                        if !is_metadata_result && result_format_codes.is_empty() && !jdbc_client {
                            vec![1]
                        } else {
                            result_format_codes.clone()
                        };
                    let described_fields = self
                        .describe_query_fields(&query, &params, &effective_result_format_codes)
                        .await?;
                    let fields = if described_fields.is_empty() {
                        Self::fields_with_format(&columns, &rows, &effective_result_format_codes)
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
                                        field.format(),
                                    )
                                })
                                .collect::<Vec<_>>(),
                        )
                    };

                    for row in rows {
                        client
                            .send(PgWireBackendMessage::DataRow(Self::encode_row_for_fields(
                                fields.clone(),
                                row,
                            )?))
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
                            Self::pg_command_tag(&message),
                        )))
                        .await
                        .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
                }
            },
            Err(e) => {
                client
                    .send(PgWireBackendMessage::ErrorResponse(
                        Self::fusion_error("Execution Error", &e).into(),
                    ))
                    .await
                    .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
            }
        }

        Self::trace("execute done");
        Ok(())
    }

    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        Self::trace("sync start");
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
        Self::trace(format!("sync done status={:?}", transaction_status));
        Ok(())
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        Self::trace(format!(
            "describe message start target={} name={}",
            message.target_type as char,
            message.name.clone().unwrap_or_default()
        ));
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
                client
                    .send(PgWireBackendMessage::ParameterDescription(
                        ParameterDescription::new(
                            parameter_types.iter().map(Type::oid).collect::<Vec<_>>(),
                        ),
                    ))
                    .await
                    .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
                if Self::parse_copy_from_stdin_query(&query)
                    .map(|stmt| stmt.is_some())
                    .unwrap_or(false)
                {
                    client
                        .send(PgWireBackendMessage::NoData(NoData::new()))
                        .await
                        .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
                } else {
                    let fields = self.describe_query_fields(&query, &[], &[]).await?;
                    self.send_row_description_or_nodata(client, fields).await?;
                }
            }
            b'P' => {
                let (query, params, result_format_codes) = {
                    let session = self.session.lock().await;
                    let name = message.name.clone().unwrap_or_default();
                    session
                        .portals
                        .get(&name)
                        .map(|portal| {
                            (
                                portal.query.clone(),
                                portal.params.clone(),
                                portal.result_format_codes.clone(),
                            )
                        })
                        .unwrap_or_default()
                };
                if Self::parse_copy_from_stdin_query(&query)
                    .map(|stmt| stmt.is_some())
                    .unwrap_or(false)
                {
                    client
                        .send(PgWireBackendMessage::NoData(NoData::new()))
                        .await
                        .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
                } else {
                    let fields = self
                        .describe_query_fields(&query, &params, &result_format_codes)
                        .await?;
                    self.send_row_description_or_nodata(client, fields).await?;
                }
            }
            _ => {}
        }
        Self::trace("describe message done");
        Ok(())
    }

    async fn on_close<C>(&self, _client: &mut C, _message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        Ok(())
    }
}

#[async_trait::async_trait]
impl CopyHandler for PgHandler {
    async fn on_copy_data<C>(&self, _client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut session = self.session.lock().await;
        let Some(copy_in) = session.copy_in.as_mut() else {
            return Err(PgWireError::UserError(Box::new(Self::execution_error(
                "COPY data received without active COPY FROM STDIN",
            ))));
        };
        copy_in.data.extend_from_slice(copy_data.data.as_ref());
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        enum CopyDoneWork {
            Count {
                count: usize,
                simple_query: bool,
                in_transaction: bool,
            },
            ImplicitTransaction {
                statement: Statement,
                query: String,
                payload: Vec<u8>,
                simple_query: bool,
            },
        }

        let work = {
            let mut session = self.session.lock().await;
            let Some(copy_in) = session.copy_in.take() else {
                return Err(PgWireError::UserError(Box::new(Self::execution_error(
                    "COPY done received without active COPY FROM STDIN",
                ))));
            };

            Self::trace(format!(
                "copy done received: simple={} bytes={}",
                copy_in.simple_query,
                copy_in.data.len()
            ));
            if let Some(txn) = session.transaction.as_mut() {
                let count = self
                    .executor
                    .execute_copy_stdin_payload(&copy_in.statement, &copy_in.data, &mut **txn)
                    .await
                    .map_err(|e| PgWireError::UserError(Box::new(Self::copy_error(e))))?;
                session.transaction_may_change_query_results = true;
                CopyDoneWork::Count {
                    count,
                    simple_query: copy_in.simple_query,
                    in_transaction: true,
                }
            } else {
                CopyDoneWork::ImplicitTransaction {
                    statement: copy_in.statement,
                    query: copy_in.query,
                    payload: copy_in.data,
                    simple_query: copy_in.simple_query,
                }
            }
        };

        let (count, simple_query, transaction_status) = match work {
            CopyDoneWork::Count {
                count,
                simple_query,
                in_transaction,
            } => (
                count,
                simple_query,
                if in_transaction {
                    TransactionStatus::Transaction
                } else {
                    TransactionStatus::Idle
                },
            ),
            CopyDoneWork::ImplicitTransaction {
                statement,
                query,
                payload,
                simple_query,
            } => {
                let mut txn = self.storage.begin_transaction().await.map_err(|e| {
                    PgWireError::UserError(Box::new(Self::execution_error(format!(
                        "COPY failed to begin transaction: {:?}",
                        e
                    ))))
                })?;
                let route_action = match self
                    .executor
                    .copy_stdin_routing_decisions_for_payload(&statement, &payload, &mut *txn)
                    .await
                {
                    Ok(decisions) => Self::shard_write_route_action(decisions),
                    Err(e) => {
                        let _ = txn.rollback().await;
                        return Err(PgWireError::UserError(Box::new(Self::copy_error(e))));
                    }
                };
                match route_action {
                    PgShardWriteRouteAction::Local => {
                        match self
                            .executor
                            .execute_copy_stdin_payload(&statement, &payload, &mut *txn)
                            .await
                        {
                            Ok(count) => {
                                txn.commit().await.map_err(|e| {
                                    PgWireError::UserError(Box::new(Self::execution_error(
                                        format!("COPY failed to commit transaction: {:?}", e),
                                    )))
                                })?;
                                self.executor.invalidate_query_result_cache();
                                (count, simple_query, TransactionStatus::Idle)
                            }
                            Err(e) => {
                                let _ = txn.rollback().await;
                                return Err(PgWireError::UserError(Box::new(Self::copy_error(e))));
                            }
                        }
                    }
                    PgShardWriteRouteAction::Forward(decision) => {
                        let _ = txn.rollback().await;
                        let username = Self::username_for_client(client);
                        let count = match self
                            .forward_copy_to_shard_owner(&query, &payload, &username, &decision)
                            .await?
                        {
                            Ok(count) => count,
                            Err(error) => {
                                return Err(PgWireError::UserError(Box::new(error)));
                            }
                        };
                        (count, simple_query, TransactionStatus::Idle)
                    }
                    PgShardWriteRouteAction::Conflict(message) => {
                        let _ = txn.rollback().await;
                        return Err(PgWireError::UserError(Box::new(Self::shard_route_error(
                            message,
                        ))));
                    }
                }
            }
        };

        client
            .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                format!("COPY {}", count),
            )))
            .await
            .map_err(|_| Self::sink_error())?;

        if simple_query {
            client.set_transaction_status(transaction_status);
            client.set_state(PgWireConnectionState::ReadyForQuery);
            client
                .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    transaction_status,
                )))
                .await
                .map_err(|_| Self::sink_error())?;
        } else if matches!(client.state(), PgWireConnectionState::CopyInProgress(true)) {
            client.set_state(PgWireConnectionState::ReadyForQuery);
        }
        Self::trace(format!(
            "copy done completed: count={} simple={} status={:?}",
            count, simple_query, transaction_status
        ));
        Ok(())
    }

    async fn on_copy_fail<C>(&self, _client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut session = self.session.lock().await;
        session.copy_in = None;
        PgWireError::UserError(Box::new(Self::execution_error(format!(
            "COPY IN mode terminated by the user: {}",
            fail.message
        ))))
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
    start_pg_server_with_connection_limit(
        executor,
        storage,
        bind,
        port,
        password,
        _tls_acceptor,
        DEFAULT_MAX_CONNECTIONS,
    )
    .await
}

pub async fn start_pg_server_with_connection_limit(
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    bind: &str,
    port: u16,
    password: &str,
    _tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    max_connections: usize,
) {
    let addr = format!("{}:{}", bind, port);
    let listener = TcpListener::bind(&addr).await.unwrap();
    let limiter = PgConnectionLimiter::new(max_connections);
    monitor::set_pg_connection_limit(limiter.max_connections() as u64);
    println!(
        "FusionDB Postgres Server running on {} (max_connections={})",
        addr,
        limiter.max_connections()
    );

    let password = password.to_string();

    loop {
        let (stream, peer_addr) = listener.accept().await.unwrap();
        let Some(connection_slot) = limiter.try_acquire() else {
            eprintln!(
                "Postgres connection rejected from {:?}: max_connections={} reached",
                peer_addr,
                limiter.max_connections()
            );
            drop(stream);
            continue;
        };
        let executor = executor.clone();
        let storage = storage.clone();
        let password = password.clone();

        tokio::spawn(async move {
            let _connection_slot = connection_slot;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_connection_limiter_clamps_zero_to_one_slot() {
        let limiter = PgConnectionLimiter::new(0);

        assert_eq!(limiter.max_connections(), 1);
        assert_eq!(limiter.semaphore.available_permits(), 1);
    }

    #[test]
    fn pg_connection_limiter_releases_slot_on_drop() {
        let limiter = PgConnectionLimiter::new(1);
        let slot = limiter.try_acquire().expect("first connection allowed");

        assert_eq!(limiter.semaphore.available_permits(), 0);
        assert!(limiter.try_acquire().is_none());

        drop(slot);

        assert_eq!(limiter.semaphore.available_permits(), 1);
        assert!(limiter.try_acquire().is_some());
    }
}

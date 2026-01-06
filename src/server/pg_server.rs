use futures::{Sink, SinkExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex; // Required for client.send

use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::{ParameterDescription, RowDescription};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Close, Describe, Execute, Parse, ParseComplete, Sync as PgSync,
};
use pgwire::messages::response::CommandComplete;
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::PgWireFrontendMessage; // Helper struct

use sqlparser::ast::Statement;

use crate::common::{FusionError, Value}; // Import FusionError
use crate::execution::{Executor, QueryResult};
use crate::parser::parse_sql;
use crate::storage::{Storage, Transaction};

struct Session {
    transaction: Option<Box<dyn Transaction>>,
    statements: HashMap<String, String>,  // name -> query sql
    portals: HashMap<String, PortalData>, // name -> portal data
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
}

impl pgwire::api::PgWireServerHandlers for PgHandler {}

#[async_trait::async_trait]
impl StartupHandler for PgHandler {
    async fn on_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        eprintln!("PG Startup called");
        Ok(())
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for PgHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        eprintln!("PG Simple Query called: {}", query);

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
                        let fields = Arc::new(
                            columns
                                .iter()
                                .map(|name| {
                                    FieldInfo::new(
                                        name.clone(),
                                        None,
                                        None,
                                        Type::TEXT,
                                        pgwire::api::results::FieldFormat::Text,
                                    )
                                })
                                .collect::<Vec<_>>(),
                        );

                        let mut data_rows = Vec::new();
                        for row in rows {
                            let mut encoder = DataRowEncoder::new(fields.clone());
                            for val in row {
                                match val {
                                    Value::Integer(i) => encoder.encode_field(&i.to_string())?,
                                    Value::Float(f) => encoder.encode_field(&f.to_string())?,
                                    Value::String(s) => encoder.encode_field(&s)?,
                                    Value::Boolean(b) => encoder.encode_field(&b.to_string())?,
                                    Value::Null => encoder.encode_field(&None::<String>)?,
                                    _ => encoder.encode_field(&format!("{:?}", val))?,
                                }
                            }
                            data_rows.push(encoder.take_row());
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
        let mut session = self.session.lock().await;
        let name = message.name.clone().unwrap_or_default();
        session.statements.insert(name, message.query.clone());
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
            q.clone()
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

        let mut params = Vec::new();
        for param_bytes in message.parameters.iter() {
            if let Some(bytes) = param_bytes {
                let s = String::from_utf8_lossy(bytes).to_string();
                if let Ok(i) = s.parse::<i64>() {
                    params.push(Value::Integer(i));
                } else if let Ok(f) = s.parse::<f64>() {
                    params.push(Value::Float(f));
                } else {
                    params.push(Value::String(s));
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

        let statements = match parse_sql(&query) {
            Ok(stmts) => stmts,
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
                    .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
                return Ok(());
            }
        };

        if statements.is_empty() {
            client
                .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                    "EMPTY".to_string(),
                )))
                .await
                .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
            return Ok(());
        }

        let stmt = &statements[0];

        let mut session = self.session.lock().await;
        let result = if let Some(txn) = session.transaction.as_mut() {
            self.executor
                .execute_in_transaction_with_params(stmt, &mut **txn, &params)
                .await
        } else {
            match self.storage.begin_transaction().await {
                Ok(mut txn) => {
                    let res = self
                        .executor
                        .execute_in_transaction_with_params(stmt, &mut *txn, &params)
                        .await;
                    if res.is_ok() {
                        let _ = txn.commit().await;
                    } else {
                        let _ = txn.rollback().await;
                    }
                    res
                }
                Err(e) => Err(FusionError::Storage(format!(
                    "Failed to start implicit txn: {:?}",
                    e
                ))),
            }
        };

        match result {
            Ok(res) => match res {
                QueryResult::Select { columns, rows } => {
                    let fields = Arc::new(
                        columns
                            .iter()
                            .map(|name| {
                                FieldInfo::new(
                                    name.clone(),
                                    None,
                                    None,
                                    Type::TEXT,
                                    pgwire::api::results::FieldFormat::Text,
                                )
                            })
                            .collect::<Vec<_>>(),
                    );

                    for row in rows {
                        let mut encoder = DataRowEncoder::new(fields.clone());
                        for val in row {
                            match val {
                                Value::Integer(i) => encoder.encode_field(&i.to_string())?,
                                Value::Float(f) => encoder.encode_field(&f.to_string())?,
                                Value::String(s) => encoder.encode_field(&s)?,
                                Value::Boolean(b) => encoder.encode_field(&b.to_string())?,
                                Value::Null => encoder.encode_field(&None::<String>)?,
                                _ => encoder.encode_field(&format!("{:?}", val))?,
                            }
                        }
                        client
                            .send(PgWireBackendMessage::DataRow(encoder.take_row()))
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
            .map_err(|_| {
                PgWireError::IoError(std::io::Error::other("Sink Error"))
            })?;
        Ok(())
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + Unpin + Send + Sync + Sink<PgWireBackendMessage>,
    {
        let target_type = message.target_type;
        match target_type {
            b'S' => {
                client
                    .send(PgWireBackendMessage::ParameterDescription(
                        ParameterDescription::new(vec![]),
                    ))
                    .await
                    .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
                client
                    .send(PgWireBackendMessage::RowDescription(RowDescription::new(
                        vec![],
                    )))
                    .await
                    .map_err(|_| PgWireError::IoError(std::io::Error::other("Sink Error")))?;
            }
            b'P' => {
                client
                    .send(PgWireBackendMessage::RowDescription(RowDescription::new(
                        vec![],
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

pub async fn start_pg_server(executor: Arc<Executor>, storage: Arc<dyn Storage>, port: u16) {
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await.unwrap();
    println!("FusionDB Postgres Server running on {}", addr);

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let executor = executor.clone();
        let storage = storage.clone();

        tokio::spawn(async move {
            let handler = Arc::new(PgHandler::new(executor, storage));
            let _ = pgwire::tokio::process_socket(stream, None, handler).await;
        });
    }
}

use crate::common::Value;
use crate::execution::{Executor, QueryResult};
use crate::storage::fusion::FusionStorage;
use crate::storage::{Storage, Transaction};
use sqlparser::ast::Statement;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// Custom Binary Protocol for High-Performance Vector Search
// Format:
// [Magic: 2 bytes "WD"] [OpCode: 1 byte] [PayloadLen: 4 bytes LE] [Payload...]

const MAGIC: &[u8] = b"WD";
const OP_VECTOR_SEARCH: u8 = 1;
const OP_SQL_QUERY: u8 = 2;

pub async fn start_tcp_server(executor: Arc<Executor>, storage: Arc<dyn Storage>, port: u16) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr)
        .await
        .expect("Failed to bind TCP listener");
    println!("FusionDB TCP Server running on {}", addr);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let storage = storage.clone();
        let executor = executor.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, executor, storage).await {
                eprintln!("TCP Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(
    socket: TcpStream,
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
) -> std::io::Result<()> {
    // Keep connection open (Keep-Alive)
    let (reader, writer) = socket.into_split();
    let mut reader = tokio::io::BufReader::new(reader);
    let mut writer = tokio::io::BufWriter::new(writer);

    let mut header_buf = [0u8; 7]; // 2 Magic + 1 Op + 4 Len
    let mut active_txn: Option<Box<dyn Transaction>> = None;
    let mut active_txn_may_change_query_results = false;

    loop {
        // Read Header
        let n = reader.read(&mut header_buf).await?;
        if n == 0 {
            return Ok(());
        }
        if n < 7 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Incomplete header",
            ));
        }

        if &header_buf[0..2] != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid Magic",
            ));
        }

        let op = header_buf[2];
        let len = u32::from_le_bytes(header_buf[3..7].try_into().unwrap()) as usize;

        // Read Payload
        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload).await?;

        match op {
            OP_VECTOR_SEARCH => {
                handle_vector_search(&mut writer, &payload, &storage).await?;
            }
            OP_SQL_QUERY => {
                handle_sql_query(
                    &mut writer,
                    &payload,
                    &executor,
                    &storage,
                    &mut active_txn,
                    &mut active_txn_may_change_query_results,
                )
                .await?;
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Unknown OpCode",
                ));
            }
        }
        writer.flush().await?;
    }
}

async fn handle_sql_query<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    payload: &[u8],
    executor: &Arc<Executor>,
    storage: &Arc<dyn Storage>,
    active_txn: &mut Option<Box<dyn Transaction>>,
    active_txn_may_change_query_results: &mut bool,
) -> std::io::Result<()> {
    let sql = String::from_utf8(payload.to_vec()).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid UTF-8 SQL: {}", e),
        )
    })?;

    let stmts = executor
        .prepare(&sql)
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    if stmts.is_empty() {
        // Send Empty Success
        writer.write_all(&[0]).await?; // Status Success
        writer.write_all(&[0]).await?; // Type Message
        let msg = "No statement executed".as_bytes();
        writer.write_all(&(msg.len() as u32).to_le_bytes()).await?;
        writer.write_all(msg).await?;
        return Ok(());
    }

    // Execute first statement only for now
    let stmt = &stmts[0];

    let result = match stmt {
        Statement::StartTransaction { .. } => {
            if active_txn.is_some() {
                Err(std::io::Error::other("Nested transactions not supported"))
            } else {
                *active_txn = Some(
                    storage
                        .begin_transaction()
                        .await
                        .map_err(|e| std::io::Error::other(e.to_string()))?,
                );
                *active_txn_may_change_query_results = false;
                Ok(QueryResult::Success {
                    message: "Transaction started".to_string(),
                })
            }
        }
        Statement::Commit { .. } => {
            if let Some(txn) = active_txn.take() {
                txn.commit()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                if *active_txn_may_change_query_results {
                    executor.invalidate_query_result_cache();
                }
                *active_txn_may_change_query_results = false;
                Ok(QueryResult::Success {
                    message: "Transaction committed".to_string(),
                })
            } else {
                Ok(QueryResult::Success {
                    message: "No active transaction to commit".to_string(),
                })
            }
        }
        Statement::Rollback { .. } => {
            if let Some(txn) = active_txn.take() {
                txn.rollback()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                *active_txn_may_change_query_results = false;
                Ok(QueryResult::Success {
                    message: "Transaction rolled back".to_string(),
                })
            } else {
                Ok(QueryResult::Success {
                    message: "No active transaction to rollback".to_string(),
                })
            }
        }
        _ => {
            if let Some(txn) = active_txn.as_mut() {
                *active_txn_may_change_query_results |=
                    Executor::statement_may_change_query_results(stmt);
                executor
                    .execute_in_transaction(stmt, &mut **txn)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))
            } else {
                executor
                    .execute(stmt)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))
            }
        }
    };

    match result {
        Ok(res) => {
            // Status: Success (0)
            writer.write_all(&[0]).await?;

            match res {
                QueryResult::Success { message } => {
                    // Type: Message (0)
                    writer.write_all(&[0]).await?;
                    let bytes = message.as_bytes();
                    writer
                        .write_all(&(bytes.len() as u32).to_le_bytes())
                        .await?;
                    writer.write_all(bytes).await?;
                }
                QueryResult::Select { columns, rows } => {
                    // Type: Rows (1)
                    writer.write_all(&[1]).await?;

                    // Col Count
                    writer
                        .write_all(&(columns.len() as u32).to_le_bytes())
                        .await?;
                    // Col Names
                    for col in columns {
                        let bytes = col.as_bytes();
                        writer
                            .write_all(&(bytes.len() as u32).to_le_bytes())
                            .await?;
                        writer.write_all(bytes).await?;
                    }

                    // Row Count
                    writer.write_all(&(rows.len() as u32).to_le_bytes()).await?;
                    // Rows
                    for row in rows {
                        for val in row {
                            write_value(writer, &val).await?;
                        }
                    }
                }
            }
        }
        Err(e) => {
            // Status: Error (1)
            writer.write_all(&[1]).await?;
            let msg = e.to_string();
            let bytes = msg.as_bytes();
            writer
                .write_all(&(bytes.len() as u32).to_le_bytes())
                .await?;
            writer.write_all(bytes).await?;
        }
    }

    Ok(())
}

async fn write_value<W: AsyncWriteExt + Unpin>(writer: &mut W, val: &Value) -> std::io::Result<()> {
    match val {
        Value::Null => {
            writer.write_all(&[0]).await?;
        }
        Value::Integer(i) => {
            writer.write_all(&[1]).await?;
            writer.write_all(&i.to_le_bytes()).await?;
        }
        Value::Float(f) => {
            writer.write_all(&[2]).await?;
            writer.write_all(&f.to_le_bytes()).await?;
        }
        Value::String(s) => {
            writer.write_all(&[3]).await?;
            let bytes = s.as_bytes();
            writer
                .write_all(&(bytes.len() as u32).to_le_bytes())
                .await?;
            writer.write_all(bytes).await?;
        }
        Value::Boolean(b) => {
            writer.write_all(&[4]).await?;
            writer.write_all(&[if *b { 1 } else { 0 }]).await?;
        }
        Value::Vector(v) => {
            writer.write_all(&[5]).await?;
            writer.write_all(&(v.len() as u32).to_le_bytes()).await?;
            for f in v {
                writer.write_all(&f.to_le_bytes()).await?;
            }
        }
        _ => {
            // Fallback to Null
            writer.write_all(&[0]).await?;
        }
    }
    Ok(())
}

async fn handle_vector_search<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    payload: &[u8],
    storage: &Arc<dyn Storage>,
) -> std::io::Result<()> {
    // Payload: [Limit: 4 bytes] [Vector: 3 * 4 bytes (f32)] (assuming dim=3 for bench)
    if payload.len() < 16 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Payload too short",
        ));
    }

    let limit = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;

    // Read vector (3 dims)
    let v1 = f32::from_le_bytes(payload[4..8].try_into().unwrap());
    let v2 = f32::from_le_bytes(payload[8..12].try_into().unwrap());
    let v3 = f32::from_le_bytes(payload[12..16].try_into().unwrap());
    let query = vec![v1, v2, v3];

    // Execute
    // We assume storage is FusionStorage. If not, empty result.
    let results = if let Some(fusion) = storage.as_any().downcast_ref::<FusionStorage>() {
        fusion.vector_search(&query, limit).await
    } else {
        Vec::new()
    };

    // Response Format:
    // [Count: 4 bytes]
    // [ (Score: 4 bytes, IdLen: 4 bytes, Id: bytes...) ]

    let mut resp_buf = Vec::new();
    resp_buf.extend_from_slice(&(results.len() as u32).to_le_bytes());

    for (id, score) in results {
        resp_buf.extend_from_slice(&score.to_le_bytes());
        let id_bytes = id.as_bytes();
        resp_buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
        resp_buf.extend_from_slice(id_bytes);
    }

    writer.write_all(&resp_buf).await?;

    Ok(())
}

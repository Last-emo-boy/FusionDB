use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::storage::fusion::FusionStorage;
use crate::storage::Storage;
use crate::execution::{Executor, QueryResult};
use crate::common::Value;

// Custom Binary Protocol for High-Performance Vector Search
// Format:
// [Magic: 2 bytes "WD"] [OpCode: 1 byte] [PayloadLen: 4 bytes LE] [Payload...]

const MAGIC: &[u8] = b"WD";
const OP_VECTOR_SEARCH: u8 = 1;
const OP_SQL_QUERY: u8 = 2;

pub async fn start_tcp_server(executor: Arc<Executor>, storage: Arc<dyn Storage>, port: u16) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await.expect("Failed to bind TCP listener");
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

async fn handle_connection(mut socket: TcpStream, executor: Arc<Executor>, storage: Arc<dyn Storage>) -> std::io::Result<()> {
    // Keep connection open (Keep-Alive)
    let mut header_buf = [0u8; 7]; // 2 Magic + 1 Op + 4 Len
    
    loop {
        // Read Header
        let n = socket.read(&mut header_buf).await?;
        if n == 0 { return Ok(()); }
        if n < 7 { return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Incomplete header")); }

        if &header_buf[0..2] != MAGIC {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid Magic"));
        }
        
        let op = header_buf[2];
        let len = u32::from_le_bytes(header_buf[3..7].try_into().unwrap()) as usize;

        // Read Payload
        let mut payload = vec![0u8; len];
        socket.read_exact(&mut payload).await?;

        match op {
            OP_VECTOR_SEARCH => {
                handle_vector_search(&mut socket, &payload, &storage).await?;
            }
            OP_SQL_QUERY => {
                handle_sql_query(&mut socket, &payload, &executor).await?;
            }
            _ => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unknown OpCode"));
            }
        }
    }
}

async fn handle_sql_query(socket: &mut TcpStream, payload: &[u8], executor: &Arc<Executor>) -> std::io::Result<()> {
    let sql = String::from_utf8(payload.to_vec())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Invalid UTF-8 SQL: {}", e)))?;
        
    // Execute SQL
    // We need to parse first? Executor has prepare/execute methods.
    // Let's use `executor.prepare` then `executor.execute`.
    // Actually `Executor` doesn't have a direct `execute_sql` convenience method exposed publicly that does both?
    // Let's look at `executor.execute(&stmt)`.
    
    let stmts = executor.prepare(&sql).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    if stmts.is_empty() {
        // Send Empty Success
        socket.write_all(&[0]).await?; // Status Success
        socket.write_all(&[0]).await?; // Type Message
        let msg = "No statement executed".as_bytes();
        socket.write_all(&(msg.len() as u32).to_le_bytes()).await?;
        socket.write_all(msg).await?;
        return Ok(());
    }
    
    // Execute first statement only for now
    let stmt = &stmts[0];
    let result = executor.execute(stmt).await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
    
    match result {
        Ok(res) => {
            // Status: Success (0)
            socket.write_all(&[0]).await?;
            
            match res {
                QueryResult::Success { message } => {
                    // Type: Message (0)
                    socket.write_all(&[0]).await?;
                    let bytes = message.as_bytes();
                    socket.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
                    socket.write_all(bytes).await?;
                },
                QueryResult::Select { columns, rows } => {
                    // Type: Rows (1)
                    socket.write_all(&[1]).await?;
                    
                    // Col Count
                    socket.write_all(&(columns.len() as u32).to_le_bytes()).await?;
                    // Col Names
                    for col in columns {
                        let bytes = col.as_bytes();
                        socket.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
                        socket.write_all(bytes).await?;
                    }
                    
                    // Row Count
                    socket.write_all(&(rows.len() as u32).to_le_bytes()).await?;
                    // Rows
                    for row in rows {
                        for val in row {
                            write_value(socket, &val).await?;
                        }
                    }
                }
            }
        },
        Err(e) => {
            // Status: Error (1)
            socket.write_all(&[1]).await?;
            let msg = e.to_string();
            let bytes = msg.as_bytes();
            socket.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
            socket.write_all(bytes).await?;
        }
    }
    
    Ok(())
}

async fn write_value(socket: &mut TcpStream, val: &Value) -> std::io::Result<()> {
    // Simple Binary Encoding for Value
    // [Type: 1 byte] [Data...]
    // 0: Null
    // 1: Integer (8 bytes)
    // 2: Float (8 bytes)
    // 3: String (Len + Bytes)
    // 4: Boolean (1 byte)
    // 5: Vector (Len + Floats)
    
    match val {
        Value::Null => {
            socket.write_all(&[0]).await?;
        },
        Value::Integer(i) => {
            socket.write_all(&[1]).await?;
            socket.write_all(&i.to_le_bytes()).await?;
        },
        Value::Float(f) => {
            socket.write_all(&[2]).await?;
            socket.write_all(&f.to_le_bytes()).await?;
        },
        Value::String(s) => {
            socket.write_all(&[3]).await?;
            let bytes = s.as_bytes();
            socket.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
            socket.write_all(bytes).await?;
        },
        Value::Boolean(b) => {
            socket.write_all(&[4]).await?;
            socket.write_all(&[if *b { 1 } else { 0 }]).await?;
        },
        Value::Vector(v) => {
             socket.write_all(&[5]).await?;
             socket.write_all(&(v.len() as u32).to_le_bytes()).await?;
             for f in v {
                 socket.write_all(&f.to_le_bytes()).await?;
             }
        },
        _ => {
             // Fallback to Null or String representation
             socket.write_all(&[0]).await?;
        }
    }
    Ok(())
}

async fn handle_vector_search(socket: &mut TcpStream, payload: &[u8], storage: &Arc<dyn Storage>) -> std::io::Result<()> {
    // Payload: [Limit: 4 bytes] [Vector: 3 * 4 bytes (f32)] (assuming dim=3 for bench)
    if payload.len() < 16 { return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Payload too short")); }
    
    let limit = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    
    // Read vector (3 dims)
    let v1 = f32::from_le_bytes(payload[4..8].try_into().unwrap());
    let v2 = f32::from_le_bytes(payload[8..12].try_into().unwrap());
    let v3 = f32::from_le_bytes(payload[12..16].try_into().unwrap());
    let query = vec![v1, v2, v3];

    // Execute
    // We assume storage is FusionStorage. If not, empty result.
    let results = if let Some(fusion) = storage.as_any().downcast_ref::<FusionStorage>() {
        fusion.vector_search(&query, limit)
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
    
    socket.write_all(&resp_buf).await?;
    
    Ok(())
}

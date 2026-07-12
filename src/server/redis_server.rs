use crate::common::{FusionError, Result};
use crate::storage::Storage;
use std::sync::Arc;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

const KEY_PREFIX: &[u8] = b"redis:";
const MAX_BULK_LEN: usize = 16 * 1024 * 1024;
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_ARRAY_ITEMS: usize = 1024;
const MAX_RESP_HEADER_BYTES: usize = 1024;

#[derive(Debug, PartialEq, Eq)]
enum RedisCommand {
    Auth(Vec<u8>),
    Ping(Option<Vec<u8>>),
    Echo(Vec<u8>),
    Select(Vec<u8>),
    Info(Option<Vec<u8>>),
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        condition: SetCondition,
        return_old: bool,
    },
    Get(Vec<u8>),
    MGet(Vec<Vec<u8>>),
    MSet(Vec<(Vec<u8>, Vec<u8>)>),
    Exists(Vec<Vec<u8>>),
    Del(Vec<Vec<u8>>),
    Incr(Vec<u8>),
    Quit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetCondition {
    Always,
    IfExists,
    IfNotExists,
}

pub async fn start_redis_server(
    storage: Arc<dyn Storage>,
    bind: &str,
    port: u16,
    password: String,
    max_connections: usize,
) {
    let addr = format!("{}:{}", bind, port);
    let listener = TcpListener::bind(&addr)
        .await
        .expect("Failed to bind Redis-compatible listener");
    println!("FusionDB Redis-compatible Server running on {}", addr);
    let password: Arc<[u8]> = Arc::from(password.into_bytes());
    let connection_limit = Arc::new(Semaphore::new(max_connections.max(1)));

    loop {
        let (socket, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("Redis-compatible accept error: {}", e);
                continue;
            }
        };
        let storage = storage.clone();
        let password = password.clone();
        let permit = match connection_limit.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };

        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = handle_connection(socket, storage, &password).await {
                eprintln!("Redis-compatible connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(
    socket: TcpStream,
    storage: Arc<dyn Storage>,
    password: &[u8],
) -> std::io::Result<()> {
    let (reader, writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);
    let mut authenticated = false;

    loop {
        let frame = match read_resp_array(&mut reader).await {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(e) => {
                write_error(&mut writer, &e.to_string()).await?;
                writer.flush().await?;
                return Ok(());
            }
        };

        match parse_command(frame) {
            Ok(RedisCommand::Auth(candidate)) => {
                if constant_time_bytes_eq(&candidate, password) {
                    authenticated = true;
                    write_simple(&mut writer, "OK").await?;
                } else {
                    write_error(&mut writer, "WRONGPASS invalid username-password pair").await?;
                }
            }
            Ok(RedisCommand::Quit) => {
                write_simple(&mut writer, "OK").await?;
                writer.flush().await?;
                return Ok(());
            }
            Ok(_) if !authenticated => {
                write_error(&mut writer, "NOAUTH Authentication required").await?;
            }
            Ok(command) => {
                if let Err(e) = execute_command(&storage, &mut writer, command).await {
                    write_error(&mut writer, &e.to_string()).await?;
                }
            }
            Err(e) => write_error(&mut writer, &e.to_string()).await?,
        }
        writer.flush().await?;
    }
}

async fn execute_command<W>(
    storage: &Arc<dyn Storage>,
    writer: &mut W,
    command: RedisCommand,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match command {
        RedisCommand::Auth(_) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "ERR AUTH is handled at the connection boundary",
        )),
        RedisCommand::Ping(payload) => match payload {
            Some(bytes) => write_bulk(writer, Some(&bytes)).await,
            None => write_simple(writer, "PONG").await,
        },
        RedisCommand::Echo(payload) => write_bulk(writer, Some(&payload)).await,
        RedisCommand::Select(db) => {
            if db.as_slice() == b"0" {
                write_simple(writer, "OK").await
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "ERR SELECT supports database 0 only",
                ))
            }
        }
        RedisCommand::Info(section) => {
            let section = section
                .as_deref()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .unwrap_or("default");
            let info = format!(
                "# Server\r\nredis_version:7.0.0-fusiondb\r\nfusiondb_redis_endpoint:1\r\n\r\n# Clients\r\nconnected_clients:1\r\n\r\n# Keyspace\r\ndb0:keys=0,expires=0,avg_ttl=0\r\n\r\n# Requested\r\nsection:{}\r\n",
                section
            );
            write_bulk(writer, Some(info.as_bytes())).await
        }
        RedisCommand::Set {
            key,
            value,
            condition,
            return_old,
        } => {
            let mut txn = storage
                .begin_transaction()
                .await
                .map_err(storage_io_error)?;
            let storage_key = redis_key(&key);
            let previous = txn.get(&storage_key).await.map_err(storage_io_error)?;
            let exists = previous.is_some();
            let should_write = match condition {
                SetCondition::Always => true,
                SetCondition::IfExists => exists,
                SetCondition::IfNotExists => !exists,
            };
            if should_write {
                txn.put(&storage_key, &value)
                    .await
                    .map_err(storage_io_error)?;
            }
            txn.commit().await.map_err(storage_io_error)?;
            if return_old {
                write_bulk(writer, previous.as_deref()).await
            } else if should_write {
                write_simple(writer, "OK").await
            } else {
                write_bulk(writer, None).await
            }
        }
        RedisCommand::Get(key) => {
            let txn = storage
                .begin_transaction()
                .await
                .map_err(storage_io_error)?;
            let value = txn.get(&redis_key(&key)).await.map_err(storage_io_error)?;
            write_bulk(writer, value.as_deref()).await
        }
        RedisCommand::MGet(keys) => {
            let txn = storage
                .begin_transaction()
                .await
                .map_err(storage_io_error)?;
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values.push(txn.get(&redis_key(&key)).await.map_err(storage_io_error)?);
            }
            write_array_bulk(writer, &values).await
        }
        RedisCommand::MSet(pairs) => {
            let mut txn = storage
                .begin_transaction()
                .await
                .map_err(storage_io_error)?;
            for (key, value) in pairs {
                txn.put(&redis_key(&key), &value)
                    .await
                    .map_err(storage_io_error)?;
            }
            txn.commit().await.map_err(storage_io_error)?;
            write_simple(writer, "OK").await
        }
        RedisCommand::Exists(keys) => {
            let txn = storage
                .begin_transaction()
                .await
                .map_err(storage_io_error)?;
            let mut found = 0_i64;
            for key in keys {
                if txn
                    .get(&redis_key(&key))
                    .await
                    .map_err(storage_io_error)?
                    .is_some()
                {
                    found += 1;
                }
            }
            write_integer(writer, found).await
        }
        RedisCommand::Del(keys) => {
            let mut txn = storage
                .begin_transaction()
                .await
                .map_err(storage_io_error)?;
            let mut deleted = 0_i64;
            for key in keys {
                let storage_key = redis_key(&key);
                if txn
                    .get(&storage_key)
                    .await
                    .map_err(storage_io_error)?
                    .is_some()
                {
                    txn.delete(&storage_key).await.map_err(storage_io_error)?;
                    deleted += 1;
                }
            }
            txn.commit().await.map_err(storage_io_error)?;
            write_integer(writer, deleted).await
        }
        RedisCommand::Incr(key) => {
            let mut txn = storage
                .begin_transaction()
                .await
                .map_err(storage_io_error)?;
            let storage_key = redis_key(&key);
            let current = txn
                .get(&storage_key)
                .await
                .map_err(storage_io_error)?
                .unwrap_or_else(|| b"0".to_vec());
            let current_str = std::str::from_utf8(&current).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "ERR value is not an integer or out of range",
                )
            })?;
            let next = current_str.trim().parse::<i64>().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "ERR value is not an integer or out of range",
                )
            })? + 1;
            txn.put(&storage_key, next.to_string().as_bytes())
                .await
                .map_err(storage_io_error)?;
            txn.commit().await.map_err(storage_io_error)?;
            write_integer(writer, next).await
        }
        RedisCommand::Quit => write_simple(writer, "OK").await,
    }
}

fn storage_io_error(error: FusionError) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

fn constant_time_bytes_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let max_len = left.len().max(right.len());
    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}

fn redis_key(key: &[u8]) -> Vec<u8> {
    let mut namespaced = Vec::with_capacity(KEY_PREFIX.len() + key.len());
    namespaced.extend_from_slice(KEY_PREFIX);
    namespaced.extend_from_slice(key);
    namespaced
}

async fn read_resp_array<R>(reader: &mut R) -> Result<Option<Vec<Vec<u8>>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    let read = read_resp_header_line(reader, &mut line).await?;
    if read == 0 {
        return Ok(None);
    }
    strip_crlf(&mut line)?;
    if !line.starts_with(b"*") {
        return Err(FusionError::Parser(
            "expected RESP array for Redis command".to_string(),
        ));
    }
    let item_count = parse_len(&line[1..], "array length")?;
    if item_count <= 0 {
        return Err(FusionError::Parser("empty Redis command".to_string()));
    }
    let item_count = usize::try_from(item_count)
        .map_err(|_| FusionError::Parser("Redis array length overflows usize".to_string()))?;
    if item_count > MAX_ARRAY_ITEMS {
        return Err(FusionError::Parser(format!(
            "Redis command exceeds max array items {}",
            MAX_ARRAY_ITEMS
        )));
    }

    let mut items = Vec::with_capacity(item_count);
    let mut request_bytes = 0usize;
    for _ in 0..item_count {
        let mut header = Vec::new();
        let read = read_resp_header_line(reader, &mut header).await?;
        if read == 0 {
            return Err(FusionError::Parser(
                "unexpected EOF in RESP bulk".to_string(),
            ));
        }
        strip_crlf(&mut header)?;
        if !header.starts_with(b"$") {
            return Err(FusionError::Parser(
                "expected RESP bulk string in array".to_string(),
            ));
        }
        let len = parse_len(&header[1..], "bulk length")?;
        if len < 0 {
            return Err(FusionError::Parser(
                "null bulk string is not valid in Redis command".to_string(),
            ));
        }
        let len = len as usize;
        if len > MAX_BULK_LEN {
            return Err(FusionError::Parser(format!(
                "bulk string exceeds max length {}",
                MAX_BULK_LEN
            )));
        }
        request_bytes = request_bytes
            .checked_add(len)
            .ok_or_else(|| FusionError::Parser("Redis command byte length overflow".to_string()))?;
        if request_bytes > MAX_REQUEST_BYTES {
            return Err(FusionError::Parser(format!(
                "Redis command exceeds max request bytes {}",
                MAX_REQUEST_BYTES
            )));
        }
        let frame_len = len
            .checked_add(2)
            .ok_or_else(|| FusionError::Parser("Redis bulk frame length overflow".to_string()))?;
        let mut bulk = vec![0_u8; frame_len];
        reader.read_exact(&mut bulk).await?;
        if &bulk[len..] != b"\r\n" {
            return Err(FusionError::Parser(
                "bulk string missing CRLF terminator".to_string(),
            ));
        }
        bulk.truncate(len);
        items.push(bulk);
    }

    Ok(Some(items))
}

async fn read_resp_header_line<R>(reader: &mut R, line: &mut Vec<u8>) -> Result<usize>
where
    R: AsyncBufRead + Unpin,
{
    line.clear();
    let mut limited = reader.take((MAX_RESP_HEADER_BYTES + 1) as u64);
    let read = limited.read_until(b'\n', line).await?;
    if line.len() > MAX_RESP_HEADER_BYTES {
        return Err(FusionError::Parser(format!(
            "RESP header exceeds max length {}",
            MAX_RESP_HEADER_BYTES
        )));
    }
    Ok(read)
}

fn strip_crlf(line: &mut Vec<u8>) -> Result<()> {
    if line.ends_with(b"\r\n") {
        line.truncate(line.len() - 2);
        Ok(())
    } else if line.ends_with(b"\n") {
        line.truncate(line.len() - 1);
        Ok(())
    } else {
        Err(FusionError::Parser("RESP line missing LF".to_string()))
    }
}

fn parse_len(bytes: &[u8], label: &str) -> Result<i64> {
    let value = std::str::from_utf8(bytes)
        .map_err(|_| FusionError::Parser(format!("invalid UTF-8 {}", label)))?
        .parse::<i64>()
        .map_err(|_| FusionError::Parser(format!("invalid {}", label)))?;
    Ok(value)
}

fn parse_command(items: Vec<Vec<u8>>) -> Result<RedisCommand> {
    let command = std::str::from_utf8(&items[0])
        .map_err(|_| FusionError::Parser("Redis command is not UTF-8".to_string()))?
        .to_ascii_uppercase();
    match command.as_str() {
        "AUTH" => match items.len() {
            2 => Ok(RedisCommand::Auth(items[1].clone())),
            3 if items[1].eq_ignore_ascii_case(b"default") => {
                Ok(RedisCommand::Auth(items[2].clone()))
            }
            _ => Err(FusionError::Parser(
                "ERR wrong number of arguments for 'auth' command".to_string(),
            )),
        },
        "PING" => match items.len() {
            1 => Ok(RedisCommand::Ping(None)),
            2 => Ok(RedisCommand::Ping(Some(items[1].clone()))),
            _ => Err(FusionError::Parser(
                "ERR wrong number of arguments for 'ping' command".to_string(),
            )),
        },
        "ECHO" => {
            if items.len() != 2 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'echo' command".to_string(),
                ));
            }
            Ok(RedisCommand::Echo(items[1].clone()))
        }
        "SELECT" => {
            if items.len() != 2 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'select' command".to_string(),
                ));
            }
            Ok(RedisCommand::Select(items[1].clone()))
        }
        "INFO" => match items.len() {
            1 => Ok(RedisCommand::Info(None)),
            2 => Ok(RedisCommand::Info(Some(items[1].clone()))),
            _ => Err(FusionError::Parser(
                "ERR wrong number of arguments for 'info' command".to_string(),
            )),
        },
        "SET" => {
            if items.len() < 3 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'set' command".to_string(),
                ));
            }
            let set_options = parse_set_options(&items[3..])?;
            Ok(RedisCommand::Set {
                key: items[1].clone(),
                value: items[2].clone(),
                condition: set_options.condition,
                return_old: set_options.return_old,
            })
        }
        "SETEX" => {
            if items.len() != 4 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'setex' command".to_string(),
                ));
            }
            validate_positive_integer(&items[2], "expire time")?;
            Err(FusionError::Parser(
                "ERR SETEX is not supported because this endpoint does not implement TTL"
                    .to_string(),
            ))
        }
        "GET" => {
            if items.len() != 2 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'get' command".to_string(),
                ));
            }
            Ok(RedisCommand::Get(items[1].clone()))
        }
        "MGET" => {
            if items.len() < 2 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'mget' command".to_string(),
                ));
            }
            Ok(RedisCommand::MGet(items[1..].to_vec()))
        }
        "MSET" => {
            if items.len() < 3 || items.len() % 2 == 0 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'mset' command".to_string(),
                ));
            }
            let pairs = items[1..]
                .chunks_exact(2)
                .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
                .collect();
            Ok(RedisCommand::MSet(pairs))
        }
        "EXISTS" => {
            if items.len() < 2 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'exists' command".to_string(),
                ));
            }
            Ok(RedisCommand::Exists(items[1..].to_vec()))
        }
        "DEL" => {
            if items.len() < 2 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'del' command".to_string(),
                ));
            }
            Ok(RedisCommand::Del(items[1..].to_vec()))
        }
        "INCR" => {
            if items.len() != 2 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'incr' command".to_string(),
                ));
            }
            Ok(RedisCommand::Incr(items[1].clone()))
        }
        "QUIT" => {
            if items.len() != 1 {
                return Err(FusionError::Parser(
                    "ERR wrong number of arguments for 'quit' command".to_string(),
                ));
            }
            Ok(RedisCommand::Quit)
        }
        _ => Err(FusionError::Parser(format!(
            "ERR unknown command '{}'",
            command
        ))),
    }
}

struct SetOptions {
    condition: SetCondition,
    return_old: bool,
}

fn parse_set_options(options: &[Vec<u8>]) -> Result<SetOptions> {
    let mut parsed = SetOptions {
        condition: SetCondition::Always,
        return_old: false,
    };
    let mut index = 0;
    while index < options.len() {
        let option = uppercase_ascii(&options[index])?;
        match option.as_str() {
            "NX" => {
                parsed.condition = SetCondition::IfNotExists;
                index += 1;
            }
            "XX" => {
                parsed.condition = SetCondition::IfExists;
                index += 1;
            }
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if index + 1 >= options.len() {
                    return Err(FusionError::Parser(format!(
                        "ERR syntax error: {} requires an argument",
                        option
                    )));
                }
                validate_positive_integer(&options[index + 1], "expire time")?;
                return Err(FusionError::Parser(format!(
                    "ERR SET {} is not supported because this endpoint does not implement TTL",
                    option
                )));
            }
            "KEEPTTL" => {
                return Err(FusionError::Parser(
                    "ERR SET KEEPTTL is not supported because this endpoint does not implement TTL"
                        .to_string(),
                ));
            }
            "GET" => {
                parsed.return_old = true;
                index += 1;
            }
            _ => {
                return Err(FusionError::Parser(format!(
                    "ERR syntax error: unsupported SET option '{}'",
                    option
                )));
            }
        }
    }
    Ok(parsed)
}

fn validate_positive_integer(bytes: &[u8], label: &str) -> Result<()> {
    let value = std::str::from_utf8(bytes)
        .map_err(|_| FusionError::Parser(format!("invalid {}", label)))?
        .parse::<i64>()
        .map_err(|_| FusionError::Parser(format!("invalid {}", label)))?;
    if value <= 0 {
        return Err(FusionError::Parser(format!("invalid {}", label)));
    }
    Ok(())
}

fn uppercase_ascii(bytes: &[u8]) -> Result<String> {
    Ok(std::str::from_utf8(bytes)
        .map_err(|_| FusionError::Parser("Redis command option is not UTF-8".to_string()))?
        .to_ascii_uppercase())
}

async fn write_simple<W>(writer: &mut W, value: &str) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(b"+").await?;
    writer.write_all(value.as_bytes()).await?;
    writer.write_all(b"\r\n").await
}

async fn write_error<W>(writer: &mut W, value: &str) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(b"-").await?;
    writer.write_all(value.as_bytes()).await?;
    writer.write_all(b"\r\n").await
}

async fn write_integer<W>(writer: &mut W, value: i64) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(format!(":{}\r\n", value).as_bytes()).await
}

async fn write_bulk<W>(writer: &mut W, value: Option<&[u8]>) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match value {
        Some(bytes) => {
            writer
                .write_all(format!("${}\r\n", bytes.len()).as_bytes())
                .await?;
            writer.write_all(bytes).await?;
            writer.write_all(b"\r\n").await
        }
        None => writer.write_all(b"$-1\r\n").await,
    }
}

async fn write_array_bulk<W>(writer: &mut W, values: &[Option<Vec<u8>>]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer
        .write_all(format!("*{}\r\n", values.len()).as_bytes())
        .await?;
    for value in values {
        write_bulk(writer, value.as_deref()).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryStorage;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn resp_parser_reads_array_bulk_command() {
        let (mut client, server) = tokio::io::duplex(128);
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .unwrap();
        drop(client);

        let mut reader = BufReader::new(server);
        let frame = read_resp_array(&mut reader).await.unwrap().unwrap();
        assert_eq!(
            parse_command(frame).unwrap(),
            RedisCommand::Set {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
                condition: SetCondition::Always,
                return_old: false,
            }
        );
    }

    #[tokio::test]
    async fn resp_parser_reads_pipelined_frames() {
        let (mut client, server) = tokio::io::duplex(256);
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
            .await
            .unwrap();
        drop(client);

        let mut reader = BufReader::new(server);
        let first = read_resp_array(&mut reader).await.unwrap().unwrap();
        let second = read_resp_array(&mut reader).await.unwrap().unwrap();

        assert_eq!(parse_command(first).unwrap(), RedisCommand::Ping(None));
        assert_eq!(
            parse_command(second).unwrap(),
            RedisCommand::Echo(b"hello".to_vec())
        );
    }

    #[test]
    fn redis_password_comparison_covers_length_and_content() {
        assert!(constant_time_bytes_eq(b"secret", b"secret"));
        assert!(!constant_time_bytes_eq(b"secret", b"secrex"));
        assert!(!constant_time_bytes_eq(b"secret", b"secret-longer"));
    }

    #[tokio::test]
    async fn redis_connection_requires_successful_authentication() {
        let wal_path = format!("test_redis_auth_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_storage = storage.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, server_storage, b"correct-password")
                .await
                .unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(
                b"*1\r\n$4\r\nPING\r\n\
                  *2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n\
                  *2\r\n$4\r\nAUTH\r\n$16\r\ncorrect-password\r\n\
                  *1\r\n$4\r\nPING\r\n\
                  *1\r\n$4\r\nQUIT\r\n",
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        server.await.unwrap();

        assert_eq!(
            String::from_utf8(response).unwrap(),
            "-NOAUTH Authentication required\r\n\
             -WRONGPASS invalid username-password pair\r\n\
             +OK\r\n+PONG\r\n+OK\r\n"
        );
        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn resp_parser_rejects_oversized_command_arrays_before_allocation() {
        let input = format!("*{}\r\n", MAX_ARRAY_ITEMS + 1);
        let mut reader = BufReader::new(input.as_bytes());
        let error = read_resp_array(&mut reader).await.unwrap_err();
        assert!(error.to_string().contains("max array items"));
    }

    #[tokio::test]
    async fn resp_parser_caps_headers_before_a_newline_arrives() {
        let input = vec![b'*'; MAX_RESP_HEADER_BYTES + 1];
        let mut reader = BufReader::new(input.as_slice());
        let error = read_resp_array(&mut reader).await.unwrap_err();
        assert!(error.to_string().contains("RESP header exceeds max length"));
    }

    #[tokio::test]
    async fn redis_connection_closes_after_resp_framing_error() {
        let wal_path = format!("test_redis_framing_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_storage = storage.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, server_storage, b"correct-password")
                .await
                .unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        let mut request = vec![b'*'; MAX_RESP_HEADER_BYTES];
        request.extend_from_slice(
            b"\n*2\r\n$4\r\nAUTH\r\n$16\r\ncorrect-password\r\n\
              *1\r\n$4\r\nPING\r\n\
              *1\r\n$4\r\nQUIT\r\n",
        );
        client.write_all(&request).await.unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        server.await.unwrap();

        let response = String::from_utf8(response).unwrap();
        assert!(response.contains("RESP header exceeds max length"));
        assert_eq!(response.lines().count(), 1);
        assert!(!response.contains("+OK"));
        assert!(!response.contains("+PONG"));

        drop(storage);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn ttl_commands_fail_closed_instead_of_persisting_forever() {
        let setex = vec![
            b"SETEX".to_vec(),
            b"session".to_vec(),
            b"60".to_vec(),
            b"value".to_vec(),
        ];
        let error = parse_command(setex).unwrap_err();
        assert!(error.to_string().contains("does not implement TTL"));

        for options in [
            vec![b"EX".to_vec(), b"60".to_vec()],
            vec![b"PX".to_vec(), b"1000".to_vec()],
            vec![b"KEEPTTL".to_vec()],
        ] {
            let mut command = vec![b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()];
            command.extend(options);
            let error = parse_command(command).unwrap_err();
            assert!(error.to_string().contains("does not implement TTL"));
        }
    }

    #[tokio::test]
    async fn redis_commands_round_trip_through_storage() {
        let storage: Arc<dyn Storage> = Arc::new(
            MemoryStorage::new(&format!("test_redis_{}.wal", uuid::Uuid::new_v4())).unwrap(),
        );
        let mut out = Vec::new();

        execute_command(
            &storage,
            &mut out,
            RedisCommand::Set {
                key: b"counter".to_vec(),
                value: b"41".to_vec(),
                condition: SetCondition::Always,
                return_old: false,
            },
        )
        .await
        .unwrap();
        execute_command(&storage, &mut out, RedisCommand::Get(b"counter".to_vec()))
            .await
            .unwrap();
        execute_command(&storage, &mut out, RedisCommand::Incr(b"counter".to_vec()))
            .await
            .unwrap();
        execute_command(
            &storage,
            &mut out,
            RedisCommand::Del(vec![b"counter".to_vec()]),
        )
        .await
        .unwrap();
        execute_command(&storage, &mut out, RedisCommand::Get(b"counter".to_vec()))
            .await
            .unwrap();

        assert_eq!(
            String::from_utf8(out).unwrap(),
            "+OK\r\n$2\r\n41\r\n:42\r\n:1\r\n$-1\r\n"
        );
    }

    #[tokio::test]
    async fn redis_extended_memtier_compat_commands() {
        let storage: Arc<dyn Storage> = Arc::new(
            MemoryStorage::new(&format!("test_redis_{}.wal", uuid::Uuid::new_v4())).unwrap(),
        );
        let mut out = Vec::new();

        execute_command(
            &storage,
            &mut out,
            RedisCommand::Set {
                key: b"existing".to_vec(),
                value: b"old".to_vec(),
                condition: SetCondition::Always,
                return_old: false,
            },
        )
        .await
        .unwrap();
        execute_command(
            &storage,
            &mut out,
            RedisCommand::Set {
                key: b"existing".to_vec(),
                value: b"new".to_vec(),
                condition: SetCondition::Always,
                return_old: true,
            },
        )
        .await
        .unwrap();
        execute_command(
            &storage,
            &mut out,
            RedisCommand::Set {
                key: b"existing".to_vec(),
                value: b"nx-skip".to_vec(),
                condition: SetCondition::IfNotExists,
                return_old: false,
            },
        )
        .await
        .unwrap();
        execute_command(
            &storage,
            &mut out,
            RedisCommand::Set {
                key: b"missing".to_vec(),
                value: b"xx-skip".to_vec(),
                condition: SetCondition::IfExists,
                return_old: false,
            },
        )
        .await
        .unwrap();
        execute_command(
            &storage,
            &mut out,
            RedisCommand::MSet(vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ]),
        )
        .await
        .unwrap();
        execute_command(
            &storage,
            &mut out,
            RedisCommand::MGet(vec![
                b"existing".to_vec(),
                b"missing".to_vec(),
                b"a".to_vec(),
            ]),
        )
        .await
        .unwrap();
        execute_command(
            &storage,
            &mut out,
            RedisCommand::Exists(vec![
                b"existing".to_vec(),
                b"missing".to_vec(),
                b"b".to_vec(),
            ]),
        )
        .await
        .unwrap();
        execute_command(&storage, &mut out, RedisCommand::Info(None))
            .await
            .unwrap();

        let output = String::from_utf8(out).unwrap();
        assert!(output.starts_with("+OK\r\n$3\r\nold\r\n$-1\r\n$-1\r\n+OK\r\n"));
        assert!(output.contains("*3\r\n$3\r\nnew\r\n$-1\r\n$1\r\n1\r\n"));
        assert!(output.contains(":2\r\n"));
        assert!(output.contains("# Server\r\nredis_version:7.0.0-fusiondb"));
    }
}

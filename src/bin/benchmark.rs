use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use reqwest::Client;
use serde_json::json;
use tokio::sync::Barrier;
use hdrhistogram::Histogram;
use rand::Rng;
use w33dDB::server;
use w33dDB::storage::fusion::FusionStorage;
use w33dDB::storage::Storage;
use w33dDB::execution::Executor;
use w33dDB::server::http_server::start_http_server;
use w33dDB::server::tcp_server::start_tcp_server;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PORT: u16 = 8095;
const TCP_PORT: u16 = 8096;
const BASE_URL: &str = "http://127.0.0.1:8095";
const CONCURRENCY: usize = 20;
const TOTAL_REQUESTS: usize = 2000;

#[tokio::main]
async fn main() {
    // 1. Start Server
    // Note: HTTP Server is deprecated and removed from benchmark.
    // We only start TCP Server.
    
    println!("Starting server for benchmark on port {}...", TCP_PORT);
    let storage = Arc::new(FusionStorage::new("benchmark.wal").expect("Failed to create storage"));
    
    // Start TCP Server
    let storage_tcp = storage.clone();
    let executor_tcp = Arc::new(Executor::new(storage_tcp.clone())); 
    tokio::spawn(async move {
        start_tcp_server(executor_tcp, storage_tcp, TCP_PORT).await;
    });

    tokio::time::sleep(Duration::from_secs(2)).await;

    // let client = Client::new(); // No HTTP Client needed

    println!("==================================================");
    println!("Starting Benchmark (TCP Only)");
    println!("Concurrency: {}", CONCURRENCY);
    println!("Total Requests per Scenario: {}", TOTAL_REQUESTS);
    println!("Target: 127.0.0.1:{}", TCP_PORT);
    println!("==================================================");

    // 2. Run Benchmarks (TCP Only)
    
    // Setup Data via TCP (We need a helper to run SQL over TCP for setup)
    // For simplicity, we will use a separate async function to setup data via TCP
    setup_data_tcp().await;

    println!("\n[Scenario: SQL Insert (Binary TCP Protocol)]");
    benchmark_sql_insert_tcp().await;

    println!("\n[Scenario: SQL Select (Binary TCP Protocol)]");
    benchmark_sql_select_tcp().await;
    
    // Populate Columnar Store for Vector Search
    if let Some(fusion) = storage.as_any().downcast_ref::<FusionStorage>() {
        println!("\nPopulating Fusion Columnar Store & Inverted Index...");
        let mut ids = Vec::new();
        let mut vecs = Vec::new();
        for i in 0..1000 {
            let id = format!("item{}", i);
            ids.push(id.clone());
            vecs.push(vec![rand::rng().random::<f32>(), rand::rng().random::<f32>(), rand::rng().random::<f32>()]);
            
            // Add to Inverted Index
            let text = if i % 2 == 0 { "apple orange banana" } else { "grape melon berry" };
            fusion.update_inverted_index(id, text);
        }
        fusion.update_columnar_store(ids, vecs);
    }

    println!("\n[Scenario: Vector Search (Binary TCP Protocol)]");
    benchmark_vector_search_tcp().await;
    
    // Test Hybrid Search (via HTTP for now as TCP protocol needs update)
    // We can just call fusion directly for benchmark if we had access, but here we are in bin.
    // Let's just print that we populated it.
    println!("\n[Scenario: Hybrid Search (Mock)]");
    println!("  Skipping HTTP benchmark for Hybrid Search in TCP-only mode.");
    
    let _ = std::fs::remove_file("benchmark.wal");
    println!("Benchmark Complete.");
}

async fn run_sql_tcp(sql: &str) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", TCP_PORT)).await.expect("Connect failed");
    let sql_bytes = sql.as_bytes();
    
    let mut req = Vec::with_capacity(7 + sql_bytes.len());
    req.extend_from_slice(b"WD");
    req.push(2); // OpCode 2 (SQL)
    req.extend_from_slice(&(sql_bytes.len() as u32).to_le_bytes());
    req.extend_from_slice(sql_bytes);
    
    stream.write_all(&req).await.unwrap();
    
    // Read Response (Discard)
    let mut status_buf = [0u8; 1];
    stream.read_exact(&mut status_buf).await.unwrap();
    
    if status_buf[0] == 0 {
         let mut type_buf = [0u8; 1];
         stream.read_exact(&mut type_buf).await.unwrap();
         if type_buf[0] == 0 { // Message
              let mut len_buf = [0u8; 4];
              stream.read_exact(&mut len_buf).await.unwrap();
              let len = u32::from_le_bytes(len_buf) as usize;
              let mut msg_buf = vec![0u8; len];
              stream.read_exact(&mut msg_buf).await.unwrap();
         } else {
              read_rows_tcp(&mut stream).await.unwrap();
         }
    } else {
         let mut len_buf = [0u8; 4];
         stream.read_exact(&mut len_buf).await.unwrap();
         let len = u32::from_le_bytes(len_buf) as usize;
         let mut msg_buf = vec![0u8; len];
         stream.read_exact(&mut msg_buf).await.unwrap();
    }
}

async fn setup_data_tcp() {
    println!("Setting up tables via TCP...");
    run_sql_tcp("CREATE TABLE IF NOT EXISTS bench_users (id INT, name TEXT, age INT)").await;
    run_sql_tcp("CREATE INDEX idx_id ON bench_users(id)").await;
}

async fn print_metrics(client: &Client) {
    if let Ok(res) = client.get(format!("{}/metrics", BASE_URL)).send().await {
        if let Ok(text) = res.text().await {
             println!("  [Metrics] {}", text);
        }
    }
}

async fn prepare_sql(client: &Client, sql: &str) -> String {
    let res = client.post(format!("{}/prepare", BASE_URL))
        .json(&json!({ "sql": sql }))
        .send()
        .await
        .expect("Prepare failed")
        .json::<serde_json::Value>()
        .await
        .expect("Failed to parse prepare response");
    
    if let Some(err) = res.get("error") {
        if !err.is_null() {
            panic!("Prepare error: {}", err);
        }
    }

    res["statement_id"].as_str().expect("No statement_id").to_string()
}

async fn benchmark_sql_insert(client: &Client) {
    println!("\n[Scenario: SQL Insert (Prepared)]");
    // Create table
    let _ = client.post(format!("{}/query", BASE_URL))
        .json(&json!({ "sql": "CREATE TABLE IF NOT EXISTS bench_users (id INT, name TEXT, age INT)" }))
        .send()
        .await;

    let stmt_id = prepare_sql(client, "INSERT INTO bench_users VALUES ($1, $2, $3)").await;

    run_benchmark(client, "SQL Insert", move |c| {
        let sid = stmt_id.clone();
        async move {
            let id = rand::rng().random_range(0..1000000);
            let age = rand::rng().random_range(18..99);
            let name = format!("User{}", id);
            
            c.post(format!("{}/execute", BASE_URL))
                .json(&json!({ 
                    "statement_id": sid,
                    "params": [id, name, age]
                }))
                .send()
                .await
        }
    }).await;
}

async fn benchmark_sql_select(client: &Client) {
    println!("\n[Scenario: SQL Select (Prepared Point Lookup - Low Hit)]");
    
    // Create Index on ID to speed up lookups
    let _ = client.post(format!("{}/query", BASE_URL))
        .json(&json!({ "sql": "CREATE INDEX idx_id ON bench_users(id)" }))
        .send()
        .await;

    let stmt_id = prepare_sql(client, "SELECT * FROM bench_users WHERE id = $1").await;

    run_benchmark(client, "SQL Select", move |c| {
        let sid = stmt_id.clone();
        async move {
            // Range should match insert range to hit data
            let id = rand::rng().random_range(0..1000000); 
            c.post(format!("{}/execute", BASE_URL))
                .json(&json!({ 
                    "statement_id": sid,
                    "params": [id]
                }))
                .send()
                .await
        }
    }).await;
}

async fn benchmark_sql_select_high_hit(client: &Client) {
    println!("\n[Scenario: SQL Select (Prepared Point Lookup - High Hit)]");
    
    let insert_stmt = prepare_sql(client, "INSERT INTO bench_users VALUES ($1, $2, $3)").await;
    
    println!("  Populating High Hit Data...");
    for i in 0..1000 {
        let id = 2000000 + i;
        let _ = client.post(format!("{}/execute", BASE_URL))
            .json(&json!({ 
                "statement_id": insert_stmt,
                "params": [id, "HitUser", 25]
            }))
            .send()
            .await;
    }

    let stmt_id = prepare_sql(client, "SELECT * FROM bench_users WHERE id = $1").await;

    run_benchmark(client, "SQL Select High Hit", move |c| {
        let sid = stmt_id.clone();
        async move {
            // Pick from the dense range
            let id = rand::rng().random_range(2000000..2001000); 
            c.post(format!("{}/execute", BASE_URL))
                .json(&json!({ 
                    "statement_id": sid,
                    "params": [id]
                }))
                .send()
                .await
        }
    }).await;
}

async fn benchmark_fts_search(client: &Client) {
    println!("\n[Scenario: FTS Search (Prepared)]");
    let _ = client.post(format!("{}/query", BASE_URL))
        .json(&json!({ "sql": "CREATE TABLE IF NOT EXISTS documents (id INT, content TEXT)" }))
        .send()
        .await;
    
    let _ = client.post(format!("{}/query", BASE_URL))
        .json(&json!({ "sql": "CREATE INDEX idx_content ON documents(content) USING FTS" }))
        .send()
        .await;

    println!("  Populating FTS data (1000 docs)...");
    let insert_stmt = prepare_sql(client, "INSERT INTO documents VALUES ($1, $2)").await;
    
    for i in 0..1000 {
        let content = if i % 2 == 0 { "hello world rust programming" } else { "database benchmarking performance" };
        let _ = client.post(format!("{}/execute", BASE_URL))
            .json(&json!({ 
                "statement_id": insert_stmt,
                "params": [i, content]
            }))
            .send()
            .await;
    }

    let search_stmt = prepare_sql(client, "SELECT * FROM documents WHERE MATCH(content) AGAINST($1) LIMIT 10").await;

    run_benchmark(client, "FTS Search", move |c| {
        let sid = search_stmt.clone();
        async move {
            let term = if rand::rng().random_bool(0.5) { "rust" } else { "benchmarking" };
            c.post(format!("{}/execute", BASE_URL))
                .json(&json!({ 
                    "statement_id": sid,
                    "params": [term]
                }))
                .send()
                .await
        }
    }).await;
}

async fn benchmark_vector_search(client: &Client) {
    println!("\n[Scenario: Vector Search (HNSW)]");
    let _ = client.post(format!("{}/query", BASE_URL))
        .json(&json!({ "sql": "CREATE TABLE IF NOT EXISTS items (id INT, embedding VECTOR(3))" }))
        .send()
        .await;
    
    let _ = client.post(format!("{}/query", BASE_URL))
        .json(&json!({ "sql": "CREATE INDEX idx_embedding ON items(embedding) USING HNSW" }))
        .send()
        .await;

    println!("  Populating Vector data (1000 items)...");
    let insert_stmt = prepare_sql(client, "INSERT INTO items VALUES ($1, $2)").await;
    
    for i in 0..1000 {
        let vec = vec![rand::rng().random::<f32>(), rand::rng().random::<f32>(), rand::rng().random::<f32>()];
        let _ = client.post(format!("{}/execute", BASE_URL))
            .json(&json!({ 
                "statement_id": insert_stmt,
                "params": [i, vec]
            }))
            .send()
            .await;
    }

    let search_stmt = prepare_sql(client, "SELECT id FROM items ORDER BY VECTOR_DISTANCE(embedding, $1) LIMIT 5").await;

    run_benchmark(client, "Vector Search", move |c| {
        let sid = search_stmt.clone();
        async move {
             let query_vec = vec![rand::rng().random::<f32>(), rand::rng().random::<f32>(), rand::rng().random::<f32>()];
             c.post(format!("{}/execute", BASE_URL))
                .json(&json!({ 
                    "statement_id": sid,
                    "params": [query_vec]
                }))
                .send()
                .await
        }
    }).await;
}

async fn benchmark_join(client: &Client) {
    println!("\n[Scenario: JOIN (Hash Join)]");
    let _ = client.post(format!("{}/query", BASE_URL))
        .json(&json!({ "sql": "CREATE TABLE IF NOT EXISTS t1 (id INT, v1 TEXT)" }))
        .send()
        .await;
    let _ = client.post(format!("{}/query", BASE_URL))
        .json(&json!({ "sql": "CREATE TABLE IF NOT EXISTS t2 (id INT, t1_id INT, v2 TEXT)" }))
        .send()
        .await;

    println!("  Populating JOIN data...");
    let insert_t1 = prepare_sql(client, "INSERT INTO t1 VALUES ($1, $2)").await;
    let insert_t2 = prepare_sql(client, "INSERT INTO t2 VALUES ($1, $2, $3)").await;

    for i in 0..100 {
        let _ = client.post(format!("{}/execute", BASE_URL))
            .json(&json!({ "statement_id": insert_t1, "params": [i, format!("Val{}", i)] })).send().await;
    }
    for i in 0..1000 {
        let t1_id = i % 100;
        let _ = client.post(format!("{}/execute", BASE_URL))
            .json(&json!({ "statement_id": insert_t2, "params": [i, t1_id, format!("Detail{}", i)] })).send().await;
    }

    let join_stmt = prepare_sql(client, "SELECT t1.id, t2.v2 FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t1.id = $1").await;

    run_benchmark(client, "JOIN", move |c| {
        let sid = join_stmt.clone();
        async move {
            let id = rand::rng().random_range(0..100);
            c.post(format!("{}/execute", BASE_URL))
                .json(&json!({ 
                    "statement_id": sid,
                    "params": [id]
                }))
                .send()
                .await
        }
    }).await;
}

async fn benchmark_aggregation(client: &Client) {
    println!("\n[Scenario: Aggregation (Hash Agg)]");
    let agg_stmt = prepare_sql(client, "SELECT age, COUNT(*) FROM bench_users GROUP BY age").await;

    run_benchmark(client, "Aggregation", move |c| {
        let sid = agg_stmt.clone();
        async move {
            c.post(format!("{}/execute", BASE_URL))
                .json(&json!({ 
                    "statement_id": sid,
                    "params": []
                }))
                .send()
                .await
        }
    }).await;
}

async fn run_benchmark<F, Fut>(client: &Client, _name: &str, operation: F)
where
    F: Fn(Client) -> Fut + Send + Sync + 'static + Clone,
    Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>> + Send,
{
    let start_time = Instant::now();
    let histogram = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    let barrier = Arc::new(Barrier::new(CONCURRENCY));
    let requests_per_task = TOTAL_REQUESTS / CONCURRENCY;

    let mut handles = Vec::new();

    for _ in 0..CONCURRENCY {
        let c = client.clone();
        let op = operation.clone();
        let hist = histogram.clone();
        let bar = barrier.clone();

        handles.push(tokio::spawn(async move {
            bar.wait().await;
            
            for _ in 0..requests_per_task {
                let req_start = Instant::now();
                let res = op(c.clone()).await;
                let elapsed = req_start.elapsed().as_micros() as u64;

                if let Ok(response) = res {
                    if response.status().is_success() {
                        let mut h = hist.lock().unwrap();
                        h.record(elapsed).unwrap();
                    } else {
                        if rand::rng().random_bool(0.001) {
                             eprintln!("Request failed: {}", response.status());
                        }
                    }
                } else if let Err(e) = res {
                     if rand::rng().random_bool(0.001) {
                        eprintln!("Req Error: {}", e);
                     }
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let total_elapsed = start_time.elapsed();
    let hist = histogram.lock().unwrap();

    let total_ops = hist.len();
    let rps = total_ops as f64 / total_elapsed.as_secs_f64();

    println!("  Finished {} requests in {:.2?}", total_ops, total_elapsed);
    println!("  Throughput: {:.2} RPS", rps);
    println!("  Latency (us):");
    println!("    Min: {}", hist.min());
    println!("    P50: {}", hist.value_at_quantile(0.50));
    println!("    P95: {}", hist.value_at_quantile(0.95));
    println!("    P99: {}", hist.value_at_quantile(0.99));
    println!("    Max: {}", hist.max());
}

async fn benchmark_vector_search_tcp() {
    let start = std::time::Instant::now();
    let mut tasks = Vec::new();
    let requests_per_task = TOTAL_REQUESTS / CONCURRENCY;

    for _ in 0..CONCURRENCY {
        tasks.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(format!("127.0.0.1:{}", TCP_PORT)).await.expect("Connect failed");
            let mut latencies = Vec::new();
            
            for _ in 0..requests_per_task {
                let t0 = std::time::Instant::now();
                
                let mut req = Vec::with_capacity(7 + 16);
                req.extend_from_slice(b"WD");
                req.push(1); // OpCode 1 (Vector Search)
                req.extend_from_slice(&(16u32).to_le_bytes());
                
                req.extend_from_slice(&(5u32).to_le_bytes()); // Limit
                req.extend_from_slice(&rand::rng().random::<f32>().to_le_bytes());
                req.extend_from_slice(&rand::rng().random::<f32>().to_le_bytes());
                req.extend_from_slice(&rand::rng().random::<f32>().to_le_bytes());
                
                stream.write_all(&req).await.unwrap();
                
                let mut count_buf = [0u8; 4];
                stream.read_exact(&mut count_buf).await.unwrap();
                let count = u32::from_le_bytes(count_buf);
                
                for _ in 0..count {
                    let mut meta = [0u8; 8];
                    stream.read_exact(&mut meta).await.unwrap();
                    let id_len = u32::from_le_bytes(meta[4..8].try_into().unwrap());
                    let mut id_buf = vec![0u8; id_len as usize];
                    stream.read_exact(&mut id_buf).await.unwrap();
                }
                
                latencies.push(t0.elapsed().as_micros() as u64);
            }
            latencies
        }));
    }
    collect_and_print_metrics(tasks, start).await;
}

async fn benchmark_sql_insert_tcp() {
    let start = std::time::Instant::now();
    let mut tasks = Vec::new();
    let requests_per_task = TOTAL_REQUESTS / CONCURRENCY;

    for _ in 0..CONCURRENCY {
        tasks.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(format!("127.0.0.1:{}", TCP_PORT)).await.expect("Connect failed");
            let mut latencies = Vec::new();
            
            for _ in 0..requests_per_task {
                let id = rand::rng().random_range(0..1000000);
                let age = rand::rng().random_range(18..99);
                let name = format!("UserTcp{}", id);
                let sql = format!("INSERT INTO bench_users VALUES ({}, '{}', {})", id, name, age);
                let sql_bytes = sql.as_bytes();
                
                let t0 = std::time::Instant::now();
                
                let mut req = Vec::with_capacity(7 + sql_bytes.len());
                req.extend_from_slice(b"WD");
                req.push(2); // OpCode 2 (SQL)
                req.extend_from_slice(&(sql_bytes.len() as u32).to_le_bytes());
                req.extend_from_slice(sql_bytes);
                
                stream.write_all(&req).await.unwrap();
                
                // Read Response
                let mut status_buf = [0u8; 1];
                stream.read_exact(&mut status_buf).await.unwrap();
                if status_buf[0] == 0 {
                    // Success
                    let mut type_buf = [0u8; 1];
                    stream.read_exact(&mut type_buf).await.unwrap();
                    if type_buf[0] == 0 { // Message
                         let mut len_buf = [0u8; 4];
                         stream.read_exact(&mut len_buf).await.unwrap();
                         let len = u32::from_le_bytes(len_buf) as usize;
                         let mut msg_buf = vec![0u8; len];
                         stream.read_exact(&mut msg_buf).await.unwrap();
                    } else {
                         // Rows
                         read_rows_tcp(&mut stream).await.unwrap();
                    }
                } else {
                    // Error
                    let mut len_buf = [0u8; 4];
                    stream.read_exact(&mut len_buf).await.unwrap();
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut msg_buf = vec![0u8; len];
                    stream.read_exact(&mut msg_buf).await.unwrap();
                }

                latencies.push(t0.elapsed().as_micros() as u64);
            }
            latencies
        }));
    }
    collect_and_print_metrics(tasks, start).await;
}

async fn benchmark_sql_select_tcp() {
    let start = std::time::Instant::now();
    let mut tasks = Vec::new();
    let requests_per_task = TOTAL_REQUESTS / CONCURRENCY;

    for _ in 0..CONCURRENCY {
        tasks.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(format!("127.0.0.1:{}", TCP_PORT)).await.expect("Connect failed");
            let mut latencies = Vec::new();
            
            for _ in 0..requests_per_task {
                let id = rand::rng().random_range(0..1000000);
                let sql = format!("SELECT * FROM bench_users WHERE id = {}", id);
                let sql_bytes = sql.as_bytes();
                
                let t0 = std::time::Instant::now();
                
                let mut req = Vec::with_capacity(7 + sql_bytes.len());
                req.extend_from_slice(b"WD");
                req.push(2); // OpCode 2 (SQL)
                req.extend_from_slice(&(sql_bytes.len() as u32).to_le_bytes());
                req.extend_from_slice(sql_bytes);
                
                stream.write_all(&req).await.unwrap();
                
                // Read Response
                let mut status_buf = [0u8; 1];
                stream.read_exact(&mut status_buf).await.unwrap();
                if status_buf[0] == 0 {
                    // Success
                    let mut type_buf = [0u8; 1];
                    stream.read_exact(&mut type_buf).await.unwrap();
                    if type_buf[0] == 1 { // Rows
                         read_rows_tcp(&mut stream).await.unwrap();
                    } else {
                         // Message
                         let mut len_buf = [0u8; 4];
                         stream.read_exact(&mut len_buf).await.unwrap();
                         let len = u32::from_le_bytes(len_buf) as usize;
                         let mut msg_buf = vec![0u8; len];
                         stream.read_exact(&mut msg_buf).await.unwrap();
                    }
                } else {
                    // Error
                    let mut len_buf = [0u8; 4];
                    stream.read_exact(&mut len_buf).await.unwrap();
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut msg_buf = vec![0u8; len];
                    stream.read_exact(&mut msg_buf).await.unwrap();
                }

                latencies.push(t0.elapsed().as_micros() as u64);
            }
            latencies
        }));
    }
    collect_and_print_metrics(tasks, start).await;
}

async fn read_rows_tcp(stream: &mut TcpStream) -> std::io::Result<()> {
    // Col Count
    let mut buf4 = [0u8; 4];
    stream.read_exact(&mut buf4).await?;
    let col_count = u32::from_le_bytes(buf4);
    
    // Col Names
    for _ in 0..col_count {
        stream.read_exact(&mut buf4).await?;
        let len = u32::from_le_bytes(buf4) as usize;
        let mut name_buf = vec![0u8; len];
        stream.read_exact(&mut name_buf).await?;
    }
    
    // Row Count
    stream.read_exact(&mut buf4).await?;
    let row_count = u32::from_le_bytes(buf4);
    
    // Rows
    for _ in 0..row_count {
        for _ in 0..col_count {
             read_value_tcp(stream).await?;
        }
    }
    Ok(())
}

async fn read_value_tcp(stream: &mut TcpStream) -> std::io::Result<()> {
    let mut type_buf = [0u8; 1];
    stream.read_exact(&mut type_buf).await?;
    match type_buf[0] {
        0 => {}, // Null
        1 => { // Int
             let mut buf = [0u8; 8];
             stream.read_exact(&mut buf).await?;
        },
        2 => { // Float
             let mut buf = [0u8; 8];
             stream.read_exact(&mut buf).await?;
        },
        3 => { // String
             let mut len_buf = [0u8; 4];
             stream.read_exact(&mut len_buf).await?;
             let len = u32::from_le_bytes(len_buf) as usize;
             let mut buf = vec![0u8; len];
             stream.read_exact(&mut buf).await?;
        },
        4 => { // Bool
             let mut buf = [0u8; 1];
             stream.read_exact(&mut buf).await?;
        },
        5 => { // Vector
             let mut len_buf = [0u8; 4];
             stream.read_exact(&mut len_buf).await?;
             let len = u32::from_le_bytes(len_buf) as usize;
             let mut buf = vec![0u8; len * 4];
             stream.read_exact(&mut buf).await?;
        },
        _ => {}
    }
    Ok(())
}

async fn collect_and_print_metrics(tasks: Vec<tokio::task::JoinHandle<Vec<u64>>>, start: std::time::Instant) {
    let mut all_latencies = Vec::new();
    for t in tasks {
        let lats = t.await.unwrap();
        all_latencies.extend(lats);
    }
    
    let duration = start.elapsed();
    all_latencies.sort();
    if all_latencies.is_empty() { return; }
    
    let p50 = all_latencies[all_latencies.len() / 2];
    let p95 = all_latencies[(all_latencies.len() as f64 * 0.95) as usize];
    let p99 = all_latencies[(all_latencies.len() as f64 * 0.99) as usize];
    
    println!("  Finished {} requests in {:.2?}", all_latencies.len(), duration);
    println!("  Throughput: {:.2} RPS", all_latencies.len() as f64 / duration.as_secs_f64());
    println!("  Latency (us):");
    println!("    Min: {}", all_latencies[0]);
    println!("    P50: {}", p50);
    println!("    P95: {}", p95);
    println!("    P99: {}", p99);
    println!("    Max: {}", all_latencies[all_latencies.len() - 1]);
}

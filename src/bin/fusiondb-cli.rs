use anyhow::{anyhow, bail, Result};
use reqwest::{Client, Method};
use serde_json::json;

const DEFAULT_URL: &str = "http://127.0.0.1:8091";
const DEFAULT_USER: &str = "postgres";
const DEFAULT_PASSWORD: &str = "fusiondb";

#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    Health,
    Query(String),
    Tables,
    Metrics,
    Prometheus,
    SlowQueries,
    Checkpoint,
    Compact,
    Capabilities,
    AuthContext,
    Cdc {
        since: Option<u64>,
        limit: Option<usize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliConfig {
    base_url: String,
    user: String,
    password: String,
    command: Command,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequestSpec {
    method: Method,
    url: String,
    json_body: Option<serde_json::Value>,
    plain_text: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let Some(config) = parse_cli_args(&args)? else {
        println!("{}", usage());
        return Ok(());
    };

    let spec = request_spec(&config);
    let client = Client::new();
    let output = execute_request(&client, &config, spec).await?;
    println!("{output}");
    Ok(())
}

fn parse_cli_args(args: &[String]) -> Result<Option<CliConfig>> {
    if args.is_empty() || args.iter().any(|arg| arg == "-h" || arg == "--help") {
        return Ok(None);
    }

    let mut base_url = DEFAULT_URL.to_string();
    let mut user = std::env::var("FUSIONDB_HTTP_USER").unwrap_or_else(|_| DEFAULT_USER.to_string());
    let mut password =
        std::env::var("FUSIONDB_HTTP_PASSWORD").unwrap_or_else(|_| DEFAULT_PASSWORD.to_string());
    let mut position = 0;
    while position < args.len() {
        let arg = &args[position];
        if arg == "--url" {
            position += 1;
            let Some(value) = args.get(position) else {
                bail!("--url requires a value");
            };
            base_url = trim_base_url(value);
            position += 1;
        } else if let Some(value) = arg.strip_prefix("--url=") {
            base_url = trim_base_url(value);
            position += 1;
        } else if arg == "--user" {
            position += 1;
            let Some(value) = args.get(position) else {
                bail!("--user requires a value");
            };
            user = non_empty_value("--user", value)?;
            position += 1;
        } else if let Some(value) = arg.strip_prefix("--user=") {
            user = non_empty_value("--user", value)?;
            position += 1;
        } else if arg == "--password" {
            position += 1;
            let Some(value) = args.get(position) else {
                bail!("--password requires a value");
            };
            password = non_empty_value("--password", value)?;
            position += 1;
        } else if let Some(value) = arg.strip_prefix("--password=") {
            password = non_empty_value("--password", value)?;
            position += 1;
        } else {
            break;
        }
    }

    let Some(command_name) = args.get(position) else {
        bail!("missing command");
    };
    let command_args = &args[position + 1..];
    let command = parse_command(command_name, command_args)?;

    Ok(Some(CliConfig {
        base_url,
        user,
        password,
        command,
    }))
}

fn parse_command(name: &str, args: &[String]) -> Result<Command> {
    match name {
        "health" => no_args(name, args).map(|_| Command::Health),
        "query" => {
            if args.is_empty() {
                bail!("query requires SQL text");
            }
            Ok(Command::Query(args.join(" ")))
        }
        "tables" => no_args(name, args).map(|_| Command::Tables),
        "metrics" => no_args(name, args).map(|_| Command::Metrics),
        "prometheus" => no_args(name, args).map(|_| Command::Prometheus),
        "slow-queries" | "slow_queries" => no_args(name, args).map(|_| Command::SlowQueries),
        "checkpoint" => no_args(name, args).map(|_| Command::Checkpoint),
        "compact" | "vacuum" => no_args(name, args).map(|_| Command::Compact),
        "capabilities" => no_args(name, args).map(|_| Command::Capabilities),
        "auth-context" | "auth_context" => no_args(name, args).map(|_| Command::AuthContext),
        "cdc" | "cdc-events" | "cdc_events" => parse_cdc_command(args),
        _ => bail!("unknown command: {name}"),
    }
}

fn request_spec(config: &CliConfig) -> RequestSpec {
    let base = &config.base_url;
    match &config.command {
        Command::Health => RequestSpec {
            method: Method::GET,
            url: endpoint(base, "/health"),
            json_body: None,
            plain_text: true,
        },
        Command::Query(sql) => RequestSpec {
            method: Method::POST,
            url: endpoint(base, "/query"),
            json_body: Some(json!({ "sql": sql })),
            plain_text: false,
        },
        Command::Tables => get_json(base, "/tables"),
        Command::Metrics => get_json(base, "/metrics"),
        Command::Prometheus => RequestSpec {
            method: Method::GET,
            url: endpoint(base, "/metrics/prometheus"),
            json_body: None,
            plain_text: true,
        },
        Command::SlowQueries => get_json(base, "/slow_queries"),
        Command::Checkpoint => post_json(base, "/checkpoint"),
        Command::Compact => post_json(base, "/compact"),
        Command::Capabilities => get_json(base, "/capabilities"),
        Command::AuthContext => get_json(base, "/auth/context"),
        Command::Cdc { since, limit } => get_json(base, &cdc_events_path(*since, *limit)),
    }
}

async fn execute_request(client: &Client, config: &CliConfig, spec: RequestSpec) -> Result<String> {
    let mut request = client.request(spec.method, &spec.url);
    request = request.basic_auth(&config.user, Some(&config.password));
    if let Some(body) = spec.json_body {
        request = request.json(&body);
    }

    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("HTTP {status}: {text}");
    }
    if spec.plain_text {
        return Ok(text);
    }

    let json: serde_json::Value = serde_json::from_str(&text)
        .map_err(|error| anyhow!("expected JSON response from {}: {error}", spec.url))?;
    Ok(serde_json::to_string_pretty(&json)?)
}

fn get_json(base_url: &str, path: &str) -> RequestSpec {
    RequestSpec {
        method: Method::GET,
        url: endpoint(base_url, path),
        json_body: None,
        plain_text: false,
    }
}

fn post_json(base_url: &str, path: &str) -> RequestSpec {
    RequestSpec {
        method: Method::POST,
        url: endpoint(base_url, path),
        json_body: None,
        plain_text: false,
    }
}

fn endpoint(base_url: &str, path: &str) -> String {
    format!("{}{}", base_url.trim_end_matches('/'), path)
}

fn trim_base_url(value: &str) -> String {
    value.trim_end_matches('/').to_string()
}

fn non_empty_value(flag: &str, value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{flag} requires a non-empty value");
    }
    Ok(trimmed.to_string())
}

fn no_args(command: &str, args: &[String]) -> Result<()> {
    if args.is_empty() {
        Ok(())
    } else {
        bail!("{command} does not accept arguments")
    }
}

fn parse_cdc_command(args: &[String]) -> Result<Command> {
    let mut since = None;
    let mut limit = None;
    let mut position = 0;

    while position < args.len() {
        let arg = &args[position];
        if arg == "--since" {
            position += 1;
            let Some(value) = args.get(position) else {
                bail!("--since requires a value");
            };
            since = Some(parse_u64_flag("--since", value)?);
            position += 1;
        } else if let Some(value) = arg.strip_prefix("--since=") {
            since = Some(parse_u64_flag("--since", value)?);
            position += 1;
        } else if arg == "--limit" {
            position += 1;
            let Some(value) = args.get(position) else {
                bail!("--limit requires a value");
            };
            limit = Some(parse_usize_flag("--limit", value)?);
            position += 1;
        } else if let Some(value) = arg.strip_prefix("--limit=") {
            limit = Some(parse_usize_flag("--limit", value)?);
            position += 1;
        } else {
            bail!("unknown cdc argument: {arg}");
        }
    }

    Ok(Command::Cdc { since, limit })
}

fn parse_u64_flag(flag: &str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|error| anyhow!("{flag} requires an unsigned integer: {error}"))
}

fn parse_usize_flag(flag: &str, value: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|error| anyhow!("{flag} requires an unsigned integer: {error}"))
}

fn cdc_events_path(since: Option<u64>, limit: Option<usize>) -> String {
    let mut path = "/cdc/events".to_string();
    let mut first = true;

    if let Some(value) = since {
        push_query_param(&mut path, &mut first, "since", value);
    }
    if let Some(value) = limit {
        push_query_param(&mut path, &mut first, "limit", value);
    }

    path
}

fn push_query_param<T: std::fmt::Display>(
    path: &mut String,
    first: &mut bool,
    name: &str,
    value: T,
) {
    path.push(if *first { '?' } else { '&' });
    *first = false;
    path.push_str(name);
    path.push('=');
    path.push_str(&value.to_string());
}

fn usage() -> &'static str {
    "Usage: fusiondb-cli [--url URL] [--user USER] [--password PASSWORD] <command> [args]\n\
Environment: FUSIONDB_HTTP_USER, FUSIONDB_HTTP_PASSWORD\n\
\n\
Commands:\n\
  health                       GET /health\n\
  query <SQL...>               POST /query\n\
  tables                       GET /tables\n\
  metrics                      GET /metrics\n\
  prometheus                   GET /metrics/prometheus\n\
  slow-queries                 GET /slow_queries\n\
  checkpoint                   POST /checkpoint\n\
  compact | vacuum             POST /compact\n\
  capabilities                 GET /capabilities\n\
  cdc [--since N] [--limit N]  GET /cdc/events\n\
  auth-context                 GET /auth/context"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| (*arg).to_string()).collect()
    }

    #[test]
    fn parses_default_health_command() {
        let config = parse_cli_args(&owned(&["health"])).unwrap().unwrap();

        assert_eq!(config.base_url, DEFAULT_URL);
        assert_eq!(config.user, DEFAULT_USER);
        assert_eq!(config.password, DEFAULT_PASSWORD);
        assert_eq!(config.command, Command::Health);
    }

    #[test]
    fn parses_url_user_and_query_sql() {
        let config = parse_cli_args(&owned(&[
            "--url",
            "http://localhost:9000/",
            "--user=admin",
            "--password=secret",
            "query",
            "SELECT",
            "1",
        ]))
        .unwrap()
        .unwrap();

        assert_eq!(config.base_url, "http://localhost:9000");
        assert_eq!(config.user, "admin");
        assert_eq!(config.password, "secret");
        assert_eq!(config.command, Command::Query("SELECT 1".to_string()));
    }

    #[test]
    fn builds_query_request_body() {
        let config = parse_cli_args(&owned(&["query", "SELECT", "42"]))
            .unwrap()
            .unwrap();
        let spec = request_spec(&config);

        assert_eq!(spec.method, Method::POST);
        assert_eq!(spec.url, "http://127.0.0.1:8091/query");
        assert_eq!(spec.json_body, Some(json!({ "sql": "SELECT 42" })));
        assert!(!spec.plain_text);
    }

    #[test]
    fn vacuum_alias_uses_compact_endpoint() {
        let config = parse_cli_args(&owned(&["vacuum"])).unwrap().unwrap();
        let spec = request_spec(&config);

        assert_eq!(config.command, Command::Compact);
        assert_eq!(spec.method, Method::POST);
        assert_eq!(spec.url, "http://127.0.0.1:8091/compact");
    }

    #[test]
    fn prometheus_and_health_are_plain_text() {
        let health = request_spec(&parse_cli_args(&owned(&["health"])).unwrap().unwrap());
        let prometheus = request_spec(&parse_cli_args(&owned(&["prometheus"])).unwrap().unwrap());

        assert!(health.plain_text);
        assert!(prometheus.plain_text);
        assert_eq!(prometheus.url, "http://127.0.0.1:8091/metrics/prometheus");
    }

    #[test]
    fn cdc_command_builds_since_and_limit_query() {
        let config = parse_cli_args(&owned(&["cdc", "--since", "1048576", "--limit=25"]))
            .unwrap()
            .unwrap();
        let spec = request_spec(&config);

        assert_eq!(
            config.command,
            Command::Cdc {
                since: Some(1048576),
                limit: Some(25)
            }
        );
        assert_eq!(spec.method, Method::GET);
        assert_eq!(
            spec.url,
            "http://127.0.0.1:8091/cdc/events?since=1048576&limit=25"
        );
    }

    #[test]
    fn cdc_command_rejects_unknown_argument() {
        let err = parse_cli_args(&owned(&["cdc", "--tail"])).unwrap_err();

        assert!(err.to_string().contains("unknown cdc argument"));
    }

    #[test]
    fn rejects_unknown_command() {
        let err = parse_cli_args(&owned(&["unknown"])).unwrap_err();

        assert!(err.to_string().contains("unknown command"));
    }
}

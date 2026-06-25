use anyhow::{anyhow, bail, Result};
use reqwest::{Client, Method};
use serde_json::json;

const DEFAULT_URL: &str = "http://127.0.0.1:8091";

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliConfig {
    base_url: String,
    user: Option<String>,
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
    let mut user = None;
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
            user = Some(non_empty_value("--user", value)?);
            position += 1;
        } else if let Some(value) = arg.strip_prefix("--user=") {
            user = Some(non_empty_value("--user", value)?);
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
    }
}

async fn execute_request(client: &Client, config: &CliConfig, spec: RequestSpec) -> Result<String> {
    let mut request = client.request(spec.method, &spec.url);
    if let Some(user) = &config.user {
        request = request.header("x-fusiondb-user", user);
    }
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

fn usage() -> &'static str {
    "Usage: fusiondb-cli [--url URL] [--user USER] <command> [args]\n\
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
        assert_eq!(config.user, None);
        assert_eq!(config.command, Command::Health);
    }

    #[test]
    fn parses_url_user_and_query_sql() {
        let config = parse_cli_args(&owned(&[
            "--url",
            "http://localhost:9000/",
            "--user=admin",
            "query",
            "SELECT",
            "1",
        ]))
        .unwrap()
        .unwrap();

        assert_eq!(config.base_url, "http://localhost:9000");
        assert_eq!(config.user.as_deref(), Some("admin"));
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
    fn rejects_unknown_command() {
        let err = parse_cli_args(&owned(&["unknown"])).unwrap_err();

        assert!(err.to_string().contains("unknown command"));
    }
}

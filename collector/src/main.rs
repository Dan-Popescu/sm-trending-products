mod error;
mod normalize;
mod request;

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use error::{CollectorError, Result};
use request::{CollectionRequest, FetchedPage};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Parser)]
#[command(
    version,
    about = "Bounded one-page TikTok collector; JSON stdin and stdout"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Re-normalize an archived response without making an HTTP request.
    Normalize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayRequest {
    request: CollectionRequest,
    raw: Value,
    observed_at: DateTime<Utc>,
}

async fn run(cli: Cli) -> Result<Value> {
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(8 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| CollectorError::new("configuration", "stdin_read_failed"))?;
    if bytes.len() > 8 * 1024 * 1024 {
        return Err(CollectorError::new("configuration", "input_too_large"));
    }
    match cli.command {
        Some(Command::Normalize) => {
            let mut replay: ReplayRequest = serde_json::from_slice(&bytes)
                .map_err(|_| CollectorError::new("configuration", "invalid_replay_request"))?;
            replay.request.validate()?;
            let token = std::env::var(replay.request.source.token_name()).unwrap_or_default();
            request::redact(&mut replay.raw, &token);
            normalize::normalize(
                &replay.request,
                FetchedPage {
                    raw: replay.raw,
                    observed_at: replay.observed_at,
                    request_count: 0,
                },
            )
        }
        None => {
            let request: CollectionRequest = serde_json::from_slice(&bytes)
                .map_err(|_| CollectorError::new("configuration", "invalid_collection_request"))?;
            request.validate()?;
            let page = request::fetch(&request).await?;
            normalize::normalize(&request, page)
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let cli = Cli::parse();
    let (value, failed) = match run(cli).await {
        Ok(value) => (value, false),
        Err(error) => (json!({"error": error}), true),
    };
    let mut output = serde_json::to_vec(&value).unwrap_or_else(|_| {
        b"{\"error\":{\"category\":\"internal\",\"code\":\"serialization_failed\"}}".to_vec()
    });
    output.push(b'\n');
    let mut stdout = tokio::io::stdout();
    if stdout.write_all(&output).await.is_err() || stdout.flush().await.is_err() {
        std::process::exit(2);
    }
    if failed {
        std::process::exit(1);
    }
}

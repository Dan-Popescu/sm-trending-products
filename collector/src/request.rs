use crate::error::{CollectorError, Result};
use chrono::{DateTime, Utc};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;
use tokio_postgres::{types::Type, NoTls};

const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

// Full jitter keeps independent workers from retrying an outage in lockstep.
// The source Retry-After path returns its minimum untouched; task scheduling
// therefore cannot retry it earlier than the server requested.
fn retry_delay_with_entropy(attempt: usize, entropy: u64) -> Duration {
    let cap_millis = 200_u64 * (1_u64 << attempt.min(5));
    Duration::from_millis(1 + entropy % cap_millis)
}

fn retry_delay(attempt: usize) -> Duration {
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u32(std::process::id());
    hasher.write_usize(attempt);
    retry_delay_with_entropy(attempt, hasher.finish())
}

fn deferred_retry_seconds() -> f64 {
    5.0 + retry_delay(4).as_secs_f64()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    #[serde(rename = "tiktok_display")]
    Display,
    #[serde(rename = "tiktok_ads")]
    Ads,
    #[serde(rename = "tiktok_commercial")]
    Commercial,
}

impl Source {
    pub fn name(self) -> &'static str {
        match self {
            Self::Display => "tiktok_display",
            Self::Ads => "tiktok_ads",
            Self::Commercial => "tiktok_commercial",
        }
    }

    pub fn token_name(self) -> &'static str {
        match self {
            Self::Display => "TIKTOK_DISPLAY_ACCESS_TOKEN",
            _ => "TIKTOK_CCL_ACCESS_TOKEN",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    #[default]
    List,
    Query,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionRequest {
    pub source: Source,
    #[serde(default)]
    pub operation: Operation,
    pub account_id: String,
    #[serde(default)]
    pub synthetic: bool,
    #[serde(default = "empty_object")]
    pub payload: Value,
    pub endpoint_override: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
}

fn empty_object() -> Value {
    json!({})
}

impl CollectionRequest {
    pub fn validate(&self) -> Result<()> {
        if self.account_id.is_empty() || self.account_id.len() > 256 {
            return Err(CollectorError::new("configuration", "invalid_account_id"));
        }
        if !self.synthetic && self.observed_at.is_some() {
            return Err(CollectorError::new(
                "configuration",
                "live_time_override_forbidden",
            ));
        }
        if !self.payload.is_object() {
            return Err(CollectorError::new(
                "configuration",
                "payload_must_be_object",
            ));
        }
        self.url()?;
        self.body()?;
        Ok(())
    }

    pub fn url(&self) -> Result<Url> {
        let path = match (self.source, self.operation) {
            (Source::Display, Operation::List) => "/v2/video/list/",
            (Source::Display, Operation::Query) => "/v2/video/query/",
            (Source::Ads, _) => "/v2/research/adlib/ad/query/",
            (Source::Commercial, _) => "/v2/research/adlib/commercial_content/query/",
        };
        let mut url = if let Some(origin) = &self.endpoint_override {
            if !self.synthetic {
                return Err(CollectorError::new(
                    "configuration",
                    "live_endpoint_override_forbidden",
                ));
            }
            let parsed = Url::parse(origin)
                .map_err(|_| CollectorError::new("configuration", "invalid_test_endpoint"))?;
            let host = parsed.host_str().unwrap_or("");
            if parsed.scheme() != "http"
                || !matches!(host, "127.0.0.1" | "[::1]" | "::1")
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(CollectorError::new(
                    "configuration",
                    "test_endpoint_not_loopback",
                ));
            }
            parsed
        } else {
            Url::parse("https://open.tiktokapis.com")
                .map_err(|_| CollectorError::new("configuration", "invalid_source_origin"))?
        };
        url.set_path(path);
        let fields = match self.source {
            Source::Display => "id,create_time,video_description,duration,title,like_count,comment_count,share_count,view_count,share_url",
            Source::Ads => "ad.id,ad.first_shown_date,ad.last_shown_date,ad.status,ad.videos,ad.reach,advertiser.business_id,advertiser.business_name,advertiser.paid_for_by",
            Source::Commercial => "id,create_timestamp,create_date,label,brand_names,creator,videos",
        };
        url.query_pairs_mut().append_pair("fields", fields);
        Ok(url)
    }

    pub fn body(&self) -> Result<Value> {
        let mut body = self
            .payload
            .as_object()
            .cloned()
            .ok_or_else(|| CollectorError::new("configuration", "payload_must_be_object"))?;
        match (self.source, self.operation) {
            (Source::Display, Operation::Query) => {
                let ids = body
                    .get("filters")
                    .and_then(|v| v.get("video_ids"))
                    .and_then(Value::as_array)
                    .ok_or_else(|| CollectorError::new("configuration", "video_ids_required"))?;
                if ids.is_empty() || ids.len() > 20 || ids.iter().any(|id| id.as_str().is_none()) {
                    return Err(CollectorError::new("configuration", "invalid_video_ids"));
                }
            }
            (source, _) => {
                let limit = match source {
                    Source::Display => 20,
                    Source::Ads => 10,
                    Source::Commercial => 50,
                };
                let max_count = match body.get("max_count") {
                    Some(value) => value
                        .as_u64()
                        .ok_or_else(|| CollectorError::new("configuration", "invalid_max_count"))?,
                    None => limit,
                };
                if !(1..=limit).contains(&max_count) {
                    return Err(CollectorError::new("configuration", "invalid_max_count"));
                }
                body.insert("max_count".into(), json!(max_count));
                if !matches!(source, Source::Display)
                    && !body.get("filters").is_some_and(Value::is_object)
                {
                    return Err(CollectorError::new("configuration", "filters_required"));
                }
            }
        }
        if let Some(cursor) = body.get("cursor") {
            if !cursor.is_i64() && !cursor.is_u64() {
                return Err(CollectorError::new("configuration", "invalid_cursor"));
            }
        }
        Ok(Value::Object(body))
    }
}

pub struct FetchedPage {
    pub raw: Value,
    pub observed_at: DateTime<Utc>,
    pub request_count: usize,
}

struct Quota {
    client: Option<tokio_postgres::Client>,
    credential_key: Option<String>,
}

impl Quota {
    async fn connect(request: &CollectionRequest, token: &str) -> Result<Self> {
        let database_url = match std::env::var("DATABASE_URL") {
            Ok(value) if !value.is_empty() => value,
            _ if request.synthetic => {
                return Ok(Self {
                    client: None,
                    credential_key: None,
                })
            }
            _ => {
                return Err(CollectorError::new(
                    "configuration",
                    "database_url_required",
                ))
            }
        };
        let (client, connection) = tokio::time::timeout(
            Duration::from_secs(10),
            tokio_postgres::connect(&database_url, NoTls),
        )
        .await
        .map_err(|_| CollectorError::new("quota", "database_timeout"))?
        .map_err(|_| CollectorError::new("quota", "database_unavailable"))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self {
            client: Some(client),
            credential_key: if request.synthetic {
                None
            } else {
                Some(format!("{:x}", Sha256::digest(token.as_bytes())))
            },
        })
    }

    async fn reserve(&self, request: &CollectionRequest) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        let rows = tokio::time::timeout(
            Duration::from_secs(10),
            client.query_typed(
                "SELECT allowed, retry_after_seconds::float8 FROM reserve_request($1, $2, $3)",
                &[
                    (&request.source.name(), Type::TEXT),
                    (&request.account_id, Type::TEXT),
                    (&self.credential_key, Type::TEXT),
                ],
            ),
        )
        .await
        .map_err(|_| CollectorError::new("quota", "reservation_timeout"))?
        .map_err(|_| CollectorError::new("quota", "reservation_failed"))?;
        if rows.len() != 1 {
            return Err(CollectorError::new("quota", "invalid_reservation_contract"));
        }
        let row = &rows[0];
        let allowed: bool = row
            .try_get(0)
            .map_err(|_| CollectorError::new("quota", "invalid_reservation_contract"))?;
        let wait: f64 = row
            .try_get(1)
            .map_err(|_| CollectorError::new("quota", "invalid_reservation_contract"))?;
        if !allowed {
            return Err(CollectorError::retry("quota", "quota_exhausted", wait));
        }
        Ok(())
    }
}

pub async fn fetch(request: &CollectionRequest) -> Result<FetchedPage> {
    let token = std::env::var(request.source.token_name()).unwrap_or_default();
    if token.is_empty() {
        return Err(CollectorError::new("authorization", "missing_access_token"));
    }
    let quota = Quota::connect(request, &token).await?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent("sm-trending-products/0.1")
        .build()
        .map_err(|_| CollectorError::new("configuration", "http_client_unavailable"))?;
    let url = request.url()?;
    let body = request.body()?;
    for attempt in 0..3 {
        quota.reserve(request).await?;
        let response = client
            .post(url.clone())
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await;
        let mut response = match response {
            Ok(value) => value,
            Err(_) if attempt < 2 => {
                tokio::time::sleep(retry_delay(attempt)).await;
                continue;
            }
            Err(_) => {
                return Err(CollectorError::retry(
                    "transport",
                    "request_failed",
                    deferred_retry_seconds(),
                ))
            }
        };
        let status = response.status();
        if status.as_u16() == 429 {
            let retry_header = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok());
            let wait = retry_header
                .and_then(|v| {
                    v.parse::<f64>().ok().or_else(|| {
                        DateTime::parse_from_rfc2822(v).ok().map(|date| {
                            (date.with_timezone(&Utc) - Utc::now())
                                .num_milliseconds()
                                .max(0) as f64
                                / 1000.0
                        })
                    })
                })
                .filter(|v| v.is_finite() && *v >= 0.0)
                .unwrap_or(60.0);
            return Err(CollectorError::retry(
                "rate_limit",
                "source_rate_limit",
                wait,
            ));
        }
        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Err(CollectorError::new("authorization", "source_access_denied"));
        }
        if status.is_server_error() && attempt < 2 {
            tokio::time::sleep(retry_delay(attempt)).await;
            continue;
        }
        if status.is_server_error() {
            return Err(CollectorError::retry(
                "transport",
                "source_unavailable",
                deferred_retry_seconds(),
            ));
        }
        if !status.is_success() {
            return Err(CollectorError::new("source", "http_status_error"));
        }
        if response
            .content_length()
            .is_some_and(|size| size > MAX_RESPONSE_BYTES as u64)
        {
            return Err(CollectorError::new("schema", "response_too_large"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CollectorError::new("transport", "response_interrupted"))?
        {
            if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(CollectorError::new("schema", "response_too_large"));
            }
            bytes.extend_from_slice(&chunk);
        }
        let observed_at = request.observed_at.unwrap_or_else(Utc::now);
        let mut raw: Value =
            match serde_json::from_slice(&bytes) {
                Ok(raw) => raw,
                Err(_) => {
                    let clean_text = String::from_utf8_lossy(&bytes).replace(&token, "[REDACTED]");
                    let bounded: String = clean_text.chars().take(65_536).collect();
                    let truncated = bounded.len() < clean_text.len();
                    return Err(CollectorError::new("schema", "invalid_json_response")
                        .with_artifact(schema_artifact(
                            request,
                            json!({"invalid_json_text": bounded, "truncated": truncated}),
                            observed_at,
                        )));
                }
            };
        if let Some(code) = raw.get("error").and_then(|v| v.get("code")) {
            let okay = code == "ok" || code == 0;
            if !okay {
                let text = code.as_str().unwrap_or("");
                let source_status = raw
                    .get("error")
                    .and_then(|v| v.get("http_status_code"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let category = if source_status == 401
                    || source_status == 403
                    || text.contains("token")
                    || text.contains("scope")
                    || text.contains("auth")
                    || text.contains("permission")
                {
                    "authorization"
                } else if source_status == 429 || text.contains("rate_limit") {
                    "rate_limit"
                } else {
                    "source"
                };
                return Err(if category == "rate_limit" {
                    CollectorError::retry(category, "source_error_response", 60.0)
                } else {
                    CollectorError::new(category, "source_error_response")
                });
            }
        }
        redact(&mut raw, &token);
        return Ok(FetchedPage {
            raw,
            observed_at,
            request_count: attempt + 1,
        });
    }
    Err(CollectorError::new("transport", "retry_limit_reached"))
}

pub fn schema_artifact(
    request: &CollectionRequest,
    raw: Value,
    observed_at: DateTime<Utc>,
) -> Value {
    let mut artifact = json!({
        "raw": raw,
        "observed_at": observed_at,
        "source": request.source,
        "account_id": request.account_id,
        "synthetic": request.synthetic
    });
    let token = std::env::var(request.source.token_name()).unwrap_or_default();
    redact(&mut artifact, &token);
    artifact
}

pub fn redact(value: &mut Value, token: &str) {
    match value {
        Value::Object(object) => {
            let mut clean = Map::new();
            for (key, mut child) in std::mem::take(object) {
                let lower = key.to_ascii_lowercase();
                if [
                    "access_token",
                    "refresh_token",
                    "authorization",
                    "client_secret",
                    "token",
                    "secret",
                ]
                .contains(&lower.as_str())
                {
                    clean.insert(key, json!("[REDACTED]"));
                } else {
                    redact(&mut child, token);
                    clean.insert(key, child);
                }
            }
            *object = clean;
        }
        Value::Array(values) => {
            for child in values {
                redact(child, token);
            }
        }
        Value::String(text) if !token.is_empty() => {
            *text = text.replace(token, "[REDACTED]");
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::retry_delay_with_entropy;
    use std::time::Duration;

    #[test]
    fn transient_backoff_has_bounded_jitter_without_sleeping() {
        assert_eq!(retry_delay_with_entropy(0, 0), Duration::from_millis(1));
        assert_eq!(retry_delay_with_entropy(0, 199), Duration::from_millis(200));
        assert_eq!(retry_delay_with_entropy(1, 399), Duration::from_millis(400));
        for entropy in [0, 1, 399, 50000, u64::MAX] {
            assert!(retry_delay_with_entropy(1, entropy) <= Duration::from_millis(400));
            assert!(retry_delay_with_entropy(100, entropy) <= Duration::from_millis(6400));
        }
        assert_ne!(
            retry_delay_with_entropy(1, 17),
            retry_delay_with_entropy(1, 18)
        );
    }
}

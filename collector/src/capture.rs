//! Durable, bounded response receipts. No record normalizer runs on this path.

use crate::error::{CollectorError, Result};
use crate::{normalize, request};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const RESERVATION: u64 = 32 * 1024 * 1024;

fn sanitize(value: &mut Value, secrets: &[String]) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                if [
                    "authorization",
                    "access_token",
                    "refresh_token",
                    "client_secret",
                    "password",
                    "token",
                    "secret",
                    "api_key",
                    "database_url",
                ]
                .contains(&key.to_ascii_lowercase().as_str())
                {
                    *child = json!("[REDACTED]");
                } else {
                    sanitize(child, secrets);
                }
            }
        }
        Value::Array(values) => {
            for child in values {
                sanitize(child, secrets);
            }
        }
        Value::String(text) => {
            for secret in secrets {
                *text = text.replace(secret, "[REDACTED]");
            }
        }
        _ => {}
    }
}

fn sanitize_bytes(body: &[u8], secrets: &[String]) -> Vec<u8> {
    let mut clean = body.to_vec();
    for secret in secrets {
        let needle = secret.as_bytes();
        let mut replaced = Vec::new();
        let mut position = 0;
        while let Some(offset) = clean[position..]
            .windows(needle.len())
            .position(|part| part == needle)
        {
            replaced.extend_from_slice(&clean[position..position + offset]);
            replaced.extend_from_slice(b"[REDACTED]");
            position += offset + needle.len();
        }
        replaced.extend_from_slice(&clean[position..]);
        clean = replaced;
    }
    clean
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    request: request::CollectionRequest,
    capture_id: String,
    task_id: String,
    page_index: u64,
    worker_id: String,
    worker_generation: Option<u64>,
    #[serde(default)]
    capture_attempt: u64,
    lease_version: u64,
    request_fingerprint: String,
}

#[derive(Deserialize)]
struct ReceiptChecksum<'a> {
    #[serde(borrow)]
    response: &'a serde_json::value::RawValue,
    response_sha256: &'a str,
}

fn io_error(_: std::io::Error) -> CollectorError {
    CollectorError::new("capture", "capture_spool_io_failed")
}

fn hex_id(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn open_private(path: &Path) -> Result<File> {
    if fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink()) {
        return Err(CollectorError::new("capture", "capture_spool_symlink"));
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)
        .map_err(io_error)
}

fn read_receipt(path: &Path, input: &Input) -> Result<Option<Value>> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
        Ok(meta) if !meta.is_file() || meta.len() > RESERVATION => {
            return Err(CollectorError::new("capture", "invalid_capture_spool"));
        }
        _ => {}
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(io_error)?
        .take(RESERVATION + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    let receipt: Value = serde_json::from_slice(&bytes)
        .map_err(|_| CollectorError::new("capture", "incomplete_capture_spool"))?;
    if receipt["capture_version"] != "1"
        || receipt["capture"]["capture_id"] != input.capture_id
        || receipt["capture"]["task_id"] != input.task_id
        || receipt["capture"]["page_index"] != input.page_index
        || receipt["capture"]["request_fingerprint"] != input.request_fingerprint
    {
        return Err(CollectorError::new("capture", "capture_identity_collision"));
    }
    // Hash the stored JSON span: decoding/re-encoding floating point numbers
    // must never make a complete receipt impossible to adopt after a crash.
    let checksum: ReceiptChecksum<'_> = serde_json::from_slice(&bytes)
        .map_err(|_| CollectorError::new("capture", "invalid_capture_spool"))?;
    let expected = format!("{:x}", Sha256::digest(checksum.response.get().as_bytes()));
    if checksum.response_sha256 != expected {
        return Err(CollectorError::new("capture", "capture_checksum_mismatch"));
    }
    Ok(Some(receipt))
}

fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)
        .map_err(io_error)?
        .sync_all()
        .map_err(io_error)
}

fn reserve(directory: &Path, id: &str, maximum: u64) -> Result<PathBuf> {
    let lock = open_private(&directory.join(".budget.lock"))?;
    lock.lock().map_err(io_error)?;
    let own = directory.join(format!("{id}.reserve"));
    if !own.exists() {
        let mut used = 0_u64;
        for entry in fs::read_dir(directory).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "reserve") {
                used = used.saturating_add(RESERVATION);
            } else if path
                .extension()
                .is_some_and(|ext| ext == "json" || ext == "part")
            {
                used = used.saturating_add(entry.metadata().map_err(io_error)?.len());
            }
            if used.saturating_add(RESERVATION) > maximum {
                return Err(CollectorError::new("capture", "capture_spool_capacity"));
            }
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&own)
            .map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        sync_directory(directory)?;
    }
    Ok(own)
}

fn release_reservation(directory: &Path, id: &str) -> Result<()> {
    let lock = open_private(&directory.join(".budget.lock"))?;
    lock.lock().map_err(io_error)?;
    match fs::remove_file(directory.join(format!("{id}.reserve"))) {
        Ok(()) => sync_directory(directory),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

pub async fn capture(bytes: &[u8], directory: &Path, maximum: u64) -> Result<Value> {
    let input: Input = serde_json::from_slice(bytes)
        .map_err(|_| CollectorError::new("configuration", "invalid_capture_request"))?;
    input.request.validate()?;
    if !hex_id(&input.capture_id)
        || !hex_id(&input.task_id)
        || !hex_id(&input.request_fingerprint)
        || input.worker_id.is_empty()
        || input.worker_id.len() > 256
        || input.worker_generation == Some(0)
        || maximum < RESERVATION
    {
        return Err(CollectorError::new(
            "configuration",
            "invalid_capture_identity",
        ));
    }
    fs::create_dir_all(directory).map_err(io_error)?;
    if fs::symlink_metadata(directory)
        .map_err(io_error)?
        .file_type()
        .is_symlink()
    {
        return Err(CollectorError::new("capture", "capture_spool_symlink"));
    }
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).map_err(io_error)?;
    // Fixed lock shards bound inode use as successful task history grows. The
    // same task/page always shares one lock, including across process replacement.
    let lock = open_private(&directory.join(format!("capture-{}.lock", &input.capture_id[..2])))?;
    lock.lock().map_err(io_error)?;
    let final_path = directory.join(format!("{}.json", input.capture_id));
    if let Some(receipt) = read_receipt(&final_path, &input)? {
        release_reservation(directory, &input.capture_id)?;
        return Ok(receipt);
    }
    let partial_path = directory.join(format!("{}.part", input.capture_id));
    if let Some(receipt) = read_receipt(&partial_path, &input)? {
        File::open(&partial_path)
            .map_err(io_error)?
            .sync_all()
            .map_err(io_error)?;
        fs::rename(&partial_path, &final_path).map_err(io_error)?;
        sync_directory(directory)?;
        release_reservation(directory, &input.capture_id)?;
        return Ok(receipt);
    }
    let reservation = reserve(directory, &input.capture_id, maximum)?;
    let mut secrets: Vec<String> = std::env::vars()
        .filter_map(|(name, value)| {
            let name = name.to_ascii_uppercase();
            (value.len() > 4
                && ["TOKEN", "SECRET", "PASSWORD"]
                    .iter()
                    .any(|part| name.contains(part)))
            .then_some(value)
        })
        .collect();
    let token = std::env::var(input.request.source.token_name()).unwrap_or_default();
    if !token.is_empty() {
        secrets.push(token);
    }
    secrets.sort_by_key(|value| std::cmp::Reverse(value.len()));
    secrets.dedup();
    let fetched = request::fetch_observed(&input.request, |body, observed_at, request_count| {
        let captured_at = Utc::now();
        let (mut raw, invalid_json) = match serde_json::from_slice::<Value>(body) {
            Ok(raw) => (raw, false),
            Err(_) => {
                // Preserve every sanitized byte, not the old bounded diagnostic excerpt.
                let clean = sanitize_bytes(body, &secrets);
                let encoded: String = clean.iter().map(|byte| format!("{byte:02x}")).collect();
                (
                    json!({"invalid_json_hex": encoded, "truncated": false}),
                    true,
                )
            }
        };
        sanitize(&mut raw, &secrets);
        let continuation = if invalid_json {
            Err(CollectorError::new("schema", "captured_source_error"))
        } else if let Some(error) = request::source_error(&raw) {
            Err(error)
        } else if let Some(data) = raw.get("data").and_then(Value::as_object) {
            normalize::continuation(&input.request, data)
        } else {
            Err(CollectorError::new("schema", "data_missing"))
        };
        let (next, error) = match continuation {
            Ok(next) => (next, Value::Null),
            Err(error) => (
                Value::Null,
                json!({"category":error.category,"code":error.code,
                       "retry_after_seconds":error.retry_after_seconds}),
            ),
        };
        let raw_bytes = serde_json::to_vec(&raw)
            .map_err(|_| CollectorError::new("capture", "capture_serialization_failed"))?;
        let mut request = serde_json::to_value(&input.request)
            .map_err(|_| CollectorError::new("capture", "capture_serialization_failed"))?;
        sanitize(&mut request, &secrets);
        let mut capture = json!({
            "capture_id":input.capture_id,"task_id":input.task_id,
            "page_index":input.page_index,"worker_id":input.worker_id,
            "lease_version":input.lease_version,"request_fingerprint":input.request_fingerprint,
            "capture_attempt":input.capture_attempt,
            "captured_at":captured_at,
        });
        if let Some(generation) = input.worker_generation {
            capture["worker_generation"] = json!(generation);
        }
        let receipt = json!({
            "capture_version":"1", "request":request, "response":raw,
            "response_sha256":format!("{:x}",Sha256::digest(&raw_bytes)),
            "observed_at":observed_at, "next_payload":next, "continuation_error":error,
            "request_count":request_count,
            "capture":capture,
        });
        let payload = serde_json::to_vec(&receipt)
            .map_err(|_| CollectorError::new("capture", "capture_serialization_failed"))?;
        if payload.len() as u64 > RESERVATION {
            return Err(CollectorError::new("capture", "capture_spool_capacity"));
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&partial_path)
            .map_err(io_error)?;
        file.write_all(&payload).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        fs::rename(&partial_path, &final_path).map_err(io_error)?;
        sync_directory(directory)?;
        release_reservation(directory, &input.capture_id)?;
        Ok(())
    })
    .await;
    if let Some(receipt) = read_receipt(&final_path, &input)? {
        release_reservation(directory, &input.capture_id)?;
        return Ok(receipt);
    }
    // If no bytes were received, release capacity. A partial receipt stays reserved.
    if !partial_path.exists() && reservation.exists() {
        release_reservation(directory, &input.capture_id)?;
    }
    fetched.map(|_| Value::Null)
}

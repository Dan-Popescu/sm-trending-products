use crate::error::{CollectorError, Result};
use crate::request::{CollectionRequest, FetchedPage, Operation, Source};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashSet};

#[derive(Clone, Copy, Default)]
pub enum TransformVersion {
    #[default]
    V1,
    V2,
}

impl<'de> Deserialize<'de> for TransformVersion {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        match String::deserialize(deserializer)?.as_str() {
            "1" => Ok(Self::V1),
            "2" => Ok(Self::V2),
            other => Err(serde::de::Error::unknown_variant(other, &["1", "2"])),
        }
    }
}

#[derive(Clone, Copy)]
struct RevisionContext<'a> {
    available_at: DateTime<Utc>,
    source_dataset_id: &'a str,
}

pub fn normalize(request: &CollectionRequest, page: FetchedPage) -> Result<Value> {
    normalize_with_revision(request, page, None)
}

pub fn replay(
    request: &CollectionRequest,
    page: FetchedPage,
    version: TransformVersion,
    available_at: Option<DateTime<Utc>>,
    source_dataset_id: Option<&str>,
) -> Result<Value> {
    let revision = match version {
        TransformVersion::V1 => None,
        TransformVersion::V2 => {
            let available = available_at.ok_or_else(|| {
                CollectorError::new("configuration", "normalizer_availability_required")
            })?;
            if available < page.observed_at {
                return Err(CollectorError::new(
                    "configuration",
                    "normalizer_availability_before_observation",
                ));
            }
            let parent = source_dataset_id.ok_or_else(|| {
                CollectorError::new("configuration", "normalizer_source_dataset_required")
            })?;
            if parent.trim().is_empty()
                || parent.len() > 256
                || parent.chars().any(char::is_control)
            {
                return Err(CollectorError::new(
                    "configuration",
                    "invalid_source_dataset_id",
                ));
            }
            Some(RevisionContext {
                available_at: available,
                source_dataset_id: parent,
            })
        }
    };
    normalize_with_revision(request, page, revision)
}

fn normalize_with_revision(
    request: &CollectionRequest,
    page: FetchedPage,
    revision: Option<RevisionContext<'_>>,
) -> Result<Value> {
    match normalize_page(request, &page, revision) {
        Err(error) if error.category == "schema" => Err(error.with_artifact(
            crate::request::schema_artifact(request, page.raw, page.observed_at),
        )),
        result => result,
    }
}

fn normalize_page(
    request: &CollectionRequest,
    page: &FetchedPage,
    revision: Option<RevisionContext<'_>>,
) -> Result<Value> {
    let data = page
        .raw
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| CollectorError::new("schema", "response_data_missing"))?;
    let record_key = match request.source {
        Source::Display => "videos",
        Source::Ads => "ads",
        Source::Commercial => "commercial_contents",
    };
    let source_records = data
        .get(record_key)
        .and_then(Value::as_array)
        .ok_or_else(|| CollectorError::new("schema", "response_records_missing"))?;
    if source_records.len() > 1000 {
        return Err(CollectorError::new("schema", "record_limit_exceeded"));
    }
    let next_payload = continuation(request, data)?;
    let mut records = Vec::with_capacity(source_records.len());
    let mut quarantine = Vec::new();
    let mut identifiers = HashSet::new();
    let mut unmapped_bytes = 0;
    for row in source_records {
        let record = normalize_row(request, row, page.observed_at, revision);
        match record {
            Ok(record) => {
                let fields = &record["attributes"]["unmapped_source_fields"];
                let report_bytes =
                    if revision.is_some() && fields.as_array().is_some_and(|v| !v.is_empty()) {
                        fields.to_string().len()
                    } else {
                        0
                    };
                // Keep two copies of schema-path metadata (record + field report)
                // bounded independently of nesting amplification in the raw page.
                if unmapped_bytes + report_bytes > 2 * 1024 * 1024 {
                    quarantine
                        .push(json!({"record": row, "reason": "unmapped_source_fields_limit"}));
                    continue;
                }
                let id = record["observation_id"].as_str().unwrap_or("").to_string();
                if identifiers.insert(id.clone()) {
                    if report_bytes > 0 {
                        quarantine.push(json!({
                            "reason": "unmapped_source_fields", "observation_id": id,
                            "fields": fields,
                        }));
                        unmapped_bytes += report_bytes;
                    }
                    records.push(record);
                } else {
                    quarantine.push(json!({"record": row, "reason": "duplicate_entity_in_page"}));
                }
            }
            Err(reason) => quarantine.push(json!({"record": row, "reason": reason.code})),
        }
    }
    Ok(json!({
        "source": request.source,
        "account_id": request.account_id,
        "observed_at": page.observed_at,
        "synthetic": request.synthetic,
        "raw": page.raw,
        "records": records,
        "next_payload": next_payload,
        "quarantine": quarantine,
        "request_count": page.request_count,
        "normalizer_version": if revision.is_some() { "2" } else { "1" }
    }))
}

fn normalize_row(
    request: &CollectionRequest,
    row: &Value,
    observed_at: DateTime<Utc>,
    revision: Option<RevisionContext<'_>>,
) -> Result<Value> {
    if revision.is_some() {
        validate_nested_shapes(request.source, row)?;
    }
    let mut record = match request.source {
        Source::Display => video(request, row, observed_at)?,
        Source::Ads => ad(request, row, observed_at)?,
        Source::Commercial => commercial(request, row, observed_at)?,
    };
    if let Some(context) = revision {
        let paths = unmapped_source_fields(request.source, row)?;
        let previous = record["observation_id"].as_str().unwrap_or("").to_owned();
        let versioned = format!("normalizer:2:{}:{previous}", context.source_dataset_id);
        record["observation_id"] = json!(format!("{:x}", Sha256::digest(versioned.as_bytes())));
        record["available_at"] = json!(context.available_at);
        record["attributes"]["normalization"] = json!({
            "version": "2", "replaces_observation_id": previous,
            "source_dataset_id": context.source_dataset_id,
        });
        record["attributes"]["unmapped_source_fields"] = json!(paths);
    }
    Ok(record)
}

fn validate_nested_shapes(source: Source, row: &Value) -> Result<()> {
    let object = |value: Option<&Value>, code| match value {
        None | Some(Value::Null) | Some(Value::Object(_)) => Ok(()),
        _ => Err(CollectorError::new("schema", code)),
    };
    match source {
        Source::Ads => {
            object(row.get("ad"), "invalid_ad_object")?;
            object(row.get("advertiser"), "invalid_advertiser_object")?;
            object(
                row.get("ad").and_then(|ad| ad.get("reach")),
                "invalid_reach_object",
            )?;
        }
        Source::Commercial => {
            object(row.get("creator"), "invalid_creator_object")?;
            if row
                .get("creator")
                .and_then(|creator| creator.get("username"))
                .is_some_and(|name| !name.is_null() && !name.is_string())
            {
                return Err(CollectorError::new("schema", "invalid_creator_username"));
            }
        }
        Source::Display => {}
    }
    Ok(())
}

struct UnmappedFields {
    paths: BTreeSet<String>,
    bytes: usize,
}

impl UnmappedFields {
    fn add(&mut self, path: String) -> Result<()> {
        if !self.paths.contains(&path) {
            if self.paths.len() >= 1024 || self.bytes + path.len() > 64 * 1024 {
                return Err(CollectorError::new(
                    "schema",
                    "unmapped_source_fields_limit",
                ));
            }
            self.bytes += path.len();
            self.paths.insert(path);
        }
        Ok(())
    }

    fn unknown(&mut self, value: &Value, path: &str) -> Result<()> {
        match value {
            Value::Object(fields) if !fields.is_empty() => {
                for (name, child) in fields {
                    self.unknown(child, &format!("{path}.{name}"))?;
                }
            }
            Value::Array(values) if !values.is_empty() => {
                for child in values {
                    self.unknown(child, &format!("{path}[]"))?;
                }
            }
            _ => self.add(path.to_owned())?,
        }
        Ok(())
    }

    fn inspect(&mut self, value: &Value, prefix: &str, known: &[&str]) -> Result<()> {
        if let Some(fields) = value.as_object() {
            for (name, child) in fields {
                if !known.contains(&name.as_str()) {
                    let path = if prefix.is_empty() {
                        name.to_owned()
                    } else {
                        format!("{prefix}.{name}")
                    };
                    self.unknown(child, &path)?;
                }
            }
        }
        Ok(())
    }
}

fn unmapped_source_fields(source: Source, row: &Value) -> Result<BTreeSet<String>> {
    let mut fields = UnmappedFields {
        paths: BTreeSet::new(),
        bytes: 0,
    };
    match source {
        Source::Display => fields.inspect(
            row,
            "",
            &[
                "id",
                "create_time",
                "view_count",
                "like_count",
                "comment_count",
                "share_count",
                "title",
                "video_description",
                "duration",
                "share_url",
            ],
        )?,
        Source::Ads => {
            fields.inspect(row, "", &["ad", "advertiser"])?;
            if let Some(ad) = row.get("ad") {
                fields.inspect(
                    ad,
                    "ad",
                    &[
                        "id",
                        "first_shown_date",
                        "last_shown_date",
                        "status",
                        "videos",
                        "image_urls",
                        "reach",
                    ],
                )?;
                if let Some(reach) = ad.get("reach") {
                    let consumed = if reach.get("unique_users_seen").is_some() {
                        "unique_users_seen"
                    } else {
                        "unique_user_seen"
                    };
                    fields.inspect(reach, "ad.reach", &[consumed])?;
                }
            }
            if let Some(advertiser) = row.get("advertiser") {
                let consumed_id = if advertiser.get("business_id").is_some() {
                    "business_id"
                } else {
                    "buisness_id"
                };
                fields.inspect(
                    advertiser,
                    "advertiser",
                    &[consumed_id, "business_name", "paid_by", "paid_for_by"],
                )?;
            }
        }
        Source::Commercial => {
            fields.inspect(
                row,
                "",
                &[
                    "id",
                    "create_timestamp",
                    "create_date",
                    "label",
                    "brand_names",
                    "videos",
                    "video",
                    "creator",
                ],
            )?;
            if let Some(creator) = row.get("creator") {
                fields.inspect(creator, "creator", &["username"])?;
            }
        }
    }
    Ok(fields.paths)
}

fn continuation(request: &CollectionRequest, data: &Map<String, Value>) -> Result<Value> {
    if matches!(
        (request.source, request.operation),
        (Source::Display, Operation::Query)
    ) {
        return Ok(Value::Null);
    }
    let has_more = match data.get("has_more") {
        Some(Value::Bool(value)) => *value,
        Some(Value::String(value)) if value == "true" => true,
        Some(Value::String(value)) if value == "false" => false,
        _ => return Err(CollectorError::new("schema", "invalid_has_more")),
    };
    if !has_more {
        return Ok(Value::Null);
    }
    let mut next = request
        .body()?
        .as_object()
        .cloned()
        .ok_or_else(|| CollectorError::new("configuration", "payload_must_be_object"))?;
    match request.source {
        Source::Display => {
            let cursor = data
                .get("cursor")
                .filter(|v| v.is_i64() || v.is_u64())
                .ok_or_else(|| CollectorError::new("schema", "pagination_cursor_missing"))?;
            if request.payload.get("cursor") == Some(cursor) {
                return Err(CollectorError::new("schema", "pagination_not_advancing"));
            }
            next.insert("cursor".into(), cursor.clone());
        }
        _ => {
            let search_id = data
                .get("search_id")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| CollectorError::new("schema", "pagination_search_id_missing"))?;
            next.insert("search_id".into(), json!(search_id));
        }
    }
    Ok(Value::Object(next))
}

fn identifier(value: Option<&Value>) -> Result<String> {
    let value = value.ok_or_else(|| CollectorError::new("schema", "entity_id_missing"))?;
    let id = if let Some(text) = value.as_str() {
        text.to_string()
    } else if let Some(number) = value.as_u64() {
        number.to_string()
    } else {
        return Err(CollectorError::new("schema", "invalid_entity_id"));
    };
    if id.is_empty() || id.len() > 256 {
        return Err(CollectorError::new("schema", "invalid_entity_id"));
    }
    Ok(id)
}

fn base(request: &CollectionRequest, kind: &str, id: String, at: DateTime<Utc>) -> Value {
    let stable = format!("{}:{kind}:{id}:{}", request.source.name(), at.to_rfc3339());
    let observation_id = format!("{:x}", Sha256::digest(stable.as_bytes()));
    json!({
        "schema_version": "1",
        "observation_id": observation_id,
        "source": request.source,
        "entity_type": kind,
        "entity_id": id,
        "observed_at": at,
        "available_at": at,
        "synthetic": request.synthetic,
        "published_at": null,
        "market": null,
        "account_id": request.account_id,
        "metrics": {},
        "attributes": {
            "required_capability": "collect",
            "source_scope": match request.source {
                Source::Display => "video.list",
                _ => "research.adlib.basic"
            }
        },
        "missing_fields": {}
    })
}

fn set_timestamp(record: &mut Value, value: Option<&Value>, at: DateTime<Utc>) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    let seconds = value
        .as_i64()
        .ok_or_else(|| CollectorError::new("schema", "invalid_publication_time"))?;
    let published = DateTime::<Utc>::from_timestamp(seconds, 0)
        .ok_or_else(|| CollectorError::new("schema", "invalid_publication_time"))?;
    if published > at {
        return Err(CollectorError::new(
            "schema",
            "publication_after_collection",
        ));
    }
    record["published_at"] = json!(published);
    Ok(())
}

fn video(request: &CollectionRequest, row: &Value, at: DateTime<Utc>) -> Result<Value> {
    let mut record = base(request, "video", identifier(row.get("id"))?, at);
    set_timestamp(&mut record, row.get("create_time"), at)?;
    for key in ["view_count", "like_count", "comment_count", "share_count"] {
        match row.get(key) {
            None | Some(Value::Null) => {
                record["missing_fields"][key] = json!("not_returned");
            }
            Some(value) => {
                let count = value
                    .as_u64()
                    .filter(|value| *value <= i64::MAX as u64)
                    .ok_or_else(|| CollectorError::new("schema", "invalid_counter"))?;
                record["metrics"][key] = measurement(count, count, "exact", None);
            }
        }
    }
    let attributes = record["attributes"]
        .as_object_mut()
        .ok_or_else(|| CollectorError::new("schema", "invalid_attributes"))?;
    attributes.insert("creator_id".into(), json!(request.account_id));
    attributes.insert("distribution".into(), json!("authorized_account_video"));
    for key in ["title", "video_description", "duration", "share_url"] {
        if let Some(value) = row.get(key) {
            attributes.insert(key.to_string(), value.clone());
        }
    }
    for key in [
        "units_sold",
        "reach",
        "retention",
        "completion_rate",
        "swipe_away_rate",
    ] {
        record["missing_fields"][key] = json!("not_exposed_by_source");
    }
    Ok(record)
}

fn ad(request: &CollectionRequest, row: &Value, at: DateTime<Utc>) -> Result<Value> {
    let ad = row
        .get("ad")
        .filter(|value| value.is_object())
        .ok_or_else(|| CollectorError::new("schema", "ad_object_missing"))?;
    let mut record = base(request, "ad", identifier(ad.get("id"))?, at);
    record["attributes"]["distribution"] = json!("advertisement");
    for key in [
        "first_shown_date",
        "last_shown_date",
        "status",
        "videos",
        "image_urls",
    ] {
        if let Some(value) = ad.get(key) {
            record["attributes"][key] = value.clone();
        }
    }
    if let Some(advertiser) = row.get("advertiser") {
        let id = advertiser
            .get("business_id")
            .or_else(|| advertiser.get("buisness_id"));
        if let Some(value) = id {
            record["attributes"]["advertiser_id"] = json!(identifier(Some(value))?);
        }
        for key in ["business_name", "paid_by", "paid_for_by"] {
            if let Some(value) = advertiser.get(key) {
                record["attributes"][key] = value.clone();
            }
        }
    }
    let reach = ad.get("reach").and_then(|value| {
        value
            .get("unique_users_seen")
            .or_else(|| value.get("unique_user_seen"))
    });
    if let Some(reach) = reach {
        record["attributes"]["reach_raw"] = reach.clone();
        if let Some((lower, upper, precision)) = parse_reach(reach) {
            record["metrics"]["reach"] = measurement(lower, upper, precision, reach.as_str());
        } else {
            record["missing_fields"]["reach"] = json!("rounding_bounds_not_documented");
        }
    } else {
        record["missing_fields"]["reach"] = json!("not_returned");
    }
    for key in [
        "view_count",
        "like_count",
        "comment_count",
        "share_count",
        "units_sold",
        "retention",
        "completion_rate",
        "swipe_away_rate",
    ] {
        record["missing_fields"][key] = json!("not_exposed_by_source");
    }
    Ok(record)
}

fn commercial(request: &CollectionRequest, row: &Value, at: DateTime<Utc>) -> Result<Value> {
    let mut record = base(
        request,
        "commercial_content",
        identifier(row.get("id"))?,
        at,
    );
    set_timestamp(&mut record, row.get("create_timestamp"), at)?;
    record["attributes"]["distribution"] = json!("commercial_content");
    for key in ["create_date", "label", "brand_names", "videos", "video"] {
        if let Some(value) = row.get(key) {
            record["attributes"][key] = value.clone();
        }
    }
    if let Some(creator) = row.get("creator") {
        if let Some(username) = creator.get("username").and_then(Value::as_str) {
            record["attributes"]["creator_id"] = json!(username);
        }
    }
    for key in [
        "view_count",
        "like_count",
        "comment_count",
        "share_count",
        "units_sold",
        "reach",
        "retention",
        "completion_rate",
        "swipe_away_rate",
    ] {
        record["missing_fields"][key] = json!("not_exposed_by_source");
    }
    Ok(record)
}

fn measurement(lower: u64, upper: u64, precision: &str, raw: Option<&str>) -> Value {
    json!({"lower": lower, "upper": upper, "precision": precision,
           "semantics": "cumulative", "unit": "count", "raw": raw})
}

fn parse_reach(value: &Value) -> Option<(u64, u64, &'static str)> {
    if let Some(count) = value.as_u64().filter(|value| *value <= i64::MAX as u64) {
        return Some((count, count, "exact"));
    }
    let text = value.as_str()?.trim();
    if let Ok(count) = text.parse::<u64>() {
        if count <= i64::MAX as u64 {
            return Some((count, count, "exact"));
        }
    }
    if let Some((lower, upper)) = text.split_once('-') {
        let lower = lower.trim().parse::<u64>().ok()?;
        let upper = upper.trim().parse::<u64>().ok()?;
        if lower <= upper && upper <= i64::MAX as u64 {
            return Some((lower, upper, "bounded"));
        }
    }
    // Compact rounded strings such as "11K" have no documented rounding rule.
    // Preserve them as raw attributes instead of inventing numeric bounds.
    None
}

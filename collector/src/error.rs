use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Serialize)]
pub struct CollectorError {
    pub category: &'static str,
    pub code: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<Value>,
}

impl CollectorError {
    pub fn new(category: &'static str, code: &'static str) -> Self {
        Self {
            category,
            code,
            retry_after_seconds: None,
            artifact: None,
        }
    }

    pub fn retry(category: &'static str, code: &'static str, seconds: f64) -> Self {
        Self {
            category,
            code,
            retry_after_seconds: Some(seconds.max(0.0)),
            artifact: None,
        }
    }

    pub fn with_artifact(mut self, artifact: Value) -> Self {
        self.artifact = Some(artifact);
        self
    }
}

pub type Result<T> = std::result::Result<T, CollectorError>;

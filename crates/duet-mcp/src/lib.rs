//! Boilerplate for the "MCP tool that proxies to a local HTTP service" pattern.
//!
//! An MCP server built on [`rmcp`](https://docs.rs/rmcp) usually needs the same
//! three things before it can define tools: install a rustls crypto provider,
//! build an HTTP client that trusts a private CA, and forward calls to a
//! backend. [`BusClient`] bundles those so the server crate is just its tools.

use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};

/// Install the process-default rustls crypto provider (ring). Idempotent — safe
/// to call more than once; extra calls are ignored.
pub fn install_crypto() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
}

/// Build a `reqwest` client that trusts the PEM CA at `ca_path` (and only that
/// CA — no bundled roots), for talking to a self-signed local service.
pub fn http_client_with_ca(ca_path: &Path) -> anyhow::Result<reqwest::Client> {
    let ca = std::fs::read(ca_path)?;
    let cert = reqwest::Certificate::from_pem(&ca)?;
    Ok(reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()?)
}

/// A thin async client for a `duet-bus` broker.
#[derive(Clone)]
pub struct BusClient {
    http: reqwest::Client,
    base: String,
}

impl BusClient {
    /// Connect to the broker at `base` (e.g. `https://localhost:9443`), trusting
    /// the CA at `ca_path`. Installs the crypto provider as a side effect.
    pub fn new(base: impl Into<String>, ca_path: &Path) -> anyhow::Result<Self> {
        install_crypto();
        Ok(Self {
            http: http_client_with_ca(ca_path)?,
            base: base.into(),
        })
    }

    /// Enqueue `payload` for recipient `to`.
    pub async fn send(&self, to: &str, payload: Value) -> anyhow::Result<()> {
        self.http
            .post(format!("{}/send", self.base))
            .json(&json!({ "to": to, "payload": payload }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Long-poll for one message addressed to `me`. Returns the raw broker
    /// response (`{"status":"message","envelope":{...}}` or `{"status":"timeout"}`).
    pub async fn recv(&self, me: &str, timeout_ms: u64) -> anyhow::Result<Value> {
        Ok(self
            .http
            .get(format!("{}/recv", self.base))
            .query(&[("me", me), ("timeout_ms", &timeout_ms.to_string())])
            .timeout(Duration::from_millis(timeout_ms + 2_000))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

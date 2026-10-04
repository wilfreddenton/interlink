//! Delivery to an existing local Codex CLI thread through its shared daemon.

use std::fmt;
use std::io::ErrorKind;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

pub fn validate_thread_id(id: &str) -> Result<()> {
    let valid = id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        });
    if !valid {
        bail!("expected the current Codex thread UUID, not a session name or prefix");
    }
    Ok(())
}

/// A separate metadata connection never resumes or subscribes to the owning
/// thread, so keeping it open cannot keep that conversation artificially alive.
pub struct TitleReader {
    _child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    next_id: u64,
}

impl TitleReader {
    pub async fn connect(executable: &Path) -> Result<Self> {
        let mut child = Command::new(executable)
            .args(["app-server", "--stdio"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().context("missing metadata stdin")?;
        let output = BufReader::new(child.stdout.take().context("missing metadata stdout")?);
        let mut reader = Self {
            _child: child,
            input,
            output,
            next_id: 0,
        };
        reader.request("initialize", json!({"clientInfo":{"name":"interlink-title-reader","version":env!("CARGO_PKG_VERSION")}})).await?;
        reader
            .send(json!({"jsonrpc":"2.0","method":"initialized"}))
            .await?;
        Ok(reader)
    }

    pub async fn read_title(&mut self, thread_id: &str) -> Result<Option<String>> {
        validate_thread_id(thread_id)?;
        let response = self
            .request(
                "thread/read",
                json!({"threadId":thread_id,"includeTurns":false}),
            )
            .await?;
        let thread = &response["thread"];
        if thread["id"].as_str() != Some(thread_id) {
            bail!("metadata returned a different thread");
        }
        match thread.get("name") {
            Some(Value::String(name)) => Ok(Some(name.clone())),
            Some(Value::Null) => Ok(None),
            // Missing fields on older hosts must not erase a previously known title.
            _ => bail!("host did not provide title metadata"),
        }
    }

    async fn send(&mut self, message: Value) -> Result<()> {
        self.input
            .write_all(format!("{message}\n").as_bytes())
            .await?;
        self.input.flush().await?;
        Ok(())
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .await?;
        loop {
            let mut line = Vec::new();
            (&mut self.output)
                .take(1024 * 1024)
                .read_until(b'\n', &mut line)
                .await?;
            if line.last() != Some(&b'\n') {
                bail!("metadata response missing or too large");
            }
            let message: Value = serde_json::from_slice(&line)?;
            if message["id"] == id {
                if message.get("error").is_some() {
                    bail!("Codex metadata request failed");
                }
                return message
                    .get("result")
                    .cloned()
                    .context("missing metadata result");
            }
        }
    }
}

// Keep enough space for platform quoting, executable paths, and the fixed arguments.
pub const MAX_QUEUE_BYTES: usize = 12 * 1024;

#[derive(Debug)]
pub struct DeliveryError {
    pub retryable: bool,
    message: String,
}

impl DeliveryError {
    fn new(retryable: bool, message: impl Into<String>) -> Self {
        Self {
            retryable,
            message: message.into(),
        }
    }
}

impl fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for DeliveryError {}

pub async fn deliver(executable: &Path, thread_id: &str, text: &str) -> Result<(), DeliveryError> {
    validate_thread_id(thread_id).map_err(|e| DeliveryError::new(false, e.to_string()))?;
    if text.len() > MAX_QUEUE_BYTES || text.contains('\0') {
        return Err(DeliveryError::new(
            false,
            "message exceeds Interlink's 12 KiB Codex delivery limit or contains a NUL; recover it with failed_deliveries",
        ));
    }
    let status = timeout(
        Duration::from_secs(30),
        Command::new(executable)
            .arg("queue").arg("--thread").arg(thread_id).arg("--message").arg(text)
            // CLI diagnostics may echo message contents; keep them out of logs and MCP stdout.
            .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
            .kill_on_drop(true).status(),
    ).await.map_err(|_| DeliveryError::new(false, "Codex queue timed out; delivery outcome is unknown. A manual retry may duplicate the message"))?
        .map_err(|e| DeliveryError::new(matches!(e.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock), format!("starting codex queue: {e}")))?;
    if !status.success() {
        return Err(DeliveryError::new(
            true,
            format!("codex queue failed with {status}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_full_thread_ids() {
        assert!(validate_thread_id("01900000-1234-7000-8000-123456789abc").is_ok());
        for id in [
            "",
            "main",
            "01900000",
            "../thread",
            "01900000-1234-7000-8000-123456789abg",
        ] {
            assert!(validate_thread_id(id).is_err(), "accepted {id}");
        }
    }

    #[tokio::test]
    async fn missing_cli_is_a_delivery_failure() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            deliver(
                &dir.path().join("missing"),
                "01900000-1234-7000-8000-123456789abc",
                "hello"
            )
            .await
            .is_err()
        );
    }
}

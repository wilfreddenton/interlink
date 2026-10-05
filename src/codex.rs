//! Delivery to an existing local Codex CLI thread through its shared daemon.

use std::fmt;
use std::io::ErrorKind;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Result, bail};
use tokio::process::Command;
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

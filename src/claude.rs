//! Local wake-up delivery into the owning Claude Code session.

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::json;
use tokio::io::{AsyncWrite, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(windows)]
use tokio::net::windows::named_pipe::ClientOptions;
use tokio::time::timeout;

pub struct ClaudeSocket {
    path: PathBuf,
    token: String,
}

impl ClaudeSocket {
    pub fn from_env() -> Result<Option<Self>> {
        Self::select(
            &env::var("INTERLINK_CLAUDE_DELIVERY").unwrap_or_else(|_| "auto".into()),
            env::var_os("CLAUDE_CODE_MESSAGING_SOCKET").map(PathBuf::from),
            env::var("CLAUDE_CODE_MESSAGING_TOKEN").ok(),
        )
    }

    fn select(mode: &str, path: Option<PathBuf>, token: Option<String>) -> Result<Option<Self>> {
        if mode == "inbox" {
            return Ok(None);
        }
        if !matches!(mode, "auto" | "socket") {
            bail!("INTERLINK_CLAUDE_DELIVERY must be auto, socket, or inbox");
        }
        let path = path.filter(|path| !path.as_os_str().is_empty());
        let Some(path) = path else {
            if mode == "socket" {
                bail!("socket delivery requires CLAUDE_CODE_MESSAGING_SOCKET from the owning host");
            }
            return Ok(None);
        };
        if !path.is_absolute() {
            bail!("Claude messaging socket must be an absolute local path");
        }
        #[cfg(windows)]
        if !path.to_string_lossy().starts_with(r"\\.\pipe\") {
            bail!("Claude messaging socket must be a local Windows named pipe");
        }
        let token = token.filter(|token| !token.is_empty()).context(
            "socket delivery requires CLAUDE_CODE_MESSAGING_TOKEN from the owning host; restart Claude or explicitly select INTERLINK_CLAUDE_DELIVERY=inbox",
        )?;
        Ok(Some(Self { path, token }))
    }

    pub async fn deliver(&self, text: &str) -> Result<()> {
        let frames = format!(
            "{}\n{}\n",
            json!({"type": "auth", "token": self.token}),
            json!({"type": "user", "message": {"role": "user", "content": text}}),
        );
        // Neither a successful write nor EOF is a host/read receipt. Keep the
        // durable mailbox until its normal fetch and acknowledgement flow completes.
        timeout(Duration::from_secs(3), async {
            #[cfg(unix)]
            let mut stream = UnixStream::connect(&self.path).await?;
            #[cfg(windows)]
            let mut stream = ClientOptions::new().open(&self.path)?;
            write_frames(&mut stream, frames.as_bytes()).await
        })
        .await
        .context("Claude socket handoff timed out; delivery outcome is unknown")?
        .context("Claude socket handoff failed")
    }
}

async fn write_frames(
    stream: &mut (impl AsyncWrite + Unpin),
    frames: &[u8],
) -> std::io::Result<()> {
    stream.write_all(frames).await?;
    stream.flush().await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use serde_json::Value;
    #[cfg(unix)]
    use tokio::io::{AsyncBufReadExt, BufReader};
    #[cfg(unix)]
    use tokio::net::UnixListener;

    #[test]
    fn selection_never_silently_falls_back_from_a_configured_socket() {
        assert!(ClaudeSocket::select("auto", None, None).unwrap().is_none());
        assert!(ClaudeSocket::select("socket", None, None).is_err());
        assert!(ClaudeSocket::select("invalid", None, None).is_err());
        let path = env::temp_dir().join("claude.sock");
        assert!(ClaudeSocket::select("auto", Some(path.clone()), None).is_err());
        assert!(
            ClaudeSocket::select("inbox", Some(path), None)
                .unwrap()
                .is_none()
        );
        assert!(
            ClaudeSocket::select("socket", Some("relative".into()), Some("secret".into())).is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sends_authenticated_frames_and_reconnects_after_socket_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("claude.sock");
        let client = ClaudeSocket::select("socket", Some(path.clone()), Some("test-secret".into()))
            .unwrap()
            .unwrap();
        assert!(client.deliver("not connected").await.is_err());
        for _ in 0..2 {
            let listener = UnixListener::bind(&path).unwrap();
            let read = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut lines = BufReader::new(stream).lines();
                let auth: Value =
                    serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
                let message: Value =
                    serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
                assert_eq!(auth, json!({"type":"auth","token":"test-secret"}));
                assert_eq!(message["message"]["content"], "quotes \" and\nnewlines");
                assert_eq!(message["type"], "user");
                assert!(lines.next_line().await.unwrap().is_none());
            });
            client.deliver("quotes \" and\nnewlines").await.unwrap();
            timeout(Duration::from_secs(3), read)
                .await
                .unwrap()
                .unwrap();
            std::fs::remove_file(&path).unwrap();
        }
    }
}

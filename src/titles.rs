//! Local title metadata shared by host hooks and the session's MCP process.

use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::state::{atomic_write, lock};

const MAX_STATE_BYTES: u64 = 4096;

pub fn normalize_title(title: &str) -> Result<String> {
    let title = title.trim();
    if title.len() > 256 || title.chars().any(char::is_control) {
        bail!("title must be at most 256 UTF-8 bytes and contain no control characters");
    }
    Ok(title.to_string())
}

// Native titles and paths need not obey Interlink's input limits. Normalize them
// for display without rejecting an otherwise usable host session.
pub fn display_title(title: &str) -> String {
    let mut out = String::new();
    for c in title.trim().chars() {
        let c = if c.is_control() { ' ' } else { c };
        if out.len() + c.len_utf8() > 256 {
            break;
        }
        out.push(c);
    }
    out.trim().to_string()
}

pub fn fallback_title(project: &str, machine: &str, host: &str, session: &str) -> String {
    let short: String = session
        .chars()
        .rev()
        .take(8)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let context = display_title(project);
    // Leave room for the discriminator even with a very long project name.
    let context: String = context.chars().take(32).collect();
    let machine: String = display_title(machine).chars().take(12).collect();
    display_title(&format!("{context} · {machine} · {host} · {short}"))
}

#[derive(Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TitleState {
    pub native: Option<String>,
    // Some("") explicitly clears even a startup override on subsequent restarts.
    pub explicit: Option<String>,
}

impl TitleState {
    pub fn resolve(&self, startup: &str, fallback: &str) -> String {
        let explicit = self.explicit.as_deref().unwrap_or(startup);
        if !explicit.is_empty() {
            explicit.to_string()
        } else {
            self.native
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or(fallback)
                .to_string()
        }
    }
}

pub struct TitleStore {
    path: PathBuf,
}

impl TitleStore {
    pub fn new(root: &Path, host: &str, session: &str) -> Result<Self> {
        if !matches!(host, "Claude" | "Codex") || session.is_empty() || session.len() > 128 {
            bail!("invalid host or session for title metadata");
        }
        Ok(Self {
            path: root
                .join("titles")
                .join(host)
                .join(format!("{}.json", URL_SAFE_NO_PAD.encode(session))),
        })
    }

    pub fn read(&self) -> Result<TitleState> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(TitleState::default()),
            Err(e) => return Err(e.into()),
        };
        let mut bytes = Vec::new();
        file.take(MAX_STATE_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_STATE_BYTES {
            bail!("title metadata exceeds size limit");
        }
        let state: TitleState = serde_json::from_slice(&bytes)?;
        for title in [&state.native, &state.explicit].into_iter().flatten() {
            normalize_title(title)?;
        }
        Ok(state)
    }

    pub fn set_native(&self, title: Option<&str>) -> Result<TitleState> {
        self.update(|state| state.native = title.map(display_title).filter(|s| !s.is_empty()))
    }

    pub fn set_explicit(&self, title: &str) -> Result<TitleState> {
        let title = normalize_title(title)?;
        self.update(|state| state.explicit = Some(title))
    }

    fn update(&self, change: impl FnOnce(&mut TitleState)) -> Result<TitleState> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut state = self.read()?;
        let before = serde_json::to_vec(&state)?;
        change(&mut state);
        let after = serde_json::to_vec(&state)?;
        if after != before {
            atomic_write(&self.path, &after)?;
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn overrides_and_native_names_survive_restart_without_crossing_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let store = TitleStore::new(dir.path(), "Claude", "../session").unwrap();
        store.set_native(Some("Native")).unwrap();
        store.set_explicit("Pinned").unwrap();
        store.set_native(Some("Renamed")).unwrap();
        let reopened = TitleStore::new(dir.path(), "Claude", "../session").unwrap();
        assert_eq!(
            reopened.read().unwrap().resolve("Configured", "Fallback"),
            "Pinned"
        );
        assert_eq!(
            reopened
                .set_explicit("")
                .unwrap()
                .resolve("Configured", "Fallback"),
            "Renamed"
        );
        assert_eq!(
            reopened
                .set_native(None)
                .unwrap()
                .resolve("Configured", "Fallback"),
            "Fallback"
        );
        for (host, sid) in [("Codex", "../session"), ("Claude", "sibling")] {
            assert_eq!(
                TitleStore::new(dir.path(), host, sid)
                    .unwrap()
                    .read()
                    .unwrap()
                    .resolve("", "Other"),
                "Other"
            );
        }
    }

    #[test]
    fn concurrent_hook_and_override_updates_merge() {
        let dir = tempfile::tempdir().unwrap();
        thread::scope(|s| {
            s.spawn(|| {
                TitleStore::new(dir.path(), "Claude", "sid")
                    .unwrap()
                    .set_native(Some("Host"))
                    .unwrap()
            });
            s.spawn(|| {
                TitleStore::new(dir.path(), "Claude", "sid")
                    .unwrap()
                    .set_explicit("Manual")
                    .unwrap()
            });
        });
        let state = TitleStore::new(dir.path(), "Claude", "sid")
            .unwrap()
            .read()
            .unwrap();
        assert_eq!(state.native.as_deref(), Some("Host"));
        assert_eq!(state.explicit.as_deref(), Some("Manual"));
    }

    #[test]
    fn invalid_updates_and_write_failures_preserve_saved_names() {
        let dir = tempfile::tempdir().unwrap();
        let store = TitleStore::new(dir.path(), "Claude", "sid").unwrap();
        store.set_native(Some("Known title")).unwrap();
        assert!(store.set_explicit("bad\ntitle").is_err());
        assert_eq!(store.read().unwrap().resolve("", "Fallback"), "Known title");
        std::fs::remove_file(store.path.with_extension("lock")).unwrap();
        std::fs::create_dir(store.path.with_extension("lock")).unwrap();
        assert!(store.set_native(Some("Lost update")).is_err());
        assert_eq!(store.read().unwrap().resolve("", "Fallback"), "Known title");
        std::fs::write(&store.path, b"broken").unwrap();
        assert!(
            store.read().is_err(),
            "corrupt metadata must not be treated as a title clear"
        );
    }

    #[test]
    fn display_names_are_bounded_and_keep_fallback_discriminators() {
        let title = display_title(&format!("line\n{}", "é".repeat(300)));
        assert!(title.len() <= 256);
        assert!(!title.contains('\n'));
        let fallback = fallback_title(
            &"😀".repeat(300),
            &"😀".repeat(100),
            "Codex",
            "01900000-1234-7000-8000-123456789abc",
        );
        assert!(fallback.ends_with("56789abc"));
        assert!(fallback.len() <= 256);
    }
}

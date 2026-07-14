//! `interlinked` — launch Claude Code with native interlink channels enabled.
//!
//! The default way to use interlink is plain `claude`: the plugin runs in the
//! channel-less fallback mode (a background `interlink-mcp wait` listener + a Stop
//! hook), which needs no special flags and works even where channels are blocked.
//!
//! This launcher is only for the *channel* path — when you have Claude Code
//! development channels available and want the nicer native push. It sets
//! `INTERLINK_CHANNELS=1` (so the MCP server pushes instead of writing to the inbox,
//! and the Stop hook self-disables) and starts Claude with the channel flag. Extra
//! arguments are forwarded to `claude`.

use std::process::Command;

const PLUGIN_CHANNEL: &str = "plugin:interlink@interlink";

fn main() {
    let passthrough: Vec<String> = std::env::args().skip(1).collect();

    // A light preflight — an absent key is the usual "why is nothing working";
    // warn but still launch, since claude surfaces the MCP error too.
    if let Some(key) = key_path() {
        if !key.exists() {
            eprintln!(
                "interlinked: warning: no key at {} — run interlink-keygen first",
                key.display()
            );
        }
    }

    let mut cmd = Command::new("claude");
    cmd.env("INTERLINK_CHANNELS", "1")
        .arg("--dangerously-load-development-channels")
        .arg(PLUGIN_CHANNEL)
        .args(&passthrough);

    exec(cmd);
}

fn key_path() -> Option<std::path::PathBuf> {
    if let Some(k) = std::env::var_os("INTERLINK_KEY") {
        return Some(k.into());
    }
    std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".config/interlink/id.key"))
}

/// Replace this process with `claude` on Unix; on other platforms spawn, wait, and
/// forward the exit status.
#[cfg(unix)]
fn exec(mut cmd: Command) -> ! {
    use std::os::unix::process::CommandExt;
    let err = cmd.exec(); // only returns on failure
    eprintln!("interlinked: failed to launch claude: {err}");
    std::process::exit(127);
}

#[cfg(not(unix))]
fn exec(mut cmd: Command) -> ! {
    match cmd.status() {
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(e) => {
            eprintln!("interlinked: failed to launch claude: {e}");
            std::process::exit(127);
        }
    }
}

//! Claude Code `Stop` hook. Refuses to let an instance go idle unless a
//! background event-listener is currently armed on the bus.
//!
//! On every stop it asks the broker `GET /armed?me=<self>`. If armed, it stays
//! silent (allowing the stop). If not, it emits a `block` decision instructing
//! Claude to relaunch the listener — so an event-driven agent can never quietly
//! stop listening. Any error reaching the broker allows the stop, so a down bus
//! never traps the model in a loop.
//!
//! Config comes from flags or the matching env var (flags win):
//!   --self / DUET_SELF   this instance's id on the bus
//!   --url  / DUET_URL    broker base url (default https://localhost:9443)
//!   --ca   / DUET_CA     path to the broker's ca.pem
//!   --listen-cmd / DUET_LISTEN_CMD   override the relaunch command shown to Claude

use std::io::Read;
use std::time::Duration;

use clap::Parser;
use serde_json::{Value, json};

#[derive(Parser)]
struct Config {
    #[arg(long = "self", env = "DUET_SELF")]
    me: String,
    #[arg(long, env = "DUET_URL", default_value = "https://localhost:9443")]
    url: String,
    #[arg(long, env = "DUET_CA")]
    ca: String,
    #[arg(long, env = "DUET_LISTEN_CMD")]
    listen_cmd: Option<String>,
}

fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    // Drain the hook payload on stdin; our decision only needs config.
    let mut _buf = String::new();
    let _ = std::io::stdin().read_to_string(&mut _buf);

    // A config error is a setup mistake — allow the stop rather than trap Claude.
    let Ok(cfg) = Config::try_parse() else {
        return;
    };

    match check_armed(&cfg.url, &cfg.ca, &cfg.me) {
        Ok(true) => {} // listener live -> allow stop
        Err(_) => {}   // bus unreachable -> allow stop
        Ok(false) => println!("{}", block_decision(&cfg)),
    }
}

fn block_decision(cfg: &Config) -> Value {
    let listen_cmd = cfg.listen_cmd.clone().unwrap_or_else(|| {
        format!(
            "curl -sN --cacert {} \"{}/recv?me={}\"",
            cfg.ca, cfg.url, cfg.me
        )
    });
    let reason = format!(
        "Your event listener is not armed on the bus, so you would go idle deaf to incoming \
         messages. Before stopping, relaunch it as a background task (Bash with \
         run_in_background) exactly once:\n\n    {listen_cmd}\n\n\
         When that command later returns a JSON line with \"status\":\"message\", handle the \
         message, then relaunch the same command. On \"status\":\"timeout\", just relaunch it."
    );
    json!({ "decision": "block", "reason": reason })
}

fn check_armed(url: &str, ca_path: &str, me: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let ca_pem = std::fs::read(ca_path)?;
    let cert = reqwest::Certificate::from_pem(&ca_pem)?;
    let client = reqwest::blocking::Client::builder()
        .add_root_certificate(cert)
        .timeout(Duration::from_secs(5))
        .build()?;
    let resp: Value = client
        .get(format!("{url}/armed"))
        .query(&[("me", me)])
        .send()?
        .json()?;
    Ok(resp.get("armed").and_then(Value::as_bool).unwrap_or(false))
}

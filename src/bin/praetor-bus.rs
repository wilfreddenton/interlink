//! The bus: routes signed messages between agents, buffers for offline ones.
//!
//! Plain HTTP on loopback. There is deliberately no TLS: the traffic never
//! leaves the machine, and authenticity comes from Ed25519 signatures on the
//! messages — which, unlike TLS, survive passing through an untrusted bus. That
//! choice is also what keeps `ring` (and all C) out of the dependency tree.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use praetor::bus::Broker;
use praetor::store::Store;
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(about = "Message broker for Claude Code agents")]
struct Args {
    /// Address to listen on. Loopback unless you really mean otherwise.
    #[arg(long, env = "PRAETOR_ADDR", default_value = "127.0.0.1:9440")]
    addr: SocketAddr,
    /// Durable queue file. Omit for an in-memory bus (queues lost on restart).
    /// With a path, messages survive a restart until the recipient acks them.
    #[arg(long, env = "PRAETOR_DB")]
    db: Option<PathBuf>,
    /// Per-recipient queue cap. When full, the oldest message is dropped.
    #[arg(long, env = "PRAETOR_QUEUE_CAP", default_value_t = 1024)]
    queue_cap: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "praetor=info".into()),
        )
        .init();

    let args = Args::parse();

    // The bus has no authentication of its own: anything that can reach the
    // port may enqueue. Signatures make forgery impossible, but a non-loopback
    // bind still widens the surface for denial of service, so say so out loud.
    if !args.addr.ip().is_loopback() {
        tracing::warn!(
            addr = %args.addr,
            "bus is not bound to loopback; any host reaching this port can enqueue"
        );
    }

    let store = match &args.db {
        Some(path) => {
            tracing::info!(db = %path.display(), "durable queue");
            Store::on_disk(path)?
        }
        None => {
            tracing::warn!("no --db: in-memory queue, lost on restart");
            Store::in_memory()?
        }
    };

    let app = Broker::new(store, args.queue_cap).router();
    let listener = TcpListener::bind(args.addr).await?;
    tracing::info!(addr = %args.addr, cap = args.queue_cap, "bus listening");
    axum::serve(listener, app).await?;
    Ok(())
}

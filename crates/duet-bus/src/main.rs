//! `duet-bus` binary: serves the [`Broker`](duet_bus::Broker) router over HTTPS.
//!
//! TLS is terminated per-connection with `tokio-rustls`, then the stream is
//! handed to `hyper-util`'s connection server driving the axum router — the
//! canonical way to serve axum over rustls without the `axum-server` wrapper.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use axum::extract::Request;
use clap::Parser;
use duet_bus::Broker;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tower_service::Service;

#[derive(Parser)]
#[command(about = "duet message broker: per-recipient long-poll queue over HTTPS")]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "DUET_ADDR", default_value = "127.0.0.1:9443")]
    addr: SocketAddr,
    /// Directory holding cert.pem/key.pem/ca.pem (generated on first run).
    #[arg(long, env = "DUET_CERT_DIR", default_value = "certs")]
    cert_dir: PathBuf,
}

/// Load an existing localhost cert/key, or mint a self-signed pair. The cert is
/// also written as `ca.pem` for clients to trust.
fn ensure_cert(dir: &Path) -> anyhow::Result<(String, String)> {
    std::fs::create_dir_all(dir).context("create cert dir")?;
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    if cert_path.exists() && key_path.exists() {
        return Ok((
            std::fs::read_to_string(&cert_path)?,
            std::fs::read_to_string(&key_path)?,
        ));
    }
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    let cert_pem = cert.pem();
    let key_pem = signing_key.serialize_pem();
    std::fs::write(&cert_path, &cert_pem)?;
    std::fs::write(&key_path, &key_pem)?;
    std::fs::write(dir.join("ca.pem"), &cert_pem)?;
    tracing::info!(dir = %dir.display(), "generated self-signed cert");
    Ok((cert_pem, key_pem))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "duet_bus=info".into()),
        )
        .init();

    tokio_rustls::rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let args = Args::parse();
    let (cert_pem, key_pem) = ensure_cert(&args.cert_dir)?;

    let certs = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .context("parse certificate")?;
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).context("parse private key")?;
    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let app = Broker::new().router();
    let listener = TcpListener::bind(args.addr).await?;
    tracing::info!("duet-bus listening on https://{}", args.addr);

    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("accept error: {e}");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let tls_stream = match acceptor.accept(tcp).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!("tls handshake failed from {peer}: {e}");
                    return;
                }
            };
            let io = TokioIo::new(tls_stream);
            let service =
                hyper::service::service_fn(move |req: Request<Incoming>| app.clone().call(req));
            if let Err(e) = Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(io, service)
                .await
            {
                tracing::debug!("connection error from {peer}: {e}");
            }
        });
    }
}

//! A minimal per-recipient message queue with HTTP long-polling.
//!
//! Each recipient id owns an unbounded channel. `enqueue` never blocks; `recv`
//! long-polls until a payload arrives or the timeout elapses. The number of
//! in-flight `recv` calls is exposed as the recipient's **armed** state — the
//! signal a supervising Stop hook reads to decide whether a listener is live.
//!
//! Payloads are opaque [`serde_json::Value`], so the bus stays domain-agnostic;
//! the message schema is the caller's concern.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router, http::StatusCode};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;

/// Default long-poll timeout: chosen to sit just under common idle windows so a
/// listener re-arms periodically without a human noticing.
pub const DEFAULT_RECV_TIMEOUT_MS: u64 = 300_000;

/// A payload delivered to a recipient, tagged with the broker's receipt time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub payload: Value,
    /// Unix milliseconds, stamped by the broker when the payload was enqueued.
    pub ts: u64,
}

struct Queue {
    tx: mpsc::UnboundedSender<Envelope>,
    /// The receiver behind a mutex enforces a single consumer per recipient —
    /// there is only ever one listener per id anyway.
    rx: Mutex<mpsc::UnboundedReceiver<Envelope>>,
    waiters: AtomicUsize,
}

/// Shared broker state: one queue per recipient id. Cheap to clone (`Arc` inside).
#[derive(Clone, Default)]
pub struct Broker {
    queues: Arc<DashMap<String, Arc<Queue>>>,
}

impl Broker {
    pub fn new() -> Self {
        Self::default()
    }

    fn queue(&self, id: &str) -> Arc<Queue> {
        self.queues
            .entry(id.to_string())
            .or_insert_with(|| {
                let (tx, rx) = mpsc::unbounded_channel();
                Arc::new(Queue {
                    tx,
                    rx: Mutex::new(rx),
                    waiters: AtomicUsize::new(0),
                })
            })
            .clone()
    }

    /// Enqueue a payload for `to`. Never blocks; the payload is buffered until a
    /// `recv` drains it, so nothing is lost between listeners.
    pub fn enqueue(&self, to: &str, payload: Value) {
        let env = Envelope {
            payload,
            ts: now_ms(),
        };
        // Send only fails if the receiver was dropped, which cannot happen while
        // the queue lives in the map.
        let _ = self.queue(to).tx.send(env);
    }

    /// Whether `id` currently has an in-flight [`recv`](Self::recv) — i.e. a live
    /// listener is waiting. This is the "armed" signal.
    pub fn armed(&self, id: &str) -> bool {
        self.queues
            .get(id)
            .map(|q| q.waiters.load(Ordering::SeqCst) > 0)
            .unwrap_or(false)
    }

    /// Wait up to `wait` for the next payload addressed to `id`.
    pub async fn recv(&self, id: &str, wait: Duration) -> Option<Envelope> {
        let q = self.queue(id);
        let mut rx = q.rx.lock().await;
        q.waiters.fetch_add(1, Ordering::SeqCst);
        let out = timeout(wait, rx.recv()).await.ok().flatten();
        q.waiters.fetch_sub(1, Ordering::SeqCst);
        out
    }

    /// Build the HTTP router: `POST /send`, `GET /recv`, `GET /armed`.
    pub fn router(self) -> Router {
        Router::new()
            .route("/send", post(send))
            .route("/recv", get(recv_handler))
            .route("/armed", get(armed_handler))
            .with_state(self)
    }
}

#[derive(Deserialize)]
struct SendBody {
    to: String,
    payload: Value,
}

async fn send(State(broker): State<Broker>, Json(body): Json<SendBody>) -> StatusCode {
    tracing::info!(to = %body.to, "enqueue");
    broker.enqueue(&body.to, body.payload);
    StatusCode::ACCEPTED
}

#[derive(Deserialize)]
struct RecvQuery {
    me: String,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
}

fn default_timeout() -> u64 {
    DEFAULT_RECV_TIMEOUT_MS
}

async fn recv_handler(
    State(broker): State<Broker>,
    Query(q): Query<RecvQuery>,
) -> impl IntoResponse {
    match broker
        .recv(&q.me, Duration::from_millis(q.timeout_ms))
        .await
    {
        Some(env) => Json(json!({ "status": "message", "envelope": env })),
        None => Json(json!({ "status": "timeout" })),
    }
}

#[derive(Deserialize)]
struct ArmedQuery {
    me: String,
}

async fn armed_handler(
    State(broker): State<Broker>,
    Query(q): Query<ArmedQuery>,
) -> impl IntoResponse {
    Json(json!({ "armed": broker.armed(&q.me) }))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enqueue_then_recv_returns_payload() {
        let broker = Broker::new();
        broker.enqueue("alice", json!({ "hi": 1 }));
        let env = broker
            .recv("alice", Duration::from_millis(50))
            .await
            .expect("a buffered payload");
        assert_eq!(env.payload, json!({ "hi": 1 }));
    }

    #[tokio::test]
    async fn recv_times_out_when_empty() {
        let broker = Broker::new();
        assert!(
            broker
                .recv("nobody", Duration::from_millis(10))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn armed_reflects_inflight_recv() {
        let broker = Broker::new();
        assert!(!broker.armed("alice"));

        let b2 = broker.clone();
        let waiter =
            tokio::spawn(async move { b2.recv("alice", Duration::from_millis(200)).await });
        // Let the recv task register as a waiter.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(broker.armed("alice"));

        broker.enqueue("alice", json!("wake"));
        let got = waiter.await.unwrap().expect("delivered");
        assert_eq!(got.payload, json!("wake"));
        assert!(!broker.armed("alice"));
    }

    #[tokio::test]
    async fn payload_buffered_across_listener_gap() {
        // A payload sent while no one is listening must still be delivered to the
        // next recv — this is what makes a missed relaunch non-fatal.
        let broker = Broker::new();
        broker.enqueue("bob", json!("queued"));
        broker.enqueue("bob", json!("also"));
        let first = broker.recv("bob", Duration::from_millis(50)).await.unwrap();
        let second = broker.recv("bob", Duration::from_millis(50)).await.unwrap();
        assert_eq!(first.payload, json!("queued"));
        assert_eq!(second.payload, json!("also"));
    }
}

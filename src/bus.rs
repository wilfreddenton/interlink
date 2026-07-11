//! The broker: a durable, keep-until-acked FIFO per recipient over HTTP.
//!
//! The bus is deliberately dumb — it routes an opaque JSON payload to a
//! recipient id, never inspects it, never verifies a signature, holds no keys.
//! Messages **persist in a [`Store`] until the recipient acks them**, so a bus
//! restart doesn't lose anything queued for an offline agent. Delivery is
//! at-least-once; the recipient dedupes by `msg_id`, so a redelivered message is
//! harmless.
//!
//! Recipients are Ed25519 public keys (base64). The bus treats them as strings.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router, http::StatusCode};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

use crate::store::Store;

pub const DEFAULT_RECV_TIMEOUT_MS: u64 = 25_000;

/// A payload addressed to a recipient, stamped on arrival.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope {
    pub payload: Value,
    /// Unix milliseconds, set by the bus when the message was enqueued.
    pub ts: u64,
}

#[derive(Clone)]
pub struct Broker {
    store: Store,
    /// Per-recipient wakeups for the long-poll. In-memory and rebuildable — the
    /// durable state is entirely in the store.
    notifies: Arc<DashMap<String, Arc<Notify>>>,
    cap: usize,
}

impl Broker {
    pub fn new(store: Store, cap: usize) -> Self {
        Self {
            store,
            notifies: Arc::new(DashMap::new()),
            cap: cap.max(1),
        }
    }

    fn notify_handle(&self, id: &str) -> Arc<Notify> {
        self.notifies
            .entry(id.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Enqueue for `to`: persist, enforce the cap (drop oldest), wake a waiter.
    pub async fn enqueue(&self, to: &str, payload: Value, ts: u64) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(&Envelope { payload, ts })?;
        self.store.enqueue(to.to_string(), bytes).await?;
        // Bounded: drop oldest beyond the cap so a never-returning recipient
        // can't grow the store without limit.
        while self.store.depth(to.to_string()).await? > self.cap {
            match self.store.peek_oldest(to.to_string()).await? {
                Some((old_key, _)) => {
                    self.store.ack(old_key).await?;
                    tracing::warn!(to, cap = self.cap, "queue full; dropped oldest");
                }
                None => break,
            }
        }
        self.notify_handle(to).notify_one();
        Ok(())
    }

    /// Wait up to `wait` for the oldest un-acked message for `id`. Returns the
    /// envelope and its ack key; the message stays in the store until `ack`.
    pub async fn recv(
        &self,
        id: &str,
        wait: Duration,
    ) -> anyhow::Result<Option<(Envelope, String)>> {
        let deadline = tokio::time::Instant::now() + wait;
        let notify = self.notify_handle(id);
        loop {
            if let Some((key, bytes)) = self.store.peek_oldest(id.to_string()).await? {
                return Ok(Some((serde_json::from_slice(&bytes)?, key)));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            // Register interest before re-checking, or a message enqueued in
            // between would not wake us — the classic lost-wakeup.
            let notified = notify.notified();
            if timeout(remaining, notified).await.is_err() {
                return Ok(None);
            }
        }
    }

    pub async fn ack(&self, key: &str) -> anyhow::Result<()> {
        self.store.ack(key.to_string()).await
    }

    pub async fn depth(&self, id: &str) -> anyhow::Result<usize> {
        self.store.depth(id.to_string()).await
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/send", post(send))
            .route("/recv", get(recv_handler))
            .route("/ack", post(ack_handler))
            .with_state(self)
    }
}

#[derive(Deserialize)]
struct SendBody {
    to: String,
    payload: Value,
}

async fn send(State(broker): State<Broker>, Json(body): Json<SendBody>) -> StatusCode {
    match broker
        .enqueue(&body.to, body.payload, crate::now_ms())
        .await
    {
        Ok(()) => StatusCode::ACCEPTED,
        Err(e) => {
            tracing::error!("enqueue failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
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

async fn recv_handler(State(broker): State<Broker>, Query(q): Query<RecvQuery>) -> Response {
    match broker
        .recv(&q.me, Duration::from_millis(q.timeout_ms))
        .await
    {
        Ok(Some((env, ack))) => {
            Json(json!({ "status": "message", "envelope": env, "ack": ack })).into_response()
        }
        Ok(None) => Json(json!({ "status": "timeout" })).into_response(),
        Err(e) => {
            tracing::error!("recv failed: {e}");
            // A 5xx (not a 200 with an error body) so the client's
            // error_for_status() trips and it backs off instead of hot-looping.
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "status": "error" })),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
struct AckBody {
    me: String,
    ack: String,
}

async fn ack_handler(State(broker): State<Broker>, Json(body): Json<AckBody>) -> StatusCode {
    // The ack key encodes its recipient; only let `me` ack their own messages.
    // Weak, but consistent with the bus's threat model (loopback/tailnet, no
    // transport auth — signatures are what actually protect message integrity).
    if !body.ack.starts_with(&format!("{}\u{0}", body.me)) {
        return StatusCode::FORBIDDEN;
    }
    match broker.ack(&body.ack).await {
        Ok(()) => StatusCode::OK,
        Err(e) => {
            tracing::error!("ack failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(cap: usize) -> Broker {
        Broker::new(Store::in_memory().unwrap(), cap)
    }

    #[tokio::test]
    async fn enqueue_recv_ack_roundtrip() {
        let b = broker(8);
        b.enqueue("alice", json!({ "hi": 1 }), 5).await.unwrap();
        let (env, ack) = b
            .recv("alice", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(env.payload, json!({ "hi": 1 }));
        assert_eq!(env.ts, 5);
        // keep-until-ack: still there before ack
        assert_eq!(b.depth("alice").await.unwrap(), 1);
        b.ack(&ack).await.unwrap();
        assert_eq!(b.depth("alice").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn redelivers_until_acked() {
        let b = broker(8);
        b.enqueue("alice", json!("x"), 1).await.unwrap();
        let (_e1, ack) = b
            .recv("alice", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        // A second recv without acking sees the SAME message again.
        let (_e2, ack2) = b
            .recv("alice", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ack, ack2);
        b.ack(&ack).await.unwrap();
        assert!(
            b.recv("alice", Duration::from_millis(20))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn recv_times_out_when_empty() {
        let b = broker(8);
        assert!(
            b.recv("nobody", Duration::from_millis(10))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn fifo_order_across_acks() {
        let b = broker(8);
        for i in 0..3 {
            b.enqueue("bob", json!(i), i).await.unwrap();
        }
        for i in 0..3 {
            let (env, ack) = b
                .recv("bob", Duration::from_millis(50))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(env.payload, json!(i));
            b.ack(&ack).await.unwrap();
        }
    }

    #[tokio::test]
    async fn bounded_queue_drops_oldest() {
        let b = broker(2);
        for i in 0..4 {
            b.enqueue("bob", json!(i), i).await.unwrap();
        }
        assert_eq!(b.depth("bob").await.unwrap(), 2);
        let (env, _) = b
            .recv("bob", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(env.payload, json!(2), "0 and 1 were evicted");
    }

    #[tokio::test]
    async fn a_waiting_recv_is_woken_by_a_later_send() {
        let b = broker(8);
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.recv("alice", Duration::from_secs(2)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        b.enqueue("alice", json!("wake"), 1).await.unwrap();
        let (env, _) = waiter.await.unwrap().unwrap().unwrap();
        assert_eq!(env.payload, json!("wake"));
    }
}

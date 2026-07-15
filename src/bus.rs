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

use crate::route::Route;
use crate::store::Store;

pub const DEFAULT_RECV_TIMEOUT_MS: u64 = 25_000;

/// How long a presence announcement is *retained* without a refresh. Generous —
/// covering a multi-day laptop sleep — because a silent session may just be asleep,
/// not gone (a graceful close removes it immediately via `/unregister`). Clients
/// classify each entry live-vs-away from the `age_ms` the roster reports; this bound
/// only decides when a long-silent entry is finally dropped. See `docs/PRESENCE.md`.
pub const AWAY_RETAIN_MS: u64 = 3 * 24 * 60 * 60 * 1_000; // 3 days

/// Cap on distinct roster entries, so an announcement flood can't grow it without
/// limit. Far above any real mesh; expired entries are pruned first.
const ROSTER_CAP: usize = 4096;

/// When the wakeup map grows past this, prune idle entries so a stream of distinct
/// (unauthenticated) recipient ids can't leak `Notify`s without bound.
const NOTIFY_CAP: usize = 4096;

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
    /// Presence roster: `pubkey#session_id` → (opaque signed announcement,
    /// received-at ms). Keyed per *session* so several live sessions under one
    /// identity coexist instead of overwriting each other; clients group by the
    /// announcement's `pubkey`. In-memory and ephemeral — the bus stores and serves
    /// it but never verifies it; clients check the signatures. Just a bulletin board.
    roster: Arc<DashMap<String, (Value, u64)>>,
    cap: usize,
}

impl Broker {
    pub fn new(store: Store, cap: usize) -> Self {
        Self {
            store,
            notifies: Arc::new(DashMap::new()),
            roster: Arc::new(DashMap::new()),
            cap: cap.max(1),
        }
    }

    /// Record a presence announcement under a per-session key (`pubkey#session_id`,
    /// or a bare `pubkey` for a sessionless legacy announcement). Prunes expired
    /// entries; the announcement is stored verbatim (the bus never inspects it
    /// beyond the routing key).
    pub fn announce(&self, key: String, announcement: Value, now: u64) {
        self.roster
            .retain(|_, (_, at)| now.saturating_sub(*at) < AWAY_RETAIN_MS);
        if self.roster.len() >= ROSTER_CAP && !self.roster.contains_key(&key) {
            return; // full of live entries; drop the newcomer rather than evict
        }
        self.roster.insert(key, (announcement, now));
    }

    /// Immediately drop a session's presence (a graceful close), so a peer learns
    /// it's really gone rather than waiting out the TTL. Unsigned and best-effort:
    /// the bus verifies nothing, and a still-live session simply re-announces on its
    /// next heartbeat, so a spurious unregister is self-healing.
    pub fn unregister(&self, key: &str) {
        self.roster.remove(key);
    }

    /// The retained announcements, each stamped with an unsigned `age_ms` (how long
    /// since its last refresh) so the client can classify it live vs. away. The
    /// announcement's signed fields are untouched — `age_ms` is additive and ignored
    /// by signature verification.
    pub fn roster(&self, now: u64) -> Vec<Value> {
        self.roster
            .iter()
            .filter_map(|e| {
                let (ann, at) = e.value();
                let age = now.saturating_sub(*at);
                if age >= AWAY_RETAIN_MS {
                    return None;
                }
                let mut v = ann.clone();
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("age_ms".into(), json!(age));
                }
                Some(v)
            })
            .collect()
    }

    fn notify_handle(&self, id: &str) -> Arc<Notify> {
        // Before adding a new id, prune idle notifies when the map is large. An entry
        // whose Arc has no clone outside the map (`strong_count == 1`) has no parked
        // waiter, so dropping it can't lose a wakeup — a later waiter recreates it and
        // peeks the durable store directly. Bounds an otherwise-unbounded leak from a
        // flood of distinct recipient ids on the unauthenticated `/send`//`/recv`.
        if self.notifies.len() > NOTIFY_CAP && !self.notifies.contains_key(id) {
            self.notifies.retain(|_, v| Arc::strong_count(v) > 1);
        }
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
            .route("/announce", post(announce_handler))
            .route("/unregister", post(unregister_handler))
            .route("/roster", get(roster_handler))
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

/// The roster key: `pubkey#session_id`, or a bare `pubkey` when the announcement
/// carries no session (a legacy node). The pubkey is the only field the bus needs
/// to read; the rest (name, session, ts, sig) is opaque to it.
fn roster_key(body: &Value) -> Option<String> {
    let pubkey = body.get("pubkey").and_then(|v| v.as_str())?;
    let session_id = body
        .get("session")
        .and_then(|s| s.get("session_id"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    Some(Route::new(pubkey, session_id).to_string())
}

/// The announcement is stored verbatim under its per-session roster key.
async fn announce_handler(State(broker): State<Broker>, Json(body): Json<Value>) -> StatusCode {
    let Some(key) = roster_key(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    broker.announce(key, body, crate::now_ms());
    StatusCode::ACCEPTED
}

/// Graceful presence removal. Takes the same `{ pubkey, session }` shape as an
/// announcement (the sig is ignored — see [`Broker::unregister`]).
async fn unregister_handler(State(broker): State<Broker>, Json(body): Json<Value>) -> StatusCode {
    let Some(key) = roster_key(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    broker.unregister(&key);
    StatusCode::ACCEPTED
}

async fn roster_handler(State(broker): State<Broker>) -> Response {
    Json(json!({ "roster": broker.roster(crate::now_ms()) })).into_response()
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

    #[test]
    fn roster_stores_and_expires() {
        let b = broker(8);
        b.announce(
            "keyA".into(),
            json!({"pubkey":"keyA","name":"alice"}),
            1_000,
        );
        b.announce("keyB".into(), json!({"pubkey":"keyB","name":"bob"}), 1_000);
        assert_eq!(b.roster(1_000).len(), 2);
        assert_eq!(
            b.roster(1_000 + AWAY_RETAIN_MS - 1).len(),
            2,
            "retained within the away window"
        );
        assert_eq!(
            b.roster(1_000 + AWAY_RETAIN_MS + 1).len(),
            0,
            "dropped past the retention bound"
        );
    }

    #[test]
    fn roster_stamps_age_ms() {
        let b = broker(8);
        b.announce(
            "keyA".into(),
            json!({"pubkey":"keyA","name":"alice"}),
            1_000,
        );
        let r = b.roster(1_000 + 5_000);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0]["age_ms"], json!(5_000u64), "age since last refresh");
        // signed fields untouched
        assert_eq!(r[0]["pubkey"], json!("keyA"));
    }

    #[test]
    fn announce_upserts_by_pubkey() {
        let b = broker(8);
        b.announce("keyA".into(), json!({"pubkey":"keyA","name":"old"}), 1_000);
        b.announce("keyA".into(), json!({"pubkey":"keyA","name":"new"}), 2_000);
        let r = b.roster(2_000);
        assert_eq!(r.len(), 1, "same pubkey replaces, not appends");
        assert_eq!(r[0]["name"], "new");
    }

    #[test]
    fn roster_key_scopes_by_session() {
        let s1 = json!({"pubkey":"keyA","session":{"session_id":"aa11"}});
        let s2 = json!({"pubkey":"keyA","session":{"session_id":"bb22"}});
        assert_eq!(roster_key(&s1).unwrap(), "keyA#aa11");
        assert_ne!(
            roster_key(&s1),
            roster_key(&s2),
            "distinct sessions distinct"
        );
        // Legacy / sessionless announcement falls back to the bare pubkey.
        assert_eq!(roster_key(&json!({"pubkey":"keyA"})).unwrap(), "keyA");
        assert!(
            roster_key(&json!({"name":"x"})).is_none(),
            "pubkey required"
        );
    }

    #[test]
    fn two_sessions_under_one_identity_coexist() {
        let b = broker(8);
        let s1 = json!({"pubkey":"keyA","session":{"session_id":"aa11"}});
        let s2 = json!({"pubkey":"keyA","session":{"session_id":"bb22"}});
        b.announce(roster_key(&s1).unwrap(), s1, 1_000);
        b.announce(roster_key(&s2).unwrap(), s2, 1_000);
        assert_eq!(b.roster(1_000).len(), 2, "same identity, two live sessions");
    }

    #[test]
    fn unregister_removes_one_session_immediately() {
        let b = broker(8);
        let s1 = json!({"pubkey":"keyA","session":{"session_id":"aa11"}});
        let s2 = json!({"pubkey":"keyA","session":{"session_id":"bb22"}});
        b.announce(roster_key(&s1).unwrap(), s1.clone(), 1_000);
        b.announce(roster_key(&s2).unwrap(), s2, 1_000);
        b.unregister(&roster_key(&s1).unwrap());
        let r = b.roster(1_000);
        assert_eq!(r.len(), 1, "only the unregistered session is gone");
        assert_eq!(r[0]["session"]["session_id"], "bb22");
    }
}

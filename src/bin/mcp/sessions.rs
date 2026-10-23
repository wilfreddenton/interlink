//! Claude may replace its conversation while keeping this MCP connection alive.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use interlink::agent::Dedupe;
use interlink::claude::ClaudeSocket;
use interlink::identity::SessionInfo;
use interlink::store::Store;
use rmcp::RoleServer;
use rmcp::service::Peer;
use tokio::sync::{Mutex, Notify, RwLock as AsyncRwLock};
use tokio::task::JoinSet;
use tokio::time::timeout;

use super::{
    DEDUPE_CAP, Inner, Sink, announce_loop, detect_git_root, inbound_loop, inbox_path,
    notification_loop, outbound_loop, pairing_loop, send_goodbyes, unregister_now,
};

pub(super) struct ClaudeSessions {
    template: Arc<Inner>,
    socket: Option<ClaudeSocket>,
    channels: bool,
    pub(super) active: AsyncRwLock<Option<SessionRuntime>>,
}

pub(super) struct SessionRuntime {
    pub(super) inner: Arc<Inner>,
    workers: JoinSet<()>,
}

impl SessionRuntime {
    pub(super) fn start(inner: Arc<Inner>, sink: Arc<Sink>) -> Self {
        let mut workers = JoinSet::new();
        workers.spawn(outbound_loop(inner.clone()));
        workers.spawn(pairing_loop(inner.clone()));
        workers.spawn(announce_loop(inner.clone()));
        workers.spawn(notification_loop(inner.clone(), sink.clone()));
        for url in inner.urls.clone() {
            workers.spawn(inbound_loop(inner.clone(), sink.clone(), url));
        }
        Self { inner, workers }
    }

    pub(super) async fn stop(&mut self, end_session: bool) -> Result<()> {
        self.workers.abort_all();
        while self.workers.join_next().await.is_some() {}
        // Retire locally before best-effort network cleanup: SessionEnd hooks
        // have a short host deadline, and may be cancelled while the bus is down.
        let retired = if end_session {
            self.inner
                .mailbox()
                .and_then(|mailbox| mailbox.end_session())
        } else {
            Ok(())
        };
        let _ = timeout(Duration::from_secs(3), unregister_now(&self.inner)).await;
        let _ = timeout(Duration::from_secs(3), send_goodbyes(&self.inner)).await;
        retired
    }
}

impl ClaudeSessions {
    pub(super) fn new(template: Arc<Inner>, socket: Option<ClaudeSocket>, channels: bool) -> Self {
        Self {
            template,
            socket,
            channels,
            active: AsyncRwLock::new(None),
        }
    }

    pub(super) async fn bind(
        &self,
        id: &str,
        cwd: Option<&str>,
        peer: Peer<RoleServer>,
    ) -> Result<Arc<Inner>> {
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            bail!("expected the owning Claude session ID from a local lifecycle hook");
        }
        let mut active = self.active.write().await;
        if let Some(runtime) = active.as_ref()
            && runtime.inner.session.read().unwrap().session_id == id
        {
            return Ok(runtime.inner.clone());
        }
        if let Some(mut old) = active.take() {
            old.stop(true).await?;
        }
        let template = &self.template;
        let cwd = cwd
            .map(str::to_owned)
            .unwrap_or_else(|| template.session.read().unwrap().cwd.clone());
        // A failed switch leaves tools unbound, never attached to the old mailbox.
        // Never mutate an old Inner: in-flight work must keep its original route.
        let inner = Arc::new(Inner {
            codex: None,
            recovery: Mutex::new(()),
            key: template.key.clone(),
            policy: template.policy.clone(),
            urls: template.urls.clone(),
            http: template.http.clone(),
            dedupe: Mutex::new(Dedupe::new(DEDUPE_CAP)),
            store: Store::in_memory()?,
            outbox: Arc::new(Notify::new()),
            name: template.name.clone(),
            session: RwLock::new(SessionInfo {
                session_id: id.to_owned(),
                git_root: detect_git_root(&cwd),
                cwd,
                summary: String::new(),
            }),
            summary_guidance_sent: AtomicBool::new(false),
            sticky: RwLock::new(HashMap::new()),
        });
        let sink = if self.channels {
            Sink::Channel(Box::new(peer))
        } else if let Some(socket) = &self.socket {
            Sink::Socket {
                socket: socket.clone(),
                session_id: id.to_owned(),
                inbox: inbox_path(id).context("no state directory for socket control inbox")?,
            }
        } else {
            Sink::Inbox(inner.clone())
        };
        *active = Some(SessionRuntime::start(inner.clone(), Arc::new(sink)));
        Ok(inner)
    }

    pub(super) async fn end(&self, id: Option<&str>) -> Result<()> {
        let mut active = self.active.write().await;
        if let Some(id) = id
            && !active
                .as_ref()
                .is_some_and(|runtime| runtime.inner.session.read().unwrap().session_id == id)
        {
            return Ok(());
        }
        if let Some(mut old) = active.take() {
            // An MCP restart within the same chat retains its outstanding notice.
            // Only an explicit host SessionEnd retires that chat's wake-up.
            old.stop(id.is_some()).await?;
        }
        Ok(())
    }
}

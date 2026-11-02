//! Persistent session ownership learned from authorized, signed announcements.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

use crate::identity::{AgentId, Announcement};
use crate::policy::Policy;
use crate::state::{atomic_write, lock};

const MAX_BINDINGS: usize = 4096;
type Bindings = BTreeMap<String, BTreeSet<String>>;

pub struct SessionDirectory {
    path: PathBuf,
}

impl SessionDirectory {
    pub fn new(path: &Path) -> Self {
        Self { path: path.into() }
    }

    fn load(&self) -> Result<Bindings> {
        match fs::read(&self.path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(Bindings::new()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn observe(
        &self,
        announcements: &[Announcement],
        me: AgentId,
        policy: &Policy,
    ) -> Result<()> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut bindings = self.load()?;
        let mut changed = false;
        for announcement in announcements {
            let Ok(owner) = announcement.verify() else {
                continue;
            };
            let session = &announcement.session.session_id;
            if (owner != me && policy.peer(owner).is_none()) || session.is_empty() {
                continue;
            }
            if session.len() > 256 || session.chars().any(|c| c.is_control() || c == '#') {
                continue;
            }
            // Keep conflicting claims, including owners later removed from policy.
            // A disappearance or trust change must never silently repoint a session.
            changed |= bindings
                .entry(session.clone())
                .or_default()
                .insert(owner.to_b64());
        }
        if bindings.values().map(BTreeSet::len).sum::<usize>() > MAX_BINDINGS {
            bail!(
                "session directory is full; use an explicit peer and session rather than replacing verified ownership"
            );
        }
        if changed {
            atomic_write(&self.path, &serde_json::to_vec(&bindings)?)?;
        }
        Ok(())
    }

    pub fn resolve(&self, session: &str, me: AgentId, policy: &Policy) -> Result<Option<AgentId>> {
        let bindings = self.load()?;
        let Some(owners) = bindings.get(session) else {
            return Ok(None);
        };
        if owners.len() != 1 {
            bail!(
                "session '{session}' has conflicting identity claims; use an explicit peer and session"
            );
        }
        let owner = AgentId::from_b64(owners.first().unwrap())?;
        if owner != me && policy.peer(owner).is_none() {
            bail!("session '{session}' belongs to an identity that is not authorized");
        }
        Ok(Some(owner))
    }

    pub fn unique_owners(&self) -> Result<BTreeMap<String, String>> {
        Ok(self
            .load()?
            .into_iter()
            .filter_map(|(session, owners)| {
                (owners.len() == 1).then(|| (session, owners.into_iter().next().unwrap()))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{AgentKey, SessionInfo};
    use std::thread;

    #[test]
    fn concurrent_observers_merge_and_capacity_never_evicts_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        let me = AgentKey::generate().unwrap();
        let policy = Policy::default();
        let announcements: Vec<_> = (0..8)
            .map(|i| {
                me.announce(
                    "local",
                    &SessionInfo {
                        session_id: format!("session-{i}"),
                        ..Default::default()
                    },
                    1,
                )
            })
            .collect();
        thread::scope(|scope| {
            for announcement in &announcements {
                let path = &path;
                let me = &me;
                let policy = &policy;
                scope.spawn(move || {
                    SessionDirectory::new(path)
                        .observe(std::slice::from_ref(announcement), me.id(), policy)
                        .unwrap()
                });
            }
        });
        let directory = SessionDirectory::new(&path);
        assert_eq!(directory.load().unwrap().len(), 8);
        let bindings: Bindings = (0..MAX_BINDINGS)
            .map(|i| (format!("session-{i}"), BTreeSet::from([me.id().to_b64()])))
            .collect();
        atomic_write(&path, &serde_json::to_vec(&bindings).unwrap()).unwrap();
        let extra = me.announce(
            "local",
            &SessionInfo {
                session_id: "extra".into(),
                ..Default::default()
            },
            1,
        );
        assert!(directory.observe(&[extra], me.id(), &policy).is_err());
        assert_eq!(directory.load().unwrap(), bindings);
        assert_eq!(
            directory.resolve("session-0", me.id(), &policy).unwrap(),
            Some(me.id())
        );
    }

    #[test]
    fn verified_ownership_survives_restart_and_never_silently_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        let me = AgentKey::generate().unwrap();
        let peer = AgentKey::generate().unwrap();
        let other = AgentKey::generate().unwrap();
        let session = SessionInfo {
            session_id: "session".into(),
            ..Default::default()
        };
        let mut policy = Policy::default();
        policy.add("peer", &peer.id().to_b64()).unwrap();
        let announcement = peer.announce("peer", &session, 1);
        let mut forged = announcement.clone();
        forged.session.session_id = "forged".into();
        let directory = SessionDirectory::new(&path);
        directory
            .observe(
                &[forged, other.announce("other", &session, 1)],
                me.id(),
                &policy,
            )
            .unwrap();
        assert_eq!(
            directory.resolve("session", me.id(), &policy).unwrap(),
            None
        );
        assert_eq!(directory.resolve("forged", me.id(), &policy).unwrap(), None);
        directory
            .observe(&[announcement], me.id(), &policy)
            .unwrap();
        let directory = SessionDirectory::new(&path);
        assert_eq!(
            directory.resolve("session", me.id(), &policy).unwrap(),
            Some(peer.id())
        );
        assert!(
            directory
                .resolve("session", me.id(), &Policy::default())
                .is_err()
        );
        policy.add("other", &other.id().to_b64()).unwrap();
        directory
            .observe(&[other.announce("other", &session, 2)], me.id(), &policy)
            .unwrap();
        assert!(directory.resolve("session", me.id(), &policy).is_err());
        let mut replacement = Policy::default();
        replacement.add("other", &other.id().to_b64()).unwrap();
        assert!(directory.resolve("session", me.id(), &replacement).is_err());
    }
}

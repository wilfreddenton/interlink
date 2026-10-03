//! Shared peer settings. Readers reload complete snapshots; writers serialize updates.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use anyhow::Result;
use fs2::FileExt;

use crate::identity::AgentId;
use crate::policy::Policy;

pub struct PolicyStore {
    path: PathBuf,
}

impl PolicyStore {
    pub fn open(path: &Path) -> Result<Self> {
        // Resolve aliases once so sessions using a symlink share the same lock.
        let path = path.canonicalize()?;
        Policy::load(&path)?;
        Ok(Self { path })
    }

    pub fn read(&self) -> Result<Policy> {
        Policy::load(&self.path)
    }

    fn update<T>(&self, change: impl FnOnce(&mut Policy) -> Result<T>) -> Result<T> {
        // Lock a stable sidecar: atomic replacement changes the policy file's inode.
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.path.with_extension("json.lock"))?;
        lock.lock_exclusive()?;
        let mut policy = self.read()?;
        let result = change(&mut policy)?;
        policy.save(&self.path)?;
        Ok(result)
    }

    pub fn add(&self, name: &str, key: &str) -> Result<()> {
        self.update(|policy| policy.add(name, key))
    }

    pub fn add_for_pairing(&self, name: &str, key: &str) -> Result<String> {
        self.update(|policy| {
            // Another local session may already have paired this identity under
            // its own petname. Keep that operator-selected name.
            if let Some(peer) = policy.peer(AgentId::from_b64(key)?) {
                return Ok(peer.petname.clone());
            }
            policy.add(name, key)?;
            Ok(name.to_string())
        })
    }

    pub fn remove(&self, name: &str) -> Result<bool> {
        self.update(|policy| Ok(policy.remove(name)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AgentKey;
    use std::thread;

    #[test]
    fn independent_sessions_merge_updates_and_observe_removals() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peers.json");
        std::fs::write(&path, "{}").unwrap();
        let first = PolicyStore::open(&path).unwrap();
        let second = PolicyStore::open(&path).unwrap();
        let alice = AgentKey::generate().unwrap().id().to_b64();
        let bob = AgentKey::generate().unwrap().id().to_b64();
        thread::scope(|scope| {
            scope.spawn(|| first.add("alice", &alice).unwrap());
            scope.spawn(|| second.add("bob", &bob).unwrap());
        });
        assert_eq!(first.read().unwrap().len(), 2);
        first.remove("alice").unwrap();
        assert!(second.read().unwrap().resolve("alice").is_err());
        second.add("bob", &bob).unwrap();
        assert_eq!(first.read().unwrap().len(), 1);
    }

    #[test]
    fn failed_update_leaves_persisted_policy_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peers.json");
        std::fs::write(&path, "{}").unwrap();
        let store = PolicyStore::open(&path).unwrap();
        let alice = AgentKey::generate().unwrap().id().to_b64();
        store.add("alice", &alice).unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(
            store
                .add("alice", &AgentKey::generate().unwrap().id().to_b64())
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        // Failure to acquire the sidecar must also leave the snapshot intact.
        std::fs::remove_file(path.with_extension("json.lock")).unwrap();
        std::fs::create_dir(path.with_extension("json.lock")).unwrap();
        assert!(store.remove("alice").is_err());
        assert!(store.read().unwrap().resolve("alice").is_ok());
    }
}

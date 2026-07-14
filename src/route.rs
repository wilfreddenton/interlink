//! Bus routing addresses.
//!
//! A [`Route`] is an identity key optionally scoped to one live session
//! (`key#session_id`). The bus treats the whole thing as an **opaque** queue name —
//! it never parses the `#`. This module is the *one* place that builds and splits
//! the convention, so the client (and the bus's roster key) agree by construction
//! instead of by scattered `format!` / `split` calls.

use std::fmt;

/// An inbox address: a base64 identity `key`, optionally scoped to a `session`.
/// Keys are base64 and session ids are hex, so neither contains `#` — the single
/// separator is unambiguous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub key: String,
    pub session: Option<String>,
}

impl Route {
    /// A session-scoped route. An empty `session` collapses to a bare-key route, so
    /// a legacy/sessionless announcement round-trips to `key`.
    pub fn new(key: impl Into<String>, session: impl Into<String>) -> Self {
        let session = session.into();
        Route {
            key: key.into(),
            session: (!session.is_empty()).then_some(session),
        }
    }

    /// A bare-key route (no session scope).
    pub fn bare(key: impl Into<String>) -> Self {
        Route {
            key: key.into(),
            session: None,
        }
    }

    /// Parse `key#session` (or a bare `key`). A trailing empty session (`key#`) is
    /// treated as bare.
    pub fn parse(s: &str) -> Self {
        match s.split_once('#') {
            Some((key, session)) if !session.is_empty() => Route {
                key: key.to_string(),
                session: Some(session.to_string()),
            },
            _ => Route::bare(s),
        }
    }

    /// The session scope, if any.
    pub fn session(&self) -> Option<&str> {
        self.session.as_deref()
    }
}

impl fmt::Display for Route {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.session {
            Some(s) => write!(f, "{}#{}", self.key, s),
            None => write!(f, "{}", self.key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_session_scoped() {
        let r = Route::new("KEY", "s12");
        assert_eq!(r.to_string(), "KEY#s12");
        assert_eq!(Route::parse("KEY#s12"), r);
        assert_eq!(r.session(), Some("s12"));
    }

    #[test]
    fn empty_session_is_bare() {
        assert_eq!(Route::new("KEY", ""), Route::bare("KEY"));
        assert_eq!(Route::new("KEY", "").to_string(), "KEY");
        assert_eq!(Route::parse("KEY").session(), None);
        assert_eq!(Route::parse("KEY#").session(), None, "trailing # is bare");
    }

    #[test]
    fn parse_splits_on_the_separator() {
        let r = Route::parse("KEY#abcd");
        assert_eq!(r.key, "KEY");
        assert_eq!(r.session(), Some("abcd"));
    }
}

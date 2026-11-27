//! Phase 14 client surface for the audit query (`AuditApi`, 14-18).
//!
//! | method | path | raw |
//! |--------|------|-----|
//! | GET | `/v1/audit?since&until&tenant&action&actor&limit&cursor` | [`Client::audit_query`] |
//!
//! The answer is the server's `AuditPage` as JSON: `records` (oldest first, merged across
//! members by `(ts_ms, node, seq)`), `cursor` (`null` at the end) and `partial_nodes`
//! (members that did not answer). `since` / `until` accept Unix milliseconds or RFC 3339.

use crate::client::{CallError, Client, Response};

/// The filters of [`Client::audit_query`]; unset fields are not sent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditFilter {
    /// Inclusive lower bound: Unix ms or RFC 3339.
    pub since: Option<String>,
    /// Exclusive upper bound: Unix ms or RFC 3339.
    pub until: Option<String>,
    /// Tenant (`_` = records without a tenant).
    pub tenant: Option<String>,
    /// Action, exact or `prefix.*`.
    pub action: Option<String>,
    /// Actor id (token id, worker id, `run:<id>`, …).
    pub actor: Option<String>,
    /// Page size (server default 100, at most 1000).
    pub limit: Option<u32>,
    /// Cursor of the previous page.
    pub cursor: Option<String>,
}

impl AuditFilter {
    /// The REST query pairs.
    pub fn query(&self) -> Vec<(&'static str, String)> {
        let mut q = Vec::new();
        let mut put = |k: &'static str, v: &Option<String>| {
            if let Some(v) = v.as_ref().filter(|v| !v.is_empty()) {
                q.push((k, v.clone()));
            }
        };
        put("since", &self.since);
        put("until", &self.until);
        put("tenant", &self.tenant);
        put("action", &self.action);
        put("actor", &self.actor);
        put("cursor", &self.cursor);
        if let Some(l) = self.limit {
            q.push(("limit", l.to_string()));
        }
        q
    }
}

impl Client {
    /// `GET /v1/audit` (one page).
    pub async fn audit_query(&self, filter: &AuditFilter) -> Result<Response, CallError> {
        self.get(&["v1", "audit"], &filter.query()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_set_filters_are_sent() {
        assert!(AuditFilter::default().query().is_empty());
        let f = AuditFilter {
            since: Some("1700000000000".into()),
            action: Some("run.*".into()),
            tenant: Some(String::new()),
            limit: Some(5),
            cursor: Some("a1.1-4".into()),
            ..AuditFilter::default()
        };
        assert_eq!(
            f.query(),
            [
                ("since", "1700000000000".to_owned()),
                ("action", "run.*".to_owned()),
                ("cursor", "a1.1-4".to_owned()),
                ("limit", "5".to_owned()),
            ]
        );
    }
}

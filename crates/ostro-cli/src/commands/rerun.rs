//! `ostro rerun` (11-14, OPS-02): a new run that copies the source journal up to a cut
//! and executes the rest live (effects after the cut really re-execute).
//!
//! The cut and version arguments ([`CutArgs`], [`parse_version`]) are shared with
//! `ostro dlq requeue --mode rerun` and `ostro bulk rerun`.

use clap::Args;
use serde_json::{Map, Value, json};

use super::{Conn, report};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s};

/// A rerun cut: `--from-step NAME[#N]` or `--from-seq N`.
#[derive(Debug, Clone, Default, Args)]
pub struct CutArgs {
    /// Cut before the N-th (1-based, default 1) execution of step NAME: `NAME` or `NAME#N`.
    #[arg(long, value_name = "NAME[#N]", value_parser = parse_step_cut, conflicts_with = "from_seq")]
    pub from_step: Option<(String, u32)>,
    /// Cut at journal sequence number N (events before N are copied).
    #[arg(long, value_name = "N")]
    pub from_seq: Option<u64>,
}

impl CutArgs {
    /// The REST `from` value (`{"step": {"name", "occurrence"}}` / `{"seq": N}`), or
    /// `None` when no cut was given.
    pub fn from(&self) -> Option<Value> {
        match (&self.from_step, self.from_seq) {
            (Some((name, occurrence)), _) => {
                Some(json!({ "step": { "name": name, "occurrence": occurrence } }))
            }
            (None, Some(seq)) => Some(json!({ "seq": seq })),
            (None, None) => None,
        }
    }
}

/// `NAME` or `NAME#N` (N >= 1).
pub fn parse_step_cut(s: &str) -> Result<(String, u32), String> {
    let (name, occ) = match s.rsplit_once('#') {
        Some((n, o)) => {
            let o: u32 = o
                .parse()
                .map_err(|_| format!("invalid step cut {s:?}: occurrence must be a number"))?;
            (n, o)
        }
        None => (s, 1),
    };
    if name.is_empty() {
        return Err(format!("invalid step cut {s:?}: empty step name"));
    }
    if occ == 0 {
        return Err(format!("invalid step cut {s:?}: occurrence is 1-based"));
    }
    Ok((name.to_owned(), occ))
}

/// `--version active|source|N` as the REST value (`"active"`, `"source"`,
/// `{"version": N}`).
pub fn parse_version(s: &str) -> Result<Value, String> {
    match s.trim() {
        "active" => Ok(json!("active")),
        "source" => Ok(json!("source")),
        n => n
            .parse::<u32>()
            .map(|v| json!({ "version": v }))
            .map_err(|_| format!("invalid version {s:?}: expected active, source or a number")),
    }
}

/// `ostro rerun`.
#[derive(Debug, Clone, Args)]
#[command(group(clap::ArgGroup::new("cut").required(true).args(["from_step", "from_seq"])))]
pub struct RerunArgs {
    /// Source run id (terminal).
    pub run: String,
    #[command(flatten)]
    pub cut: CutArgs,
    /// Version to execute: `active` (server default), `source` or a number.
    #[arg(long, value_name = "active|source|N", value_parser = parse_version)]
    pub version: Option<Value>,
    /// Dedupe key: the same key returns the same rerun.
    #[arg(long)]
    pub idempotency_key: Option<String>,
    /// Validate only: the server runs the cut analysis and replay check, starts nothing.
    #[arg(long)]
    pub dry_run: bool,
    #[command(flatten)]
    pub conn: Conn,
}

impl RerunArgs {
    /// The REST body.
    pub fn body(&self) -> Value {
        let mut b = Map::new();
        if let Some(from) = self.cut.from() {
            b.insert("from".into(), from);
        }
        if let Some(v) = &self.version {
            b.insert("version".into(), v.clone());
        }
        if let Some(k) = &self.idempotency_key {
            b.insert("idempotency_key".into(), json!(k));
        }
        if self.dry_run {
            b.insert("dry_run".into(), json!(true));
        }
        Value::Object(b)
    }
}

/// Print the details of a rerun refusal (unsafe cut, nondeterministic prefix, prefix too
/// large) on stderr after the error line.
pub fn refusal_details(e: &CallError) {
    let CallError::Api { body, .. } = e else {
        return;
    };
    let d = &body["error"]["details"];
    match d["reason"].as_str() {
        Some("rerun_cut_unsafe") => {
            let nearest = match d["nearest_safe_seq"].as_u64() {
                Some(n) => format!("; nearest safe cut: --from-seq {n}"),
                None => "; no safe cut before it".to_owned(),
            };
            eprintln!(
                "ostro: cut at seq {} is unsafe ({}){nearest}",
                s(&d["seq"]),
                s(&d["detail"])
            );
        }
        Some("rerun_nondeterministic") => {
            eprintln!(
                "ostro: the copied prefix does not replay against the target module (diverged at seq {}): {}",
                s(&d["seq"]),
                s(&d["detail"])
            );
        }
        Some(_) if d.get("limit").is_some() => {
            eprintln!(
                "ostro: limit {}: max {}, actual {}",
                s(&d["limit"]),
                s(&d["max"]),
                s(&d["actual"])
            );
        }
        _ => {}
    }
}

/// `ostro rerun`.
pub async fn rerun(c: &Client, out: Out, a: &RerunArgs) -> Result<Outcome, CallError> {
    let r = match c.rerun(&a.run, &a.body()).await {
        Ok(r) => r,
        Err(e) => {
            let o = report(out, &e);
            refusal_details(&e);
            return Ok(o);
        }
    };
    let b = &r.body;
    if out.json {
        out.json(b);
        return Ok(Outcome::Ok);
    }
    let what = format!(
        "{} v{}, cut at seq {}, {} events copied, verdict {}",
        s(&b["workflow"]),
        s(&b["version"]),
        s(&b["cut_seq"]),
        s(&b["copied_events"]),
        s(&b["verdict"]),
    );
    if a.dry_run || b["new_run_id"].is_null() {
        println!("dry run: rerun of {} would be accepted ({what})", a.run);
    } else if b["existing"].as_bool() == Some(true) {
        println!(
            "existing rerun {} returned (idempotency key reused; {what})",
            s(&b["new_run_id"])
        );
    } else {
        println!(
            "started rerun {} of {} ({what})",
            s(&b["new_run_id"]),
            a.run
        );
    }
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::super::ClientCommand;
    use super::super::tests::parse;
    use super::*;

    #[test]
    fn rerun_body_from_step_and_seq() {
        let ClientCommand::Rerun(a) = parse(&[
            "rerun",
            "r1",
            "--from-step",
            "s3#2",
            "--version",
            "source",
            "--idempotency-key",
            "k",
            "--dry-run",
        ])
        .unwrap() else {
            panic!("rerun")
        };
        assert_eq!(
            a.body(),
            json!({
                "from": {"step": {"name": "s3", "occurrence": 2}},
                "version": "source",
                "idempotency_key": "k",
                "dry_run": true,
            })
        );
        let ClientCommand::Rerun(a) =
            parse(&["rerun", "r1", "--from-seq", "7", "--version", "3"]).unwrap()
        else {
            panic!("rerun")
        };
        assert_eq!(
            a.body(),
            json!({"from": {"seq": 7}, "version": {"version": 3}})
        );
        let ClientCommand::Rerun(a) = parse(&["rerun", "r1", "--from-step", "s3"]).unwrap() else {
            panic!("rerun")
        };
        assert_eq!(
            a.body(),
            json!({"from": {"step": {"name": "s3", "occurrence": 1}}})
        );
    }

    #[test]
    fn rerun_usage_errors() {
        for args in [
            &["rerun", "r1"][..],
            &["rerun", "r1", "--from-seq", "1", "--from-step", "s"],
            &["rerun", "r1", "--from-step", "s#0"],
            &["rerun", "r1", "--from-step", "#2"],
            &["rerun", "r1", "--from-step", "s#x"],
            &["rerun", "r1", "--from-seq", "1", "--version", "latest"],
        ] {
            assert_eq!(parse(args).unwrap_err().exit_code(), 2, "{args:?}");
        }
    }
}

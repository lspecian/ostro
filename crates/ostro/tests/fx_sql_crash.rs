//! 06-18: ROADMAP Phase 6 success criterion 2 with a real crash. Exactly-once SQL makes
//! exactly one row change per run when the node is killed (`kill -9`) between the user
//! database COMMIT and `effect_completed`.
//!
//! Each leg:
//! 1. creates a per-test schema (PostgreSQL, `t_<id>`, applied as `search_path` through the
//!    connection's `"schema"`, W3) or a per-test database (MySQL), and the user table
//!    `orders(run_id, amount)`;
//! 2. spawns `ostro dev` on a temp data dir with `FAILPOINTS=<site>=return(hang)`, deploys
//!    `fx-sql` and configures over REST a sealed password secret, the connection `orders`
//!    (`pool_max` 60) and the module's egress `connection:orders`;
//! 3. starts 50 runs, each an exactly-once `INSERT INTO orders(run_id, amount)` with its
//!    own key;
//! 4. waits until all 50 effects are held at the failpoint. `after_commit`: 50 token rows
//!    with a stored result (the user transaction committed). `before_user_stmt`: 50
//!    transactions holding the token table's insert lock, none committed. The polling
//!    queries go through the same connection with `exactly_once = false` (autocommit, which
//!    the failpoint does not cover), so they also prove the pool stays usable while 50
//!    effects are held (W4);
//! 5. `kill -9`, restarts without FAILPOINTS on the same data dir and waits until all 50
//!    runs are terminal;
//! 6. asserts exactly 50 user rows, one per key with its amount; every run completed with
//!    `rows_affected = 1`; one `effect_scheduled` and one `effect_completed` per run; and one
//!    token row per run whose stored result is byte-identical to the journaled completion
//!    the guest decoded.
//!
//! The history API shows only `{op}` for `effect_completed`, and this crate cannot decode
//! journal bytes (no ostro-types dev-dependency, and only 06-02 edits Cargo files). The
//! guest's output, though, is decoded from the journaled completion (P-10: the guest sees
//! completions only as journaled inputs). So the check is: every guest output is
//! `{columns: [], rows: [], rows_affected: 1}`, and every stored `ostro_effects.result` is
//! exactly `RESULT_CODEC_VER || postcard(Ok(OpOutput::Sql(SqlResult{[], [], 1})))`, the one
//! byte string that decodes to that output.
//!
//! The URLs come from `OSTRO_TEST_PG_URL` / `OSTRO_TEST_MYSQL_URL` (06-11 form; a password,
//! if present, must be at least 8 bytes because it travels as a sealed secret). A leg whose
//! variable is unset SKIPs loudly; that leg is recorded human_needed (D-FX5).
#![cfg(feature = "failpoints")]
#![cfg(unix)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Api, SERIAL, Spawned, cli_json, dev, events_of, free_addr, http, write_guest};
use serde_json::{Value, json};

/// Runs per leg.
const N: usize = 50;
/// Limit for all `N` effects to reach the failpoint.
const HOLD_LIMIT: Duration = Duration::from_secs(60);
/// Limit for all `N` runs to finish after the restart.
const FINISH_LIMIT: Duration = Duration::from_secs(120);
/// Limit for one helper (autocommit) query run.
const QUERY_LIMIT: Duration = Duration::from_secs(30);
/// Sealed secret holding the database password.
const PASS_SECRET: &str = "db/orders-pass";
/// `encode_stored(encode_outcome(&Ok(OpOutput::Sql(SqlResult{columns: [], rows: [],
/// rows_affected: 1}))))`: codec_ver 1, then postcard Ok (0), OpOutput::Sql (1), empty
/// columns (0), empty rows (0), rows_affected varint 1.
const STORED_INSERT_RESULT_HEX: &str = "010001000001";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dialect {
    Pg,
    My,
}

#[derive(Clone, Copy, Debug)]
enum Site {
    AfterCommit,
    BeforeUserStmt,
}

impl Site {
    fn failpoint(self) -> &'static str {
        match self {
            Self::AfterCommit => "bridge::sql::after_commit=return(hang)",
            Self::BeforeUserStmt => "bridge::sql::before_user_stmt=return(hang)",
        }
    }
}

impl Dialect {
    fn env(self) -> &'static str {
        match self {
            Self::Pg => "OSTRO_TEST_PG_URL",
            Self::My => "OSTRO_TEST_MYSQL_URL",
        }
    }

    fn kind(self) -> &'static str {
        match self {
            Self::Pg => "sql-postgres",
            Self::My => "sql-mysql",
        }
    }

    fn insert(self) -> &'static str {
        match self {
            Self::Pg => "INSERT INTO orders(run_id, amount) VALUES ($1, $2)",
            Self::My => "INSERT INTO orders(run_id, amount) VALUES (?, ?)",
        }
    }

    fn create_orders(self) -> &'static str {
        match self {
            Self::Pg => "CREATE TABLE orders(run_id text PRIMARY KEY, amount bigint NOT NULL)",
            Self::My => {
                "CREATE TABLE orders(run_id varchar(64) PRIMARY KEY, amount bigint NOT NULL) \
                 ENGINE=InnoDB"
            }
        }
    }

    /// Token rows with their stored result as lowercase-able hex text.
    fn effects_hex(self) -> &'static str {
        match self {
            Self::Pg => "SELECT run_id, encode(result, 'hex') FROM ostro_effects ORDER BY run_id",
            Self::My => "SELECT run_id, HEX(result) FROM ostro_effects ORDER BY run_id",
        }
    }

    /// Transactions currently holding the token table's insert lock (inserted a token,
    /// not finished): what `before_user_stmt` holds.
    fn open_token_txs(self) -> &'static str {
        match self {
            Self::Pg => {
                "SELECT count(DISTINCT pid) FROM pg_locks \
                 WHERE relation = 'ostro_effects'::regclass AND mode = 'RowExclusiveLock' \
                 AND granted"
            }
            Self::My => {
                "SELECT count(DISTINCT ENGINE_TRANSACTION_ID) FROM performance_schema.data_locks \
                 WHERE OBJECT_SCHEMA = DATABASE() AND OBJECT_NAME = 'ostro_effects' \
                 AND LOCK_TYPE = 'TABLE' AND LOCK_MODE = 'IX'"
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// URL parsing (same form as ostro_testkit::db::parse_url, which this crate cannot import).
// ---------------------------------------------------------------------------------------

struct Url {
    host: String,
    port: Option<u16>,
    user: String,
    password: Option<String>,
    database: Option<String>,
    params: Vec<(String, String)>,
}

impl Url {
    fn param(&self, k: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.as_str())
    }
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (
                (b[i + 1] as char).to_digit(16),
                (b[i + 2] as char).to_digit(16),
            )
        {
            out.push((h * 16 + l) as u8);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_url(url: &str) -> Url {
    let (_, rest) = url.split_once("://").expect("URL has scheme://");
    let (main, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (authority, path) = match main.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (main, None),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let (user, password) = match userinfo {
        Some(u) => match u.split_once(':') {
            Some((n, p)) => (percent_decode(n), Some(percent_decode(p))),
            None => (percent_decode(u), None),
        },
        None => (String::new(), None),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || hostport.starts_with('[') => {
            (h.to_owned(), Some(p.parse().expect("URL port")))
        }
        _ => (hostport.to_owned(), None),
    };
    let params = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(kv), String::new()),
        })
        .collect();
    Url {
        host,
        port,
        user,
        password,
        database: path.filter(|p| !p.is_empty()).map(percent_decode),
        params,
    }
}

/// The URL of `d`, or `None` after a loud SKIP banner (recorded human_needed, D-FX5).
fn url_or_skip(d: Dialect, test: &str) -> Option<Url> {
    match std::env::var(d.env()) {
        Ok(v) if !v.trim().is_empty() => {
            let u = parse_url(v.trim());
            if let Some(p) = &u.password {
                assert!(
                    p.len() >= 8,
                    "the {} password must be at least 8 bytes (it travels as a sealed secret)",
                    d.env()
                );
            }
            Some(u)
        }
        _ => {
            eprintln!(
                "\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\
                 !!! SKIP (human_needed): {} unset: fx_sql_crash::{test} did not run\n\
                 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n",
                d.env()
            );
            None
        }
    }
}

/// The connection config (`"v": 1`) for `u`. `scope` is the PostgreSQL schema (W3) or the
/// MySQL database; `None` uses the URL's database and default schema (admin connection).
fn conn_config(d: Dialect, u: &Url, scope: Option<&str>, extra: Value) -> Value {
    let mut v = match d {
        Dialect::Pg => json!({
            "v": 1,
            "host": u.host,
            "port": u.port.unwrap_or(5432),
            "database": u.database.clone().unwrap_or_else(|| "postgres".into()),
            "user": u.user,
            "ssl_mode": match u.param("sslmode") {
                Some("disable") => "disable",
                Some("require") => "require",
                _ => "prefer",
            },
        }),
        Dialect::My => json!({
            "v": 1,
            "host": u.host,
            "port": u.port.unwrap_or(3306),
            "database": u.database.clone().unwrap_or_else(|| "mysql".into()),
            "user": u.user,
            "ssl_mode": match u.param("ssl-mode").map(str::to_ascii_uppercase).as_deref() {
                Some("DISABLED") => "disabled",
                Some("PREFERRED") => "preferred",
                Some("VERIFY_CA") => "verify_ca",
                Some("VERIFY_IDENTITY") => "verify_identity",
                _ => "required",
            },
        }),
    };
    if let Some(s) = scope {
        match d {
            Dialect::Pg => v["schema"] = s.into(),
            Dialect::My => v["database"] = s.into(),
        }
    }
    if u.password.is_some() {
        v["password"] = format!("{{{{secret:{PASS_SECRET}}}}}").into();
    }
    for (k, x) in extra.as_object().cloned().unwrap_or_default() {
        v[k] = x;
    }
    v
}

/// A unique identifier `t_<pid><nanos>` (plain identifier, <= 63 bytes).
fn unique_scope() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("t_{:x}{nanos:x}", std::process::id())
}

// ---------------------------------------------------------------------------------------
// The spawned node.
// ---------------------------------------------------------------------------------------

struct Node {
    srv: Spawned,
    api: Api,
}

impl Node {
    async fn start(cwd: &Path, data: &Path, failpoints: Option<&str>) -> Self {
        let addr = free_addr();
        let a = addr.to_string();
        let d = data.to_str().expect("utf-8 path");
        let mut cmd = dev(cwd, &["--data-dir", d, "--listen", &a]);
        cmd.env("OSTRO_NODE__PARTITIONS", "4")
            .env("OSTRO_LOG__LEVEL", "warn")
            .env_remove("FAILPOINTS");
        if let Some(fp) = failpoints {
            cmd.env("FAILPOINTS", fp);
        }
        let srv = Spawned::spawn(cmd);
        srv.wait_readyz(&http(), addr).await;
        Self {
            srv,
            api: Api::new(addr),
        }
    }

    async fn put(&self, path: &str, body: Value) {
        let r = self
            .api
            .http
            .put(self.api.url(path))
            .json(&body)
            .send()
            .await
            .unwrap_or_else(|e| panic!("PUT {path}: {e}"));
        let st = r.status();
        let text = r.text().await.unwrap_or_default();
        assert_eq!(st, reqwest::StatusCode::OK, "PUT {path}: {text}");
    }

    /// One fx-sql run with `exactly_once = false` (autocommit; the failpoints cover only
    /// exactly-once statements) on `connection`; returns its single result, panicking on an
    /// error result.
    async fn query(&self, connection: &str, stmt: &str) -> Value {
        let run = self
            .api
            .start_new(
                "fx-sql",
                json!({ "input": {
                    "connection": connection, "statement": stmt, "exactly_once": false
                }}),
            )
            .await;
        let fin = self.api.wait_terminal(&run, QUERY_LIMIT).await;
        assert_eq!(fin["status"], "completed", "{stmt}: {fin}");
        let r = fin["result"]["results"][0].clone();
        assert!(r.get("error").is_none(), "{stmt}: {r}");
        r
    }

    /// The single cell of a one-row, one-column query, as an integer.
    async fn scalar(&self, connection: &str, stmt: &str) -> i64 {
        let r = self.query(connection, stmt).await;
        let cell = &r["rows"][0][0];
        cell_text(cell)
            .parse()
            .unwrap_or_else(|e| panic!("{stmt}: not an integer ({e}): {r}"))
    }
}

/// A tagged fx-sql output cell as text (int8 / numeric / text / bytea as UTF-8).
fn cell_text(cell: &Value) -> String {
    if let Some(n) = cell.get("int8").and_then(Value::as_i64) {
        return n.to_string();
    }
    for k in ["numeric", "text"] {
        if let Some(s) = cell.get(k).and_then(Value::as_str) {
            return s.to_owned();
        }
    }
    if let Some(s) = cell.get("bytea").and_then(Value::as_str) {
        return String::from_utf8(b64_decode(s)).expect("utf-8 bytea cell");
    }
    panic!("unexpected cell {cell}");
}

fn b64_decode(s: &str) -> Vec<u8> {
    const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.trim_end_matches('=').bytes() {
        let v = B64.iter().position(|&x| x == c).expect("base64") as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    out
}

/// Poll `cond` on the node every 500 ms until it holds (or panic after `limit`).
async fn poll_scalar(node: &Node, conn: &str, stmt: &str, want: i64, limit: Duration) {
    let deadline = Instant::now() + limit;
    loop {
        let n = node.scalar(conn, stmt).await;
        if n == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "`{stmt}` = {n}, want {want} within {limit:?}; stderr: {}",
            node.srv.stderr()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ---------------------------------------------------------------------------------------
// The scenario.
// ---------------------------------------------------------------------------------------

async fn scenario(d: Dialect, site: Site, test: &str) {
    let Some(u) = url_or_skip(d, test) else {
        return;
    };
    let _g = SERIAL.lock().await;
    let t0 = Instant::now();
    let cwd = tempfile::tempdir().expect("cwd");
    let data = tempfile::tempdir().expect("data dir");
    let scope = unique_scope();

    // --- node 1, with the failpoint ---
    let node = Node::start(cwd.path(), data.path(), Some(site.failpoint())).await;
    let wasm = write_guest(cwd.path(), "fx-sql");
    let (code, body, err) = cli_json(
        node.api.addr,
        &["deploy", wasm.to_str().expect("utf-8 path"), "--activate"],
    );
    assert_eq!(code, 0, "deploy: {body}\n{err}");

    if let Some(p) = &u.password {
        node.put(
            &format!("/v1/secrets/{}", PASS_SECRET.replace('/', "%2F")),
            json!({ "value": p }),
        )
        .await;
    }
    // `admin`: the URL's database / default schema, for CREATE / DROP of the test scope.
    node.put(
        "/v1/connections/admin",
        json!({ "kind": d.kind(), "config": conn_config(d, &u, None, json!({"manage_schema": false})) }),
    )
    .await;
    node.put(
        "/v1/connections/orders",
        json!({ "kind": d.kind(), "config": conn_config(d, &u, Some(&scope), json!({"pool_max": 60})) }),
    )
    .await;
    node.put(
        "/v1/egress/fx-sql",
        json!({ "allow": ["connection:admin", "connection:orders"] }),
    )
    .await;

    match d {
        Dialect::Pg => node.query("admin", &format!("CREATE SCHEMA {scope}")).await,
        Dialect::My => {
            node.query("admin", &format!("CREATE DATABASE {scope}"))
                .await
        }
    };
    node.query("orders", d.create_orders()).await;

    // --- 50 exactly-once inserts, all held at the failpoint ---
    let mut runs: BTreeMap<String, (String, i64)> = BTreeMap::new(); // run id -> (key, amount)
    let t_start = Instant::now();
    for i in 0..N {
        let key = format!("k-{i:03}");
        let amount = 1000 + i as i64 * 7;
        let run = node
            .api
            .start_new(
                "fx-sql",
                json!({ "input": {
                    "connection": "orders",
                    "statement": d.insert(),
                    "params": [key, amount],
                    "exactly_once": true,
                }}),
            )
            .await;
        runs.insert(run, (key, amount));
    }
    assert_eq!(runs.len(), N);
    let n = N as i64;
    match site {
        Site::AfterCommit => {
            poll_scalar(
                &node,
                "orders",
                "SELECT count(*) FROM ostro_effects WHERE result IS NOT NULL",
                n,
                HOLD_LIMIT,
            )
            .await;
            // The user transactions committed; effect_completed is not journaled yet.
            assert_eq!(
                node.scalar("orders", "SELECT count(*) FROM orders").await,
                n
            );
        }
        Site::BeforeUserStmt => {
            poll_scalar(&node, "orders", d.open_token_txs(), n, HOLD_LIMIT).await;
            // Tokens inserted, nothing committed.
            assert_eq!(
                node.scalar("orders", "SELECT count(*) FROM ostro_effects")
                    .await,
                0
            );
            assert_eq!(
                node.scalar("orders", "SELECT count(*) FROM orders").await,
                0
            );
        }
    }
    // Still held: no run has finished, and the pool kept serving autocommit queries above.
    for run in runs.keys() {
        let r = node.api.run(run).await;
        assert!(
            !common::is_terminal(r["status"].as_str().unwrap_or("")),
            "{run} finished while held: {r}"
        );
        assert!(
            events_of(&node.api.history(run).await, "effect_completed").is_empty(),
            "{run}: effect_completed while held"
        );
    }
    let held = t_start.elapsed();

    // --- kill -9, restart without failpoints ---
    let Node { mut srv, .. } = node;
    srv.signal("KILL");
    let st = srv.wait_exit(common::T);
    assert!(!st.success(), "killed: {st:?}");
    drop(srv);
    let t_kill = Instant::now();
    let node = Node::start(cwd.path(), data.path(), None).await;
    let restart = t_kill.elapsed();
    let mut outputs = BTreeMap::new();
    for run in runs.keys() {
        let fin = node.api.wait_terminal(run, FINISH_LIMIT).await;
        outputs.insert(run.clone(), fin);
    }
    let finish = t_kill.elapsed();

    // --- exactly one row change per run ---
    for (run, fin) in &outputs {
        assert_eq!(fin["status"], "completed", "{run}: {fin}");
        assert_eq!(
            fin["result"],
            json!({"results": [{"columns": [], "rows": [], "rows_affected": 1}]}),
            "{run}"
        );
        let h = node.api.history(run).await;
        let sched = events_of(&h, "effect_scheduled");
        assert_eq!(sched.len(), 1, "{run}: {h:?}");
        assert_eq!(sched[0]["details"]["connection"], "orders", "{run}");
        assert_eq!(events_of(&h, "effect_completed").len(), 1, "{run}: {h:?}");
    }
    assert_eq!(
        node.scalar("orders", "SELECT count(*) FROM orders").await,
        n
    );
    let rows = node
        .query(
            "orders",
            "SELECT run_id, amount FROM orders ORDER BY run_id",
        )
        .await;
    let got: BTreeSet<(String, i64)> = rows["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| (cell_text(&r[0]), cell_text(&r[1]).parse().expect("amount")))
        .collect();
    let want: BTreeSet<(String, i64)> = runs.values().cloned().collect();
    assert_eq!(got, want, "user rows");

    // One token row per run, its stored result byte-identical to the journaled completion
    // the guest decoded (module doc).
    let effects = node.query("orders", d.effects_hex()).await;
    let tokens: BTreeMap<String, String> = effects["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| (cell_text(&r[0]), cell_text(&r[1]).to_ascii_lowercase()))
        .collect();
    assert_eq!(
        tokens.keys().cloned().collect::<BTreeSet<_>>(),
        runs.keys().cloned().collect::<BTreeSet<_>>(),
        "token rows by run id"
    );
    for (run, hex) in &tokens {
        assert_eq!(hex, STORED_INSERT_RESULT_HEX, "{run}: stored result");
    }

    // Drop the test scope (best effort; a failed assertion above leaves it behind).
    match d {
        Dialect::Pg => {
            node.query("admin", &format!("DROP SCHEMA {scope} CASCADE"))
                .await
        }
        Dialect::My => node.query("admin", &format!("DROP DATABASE {scope}")).await,
    };
    eprintln!(
        "fx_sql_crash::{test}: N={N} held in {held:?}, restart (readyz) {restart:?}, \
         all terminal {finish:?} after kill -9, total {:?}",
        t0.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_after_commit() {
    scenario(Dialect::Pg, Site::AfterCommit, "pg_after_commit").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_before_user_stmt() {
    scenario(Dialect::Pg, Site::BeforeUserStmt, "pg_before_user_stmt").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_after_commit() {
    scenario(Dialect::My, Site::AfterCommit, "mysql_after_commit").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_before_user_stmt() {
    scenario(Dialect::My, Site::BeforeUserStmt, "mysql_before_user_stmt").await;
}

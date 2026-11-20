//! SQL connector (06-11) against MySQL 8.4: the same cases as `sql_pg.rs` (token-first
//! exactly-once, gate, error mapping, caps, value round trips, GC, B4 reconcile, W4 pool
//! release, W3 per-connection database).
//!
//! Needs `OSTRO_TEST_MYSQL_URL` (with `ssl-mode=REQUIRED`; D-FX5). Without it every test
//! prints `SKIP: OSTRO_TEST_MYSQL_URL unset (human_needed / CI service)` and passes. Each test
//! works in its own `t_<uuid>` database. Run with `--test-threads=1` when built with
//! `failpoints`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use ostro_bridge::connector::{Connector, EffectCtx, GateDecision, SendGate};
use ostro_bridge::kinds;
use ostro_bridge::meta_view::StaticMetaView;
use ostro_bridge::outcome::encode_outcome;
use ostro_bridge::secrets::{ProviderChain, ProviderError, SecretProvider, SecretResolver};
use ostro_bridge::sql::exactly_once::encode_stored;
use ostro_bridge::sql::{Reconcile, SqlConfig, SqlConnector, effect_key};
use ostro_meta::records::{Connection, EgressPolicy};
use ostro_testkit::db::{self, MyTestDb};
use ostro_types::journal::{
    EffectRequest, ErrorInfo, Failure, OpOutput, SqlRequest, SqlResult, SqlValue,
};
use ostro_types::live::LiveEffect;
use secrecy::SecretBox;

const MODULE: &str = "t/m";
const CLUSTER: &str = "test-cluster";
const PASS_SECRET: &str = "mysql-pass";

// ---- fixtures ----

struct MapProvider(BTreeMap<String, Vec<u8>>);

#[async_trait]
impl SecretProvider for MapProvider {
    fn name(&self) -> &'static str {
        "map"
    }
    async fn get(&self, name: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
        Ok(self
            .0
            .get(name)
            .map(|v| SecretBox::new(v.clone().into_boxed_slice())))
    }
}

struct Gate(GateDecision);

#[async_trait]
impl SendGate for Gate {
    async fn check(&self, _: u16, _: &str, _: u64) -> GateDecision {
        self.0
    }
}

const PROCEED: Gate = Gate(GateDecision::Proceed);

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn run_id(tag: &str) -> String {
    format!("run-{tag}-{}", now_nanos())
}

/// A connection config for `db`'s server on `database`, plus `extra` keys.
fn conn_config(db: &MyTestDb, database: &str, extra: serde_json::Value) -> Vec<u8> {
    let p = db.parts();
    let ssl = match p.param("ssl-mode").map(str::to_ascii_uppercase).as_deref() {
        Some("DISABLED") => "disabled",
        Some("PREFERRED") => "preferred",
        Some("VERIFY_CA") => "verify_ca",
        Some("VERIFY_IDENTITY") => "verify_identity",
        _ => "required",
    };
    let mut v = serde_json::json!({
        "v": 1,
        "host": p.host,
        "port": p.port.unwrap_or(3306),
        "database": database,
        "user": p.user,
        "ssl_mode": ssl,
    });
    if p.password.is_some() {
        v["password"] = format!("{{{{secret:{PASS_SECRET}}}}}").into();
    }
    for (k, x) in extra.as_object().cloned().unwrap_or_default() {
        v[k] = x;
    }
    serde_json::to_vec(&v).unwrap()
}

fn connector(conns: Vec<(&str, Vec<u8>)>, password: Option<&str>) -> Arc<SqlConnector> {
    let mut meta = StaticMetaView::new();
    let mut allow = Vec::new();
    for (name, cfg) in conns {
        allow.push(format!("connection:{name}"));
        meta = meta.insert_connection(Connection {
            name: name.into(),
            kind: "sql-mysql".into(),
            config: cfg,
            ext: vec![],
        });
    }
    meta = meta.insert_egress_policy(EgressPolicy {
        name: MODULE.into(),
        allow,
        ext: vec![],
    });
    let meta = Arc::new(meta);
    let mut secrets = BTreeMap::new();
    if let Some(p) = password {
        assert!(
            p.len() >= 8,
            "the OSTRO_TEST_MYSQL_URL password must be at least 8 bytes (templated secret minimum)"
        );
        secrets.insert(PASS_SECRET.to_owned(), p.as_bytes().to_vec());
    }
    let resolver = SecretResolver::new(
        meta.clone(),
        ProviderChain::new(vec![Arc::new(MapProvider(secrets))]),
    );
    let cfg = SqlConfig {
        reconcile: Reconcile {
            attempts: 5,
            initial: Duration::from_millis(20),
            max: Duration::from_millis(200),
        },
        ..SqlConfig::default()
    };
    Arc::new(SqlConnector::new(meta, resolver, CLUSTER, cfg))
}

struct Fx {
    conn: Arc<SqlConnector>,
}

impl Fx {
    fn new(db: &MyTestDb, extra: Vec<(&str, serde_json::Value)>) -> Self {
        let mut conns = vec![("db", conn_config(db, db.database(), serde_json::json!({})))];
        for (name, x) in extra {
            conns.push((name, conn_config(db, db.database(), x)));
        }
        Self {
            conn: connector(conns, db.parts().password.as_deref()),
        }
    }

    async fn exec(
        &self,
        connection: &str,
        run: &str,
        op: u64,
        stmt: &str,
        params: Vec<SqlValue>,
        exactly_once: bool,
    ) -> Result<OpOutput, Failure> {
        exec(
            &self.conn,
            connection,
            run,
            op,
            stmt,
            params,
            exactly_once,
            false,
        )
        .await
    }
}

fn ctx(run: &str) -> EffectCtx {
    EffectCtx {
        cluster_id: CLUSTER.into(),
        partition: 0,
        run_id: run.into(),
        attempt: 1,
        module: MODULE.into(),
        tenant: None,
        cancel: tokio_util::sync::CancellationToken::new(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn exec(
    conn: &SqlConnector,
    connection: &str,
    run: &str,
    op: u64,
    stmt: &str,
    params: Vec<SqlValue>,
    exactly_once: bool,
    redrive: bool,
) -> Result<OpOutput, Failure> {
    let effect = LiveEffect {
        op,
        request: EffectRequest::Sql(SqlRequest {
            connection: connection.into(),
            statement: stmt.into(),
            params,
            exactly_once,
        }),
        http_retry: None,
        scheduled_ts_nanos: now_nanos(),
        redrive,
    };
    conn.execute(&ctx(run), &effect, &PROCEED).await
}

fn sql(r: Result<OpOutput, Failure>) -> SqlResult {
    match r {
        Ok(OpOutput::Sql(s)) => s,
        other => panic!("sql result expected, got {other:?}"),
    }
}

fn nonretryable(r: Result<OpOutput, Failure>) -> ErrorInfo {
    match r {
        Err(Failure::NonRetryable(e)) => e,
        other => panic!("non-retryable expected, got {other:?}"),
    }
}

fn details(e: &ErrorInfo) -> serde_json::Value {
    serde_json::from_slice(&e.details.as_ref().expect("details").data).unwrap()
}

async fn count(db: &MyTestDb, q: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(q.to_owned()))
        .fetch_one(db.pool())
        .await
        .unwrap()
}

async fn ddl(db: &MyTestDb, q: &str) {
    sqlx::query(sqlx::AssertSqlSafe(q.to_owned()))
        .execute(db.pool())
        .await
        .unwrap();
}

async fn stored(db: &MyTestDb, run: &str, op: u64) -> Option<Option<Vec<u8>>> {
    sqlx::query_scalar::<_, Option<Vec<u8>>>(
        "SELECT result FROM ostro_effects WHERE effect_key = ?",
    )
    .bind(effect_key(CLUSTER, run, op).to_vec())
    .fetch_optional(db.pool())
    .await
    .unwrap()
}

// ---- MySQL ----

#[tokio::test]
async fn mysql_token_first_redrive_returns_stored_result() {
    let Some(db) = db::mysql().await else { return };
    let fx = Fx::new(&db, vec![]);
    ddl(
        &db,
        "CREATE TABLE items (id INT PRIMARY KEY, v TEXT) ENGINE=InnoDB",
    )
    .await;
    let run = run_id("token");
    let stmt = "INSERT INTO items (id, v) VALUES (?, ?)";
    let params = vec![SqlValue::Int8(1), SqlValue::Text("one".into())];
    let live = fx.exec("db", &run, 7, stmt, params.clone(), true).await;
    let first = sql(live.clone());
    assert_eq!((first.rows_affected, first.columns.len()), (1, 0));
    let bytes = stored(&db, &run, 7).await.unwrap().unwrap();
    assert_eq!(bytes, encode_stored(&encode_outcome(&live)));

    let again = exec(&fx.conn, "db", &run, 7, stmt, params, true, true).await;
    assert_eq!(again, live);
    assert_eq!(encode_stored(&encode_outcome(&again)), bytes);
    assert_eq!(count(&db, "SELECT count(*) FROM items").await, 1);

    let e = nonretryable(
        fx.exec(
            "db",
            &run,
            8,
            stmt,
            vec![SqlValue::Int8(1), SqlValue::Null],
            true,
        )
        .await,
    );
    assert_eq!(e.kind, kinds::SQL_ERROR);
    assert_eq!(details(&e)["errno"], 1062);
    assert_eq!(stored(&db, &run, 8).await, None, "token rolled back");
}

#[tokio::test]
async fn mysql_null_result_is_violation() {
    let Some(db) = db::mysql().await else { return };
    let fx = Fx::new(&db, vec![]);
    ddl(&db, "CREATE TABLE items (id INT PRIMARY KEY) ENGINE=InnoDB").await;
    let run = run_id("null");
    sql(fx.exec("db", &run, 1, "SELECT 1", vec![], true).await);
    sqlx::query("INSERT INTO ostro_effects (effect_key, run_id, op) VALUES (?, ?, 2)")
        .bind(effect_key(CLUSTER, &run, 2).to_vec())
        .bind(&run)
        .execute(db.pool())
        .await
        .unwrap();
    let e = nonretryable(
        fx.exec("db", &run, 2, "INSERT INTO items VALUES (1)", vec![], true)
            .await,
    );
    assert_eq!(e.kind, kinds::SQL_EXACTLY_ONCE_VIOLATED);
    assert_eq!(count(&db, "SELECT count(*) FROM items").await, 0);
}

#[tokio::test]
async fn mysql_alter_table_is_refused_and_nothing_runs() {
    let Some(db) = db::mysql().await else { return };
    let fx = Fx::new(&db, vec![]);
    ddl(&db, "CREATE TABLE items (id INT PRIMARY KEY) ENGINE=InnoDB").await;
    let run = run_id("alter");
    sql(fx.exec("db", &run, 1, "SELECT 1", vec![], true).await);
    for stmt in [
        "ALTER TABLE items ADD COLUMN x INT",
        "/*!50000 ALTER TABLE items ADD COLUMN x INT */",
        "CREATE TEMPORARY TABLE tt (x INT)",
        "LOCK TABLES items WRITE",
    ] {
        let e = nonretryable(fx.exec("db", &run, 2, stmt, vec![], true).await);
        assert_eq!(e.kind, kinds::SQL_NOT_TRANSACTIONAL, "{stmt}");
    }
    let cols = count(
        &db,
        "SELECT count(*) FROM information_schema.columns \
         WHERE table_schema = DATABASE() AND table_name = 'items'",
    )
    .await;
    assert_eq!(cols, 1, "the column was not added");
    assert_eq!(stored(&db, &run, 2).await, None, "no token row");
}

#[tokio::test]
async fn mysql_deadlock_and_signalled_codes_are_retryable() {
    let Some(db) = db::mysql().await else { return };
    let fx = Fx::new(&db, vec![]);
    let run = run_id("retry");
    for (op, signal) in [
        (1u64, "SQLSTATE '40001' SET MESSAGE_TEXT = 'forced'"),
        (
            2,
            "SQLSTATE 'HY000' SET MYSQL_ERRNO = 1205, MESSAGE_TEXT = 'forced lock wait timeout'",
        ),
    ] {
        ddl(
            &db,
            &format!(
                "CREATE FUNCTION boom_{op}() RETURNS INT NO SQL DETERMINISTIC \
                 BEGIN SIGNAL {signal}; RETURN 1; END"
            ),
        )
        .await;
        let r = fx
            .exec("db", &run, op, &format!("SELECT boom_{op}()"), vec![], true)
            .await;
        assert!(matches!(r, Err(Failure::Retryable(_))), "{signal}: {r:?}");
        assert_eq!(stored(&db, &run, op).await, None);
    }

    // A real deadlock. The test's transaction modifies many rows, so InnoDB picks the effect's
    // (lighter) transaction as the victim.
    ddl(
        &db,
        "CREATE TABLE items (id INT PRIMARY KEY, v TEXT) ENGINE=InnoDB",
    )
    .await;
    ddl(&db, "INSERT INTO items VALUES (1, 'x'), (2, 'x')").await;
    ddl(
        &db,
        "CREATE TABLE filler (id INT PRIMARY KEY, v INT) ENGINE=InnoDB",
    )
    .await;
    for i in 0..50 {
        ddl(&db, &format!("INSERT INTO filler VALUES ({i}, 0)")).await;
    }
    let mut a = db.pool().begin().await.unwrap();
    sqlx::query("UPDATE filler SET v = v + 1")
        .execute(&mut *a)
        .await
        .unwrap();
    sqlx::query("UPDATE items SET v = 'a' WHERE id = 1")
        .execute(&mut *a)
        .await
        .unwrap();
    let conn = fx.conn.clone();
    let run2 = run.clone();
    let b = tokio::spawn(async move {
        exec(
            &conn,
            "db",
            &run2,
            3,
            "UPDATE items SET v = 'b' WHERE id IN (1, 2) ORDER BY id DESC",
            vec![],
            true,
            false,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let a_second = sqlx::query("UPDATE items SET v = 'a' WHERE id = 2")
        .execute(&mut *a)
        .await;
    let r = b.await.unwrap();
    let Err(Failure::Retryable(e)) = r else {
        panic!("deadlock must be retryable, got {r:?} (test side: {a_second:?})")
    };
    assert_eq!(details(&e)["errno"], 1213);
    a.rollback().await.unwrap();
    assert_eq!(stored(&db, &run, 3).await, None);
}

#[tokio::test]
async fn mysql_cap_is_response_too_large_without_token() {
    let Some(db) = db::mysql().await else { return };
    let fx = Fx::new(
        &db,
        vec![
            ("small", serde_json::json!({"max_rows": 2})),
            ("tiny", serde_json::json!({"max_bytes": 64})),
        ],
    );
    let run = run_id("cap");
    sql(fx.exec("db", &run, 1, "SELECT 1", vec![], true).await);
    let five = "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 \
                UNION ALL SELECT 5";
    let e = nonretryable(fx.exec("small", &run, 2, five, vec![], true).await);
    assert_eq!(e.kind, kinds::RESPONSE_TOO_LARGE);
    assert_eq!(stored(&db, &run, 2).await, None);
    let e = nonretryable(
        fx.exec(
            "tiny",
            &run,
            4,
            "SELECT REPEAT('x', 200) AS s",
            vec![],
            true,
        )
        .await,
    );
    assert_eq!(e.kind, kinds::RESPONSE_TOO_LARGE);
    assert_eq!(stored(&db, &run, 4).await, None);
}

#[tokio::test]
async fn mysql_value_round_trip() {
    let Some(db) = db::mysql().await else { return };
    let fx = Fx::new(&db, vec![]);
    ddl(
        &db,
        "CREATE TABLE vals (b BOOLEAN, i BIGINT, f DOUBLE, n DECIMAL(30,3), t TEXT, bl BLOB, \
         j JSON, ts TIMESTAMP(6) NULL, nul INT) ENGINE=InnoDB",
    )
    .await;
    let run = run_id("values");
    let values = vec![
        SqlValue::Boolean(true),
        SqlValue::Int8(-9_000_000_000),
        SqlValue::Float8(2.5),
        SqlValue::Numeric("12345678901234567890.125".into()),
        SqlValue::Text("héllo".into()),
        SqlValue::Bytea(vec![0, 1, 254, 255]),
        SqlValue::Json(r#"{"a": [1, true, null]}"#.into()),
        SqlValue::Timestamptz {
            seconds: 1_700_000_000,
            nanoseconds: 123_456_000,
        },
        SqlValue::Null,
    ];
    let ins = sql(fx
        .exec(
            "db",
            &run,
            1,
            "INSERT INTO vals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values.clone(),
            true,
        )
        .await);
    assert_eq!(ins.rows_affected, 1);
    let out = sql(fx
        .exec("db", &run, 2, "SELECT * FROM vals", vec![], false)
        .await);
    assert_eq!(
        out.columns,
        vec!["b", "i", "f", "n", "t", "bl", "j", "ts", "nul"]
    );
    assert_eq!(out.rows, vec![values]);

    let more = sql(fx
        .exec(
            "db",
            &run,
            3,
            "SELECT CAST(18446744073709551615 AS UNSIGNED) AS u, CAST(7 AS UNSIGNED) AS v, \
             CAST('1970-01-02 00:00:00' AS DATETIME) AS d",
            vec![],
            false,
        )
        .await);
    assert_eq!(
        more.rows,
        vec![vec![
            SqlValue::Numeric("18446744073709551615".into()),
            SqlValue::Int8(7),
            SqlValue::Timestamptz {
                seconds: 86_400,
                nanoseconds: 0
            },
        ]]
    );
    let e = nonretryable(
        fx.exec("db", &run, 4, "SELECT CURDATE() AS d", vec![], false)
            .await,
    );
    assert_eq!(e.kind, kinds::SQL_UNSUPPORTED_TYPE);
    assert_eq!(
        details(&e),
        serde_json::json!({"column": "d", "type": "DATE"})
    );
}

#[tokio::test]
async fn mysql_gc_deletes_old_token_rows() {
    let Some(db) = db::mysql().await else { return };
    let fx = Fx::new(&db, vec![]);
    let run = run_id("gc");
    sql(fx.exec("db", &run, 1, "SELECT 1", vec![], true).await);
    for op in 2..5u64 {
        sqlx::query(
            "INSERT INTO ostro_effects (effect_key, run_id, op, result, created_at) \
             VALUES (?, ?, ?, X'01', NOW(6) - INTERVAL 10 DAY)",
        )
        .bind(effect_key(CLUSTER, &run, op).to_vec())
        .bind(&run)
        .bind(op)
        .execute(db.pool())
        .await
        .unwrap();
    }
    assert_eq!(count(&db, "SELECT count(*) FROM ostro_effects").await, 4);
    let n = fx.conn.gc_once(Duration::from_secs(7 * 24 * 3600)).await;
    assert_eq!(n, 3);
    assert_eq!(count(&db, "SELECT count(*) FROM ostro_effects").await, 1);
    let h = fx.conn.spawn_gc(Duration::from_secs(3600));
    tokio::time::timeout(Duration::from_secs(5), h.stop())
        .await
        .expect("GC task stops when cancelled");
}

#[tokio::test]
async fn mysql_database_isolates_connections() {
    // W3 analogue: two connections on two databases see only their own tables.
    let Some(a) = db::mysql().await else { return };
    let Some(b) = db::mysql().await else { return };
    let conn = connector(
        vec![
            ("a", conn_config(&a, a.database(), serde_json::json!({}))),
            ("b", conn_config(&a, b.database(), serde_json::json!({}))),
        ],
        a.parts().password.as_deref(),
    );
    ddl(&a, "CREATE TABLE only_here (x INT)").await;
    let run = run_id("db");
    let ok = sql(exec(
        &conn,
        "a",
        &run,
        1,
        "SELECT count(*) AS n FROM only_here",
        vec![],
        false,
        false,
    )
    .await);
    assert_eq!(ok.rows, vec![vec![SqlValue::Int8(0)]]);
    let e = nonretryable(
        exec(
            &conn,
            "b",
            &run,
            2,
            "SELECT count(*) FROM only_here",
            vec![],
            false,
            false,
        )
        .await,
    );
    assert_eq!(details(&e)["errno"], 1146);
    sql(exec(&conn, "b", &run, 3, "SELECT 1", vec![], true, false).await);
    assert_eq!(count(&b, "SELECT count(*) FROM ostro_effects").await, 1);
}

#[cfg(feature = "failpoints")]
mod failpoints {
    use super::*;

    #[tokio::test]
    async fn mysql_after_commit_error_reconciles_to_stored_bytes() {
        let Some(db) = db::mysql().await else { return };
        let fx = Fx::new(&db, vec![]);
        ddl(&db, "CREATE TABLE items (id INT PRIMARY KEY) ENGINE=InnoDB").await;
        let run = run_id("b4");
        let scenario = fail::FailScenario::setup();
        fail::cfg("bridge::sql::after_commit", "return(error)").unwrap();
        let r = fx
            .exec("db", &run, 1, "INSERT INTO items VALUES (1)", vec![], true)
            .await;
        fail::remove("bridge::sql::after_commit");
        scenario.teardown();
        let out = r.clone().expect("Ok after reconcile (never Retryable)");
        let bytes = stored(&db, &run, 1).await.unwrap().unwrap();
        assert_eq!(encode_stored(&encode_outcome(&Ok(out))), bytes);
        assert_eq!(count(&db, "SELECT count(*) FROM items").await, 1);
    }

    #[tokio::test]
    async fn mysql_pool_is_released_before_after_commit_failpoint() {
        let Some(db) = db::mysql().await else { return };
        let fx = Fx::new(&db, vec![("pool2", serde_json::json!({"pool_max": 2}))]);
        ddl(&db, "CREATE TABLE items (id INT PRIMARY KEY) ENGINE=InnoDB").await;
        let run = run_id("w4");
        sql(fx.exec("pool2", &run, 100, "SELECT 1", vec![], true).await);
        let scenario = fail::FailScenario::setup();
        fail::cfg("bridge::sql::after_commit", "return(hang)").unwrap();
        let mut tasks = Vec::new();
        for op in 1..=10u64 {
            let conn = fx.conn.clone();
            let run = run.clone();
            tasks.push(tokio::spawn(async move {
                exec(
                    &conn,
                    "pool2",
                    &run,
                    op,
                    "INSERT INTO items VALUES (?)",
                    vec![SqlValue::Int8(op as i64)],
                    true,
                    false,
                )
                .await
            }));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let done = count(
                &db,
                &format!(
                    "SELECT count(*) FROM ostro_effects WHERE run_id = '{run}' \
                     AND op BETWEEN 1 AND 10 AND result IS NOT NULL"
                ),
            )
            .await;
            if done == 10 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "only {done}/10 effects committed: a hung effect holds a pooled connection"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let r = tokio::time::timeout(
            Duration::from_secs(15),
            fx.exec(
                "pool2",
                &run,
                11,
                "SELECT count(*) AS n FROM items",
                vec![],
                false,
            ),
        )
        .await
        .expect("the 11th (autocommit) statement completes");
        assert_eq!(sql(r).rows, vec![vec![SqlValue::Int8(10)]]);
        assert!(tasks.iter().all(|t| !t.is_finished()), "all 10 still hang");
        fail::remove("bridge::sql::after_commit");
        scenario.teardown();
        for t in tasks {
            t.abort();
        }
    }
}

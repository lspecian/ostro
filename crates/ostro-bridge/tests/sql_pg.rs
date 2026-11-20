//! SQL connector (06-11) against PostgreSQL: the token-first exactly-once protocol, the
//! statement gate, error mapping, caps, value round trips, GC, the after-COMMIT reconcile loop
//! (B4), pool release before the failpoint (W4), per-connection `schema` (W3) and the
//! re-drive horizon (W12).
//!
//! Needs `OSTRO_TEST_PG_URL` (P-17); without it the DB tests print `SKIP` and pass. Each test
//! works in its own `t_<uuid>` schema. Run with `--test-threads=1` when built with
//! `failpoints` (failpoint config is process-global).

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
use ostro_testkit::db::{self, PgTestDb};
use ostro_types::journal::{
    EffectRequest, ErrorInfo, Failure, OpOutput, SqlRequest, SqlResult, SqlValue,
};
use ostro_types::live::LiveEffect;
use secrecy::SecretBox;

const MODULE: &str = "t/m";
const CLUSTER: &str = "test-cluster";
const PASS_SECRET: &str = "pg-pass";
const HOUR: u64 = 3_600_000_000_000;

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

/// A connection config for `db`'s server, using `schema`, plus `extra` keys.
fn conn_config(db: &PgTestDb, schema: &str, extra: serde_json::Value) -> Vec<u8> {
    let p = db.parts();
    let mut v = serde_json::json!({
        "v": 1,
        "host": p.host,
        "port": p.port.unwrap_or(5432),
        "database": p.database.clone().unwrap_or_else(|| "postgres".into()),
        "schema": schema,
        "user": p.user,
        "ssl_mode": match p.param("sslmode") {
            Some("disable") => "disable",
            Some("require") => "require",
            _ => "prefer",
        },
    });
    if p.password.is_some() {
        v["password"] = format!("{{{{secret:{PASS_SECRET}}}}}").into();
    }
    for (k, x) in extra.as_object().cloned().unwrap_or_default() {
        v[k] = x;
    }
    serde_json::to_vec(&v).unwrap()
}

struct Fx {
    conn: Arc<SqlConnector>,
}

fn connector(conns: Vec<(&str, Vec<u8>)>, password: Option<&str>) -> Arc<SqlConnector> {
    let mut meta = StaticMetaView::new();
    let mut allow = Vec::new();
    for (name, cfg) in conns {
        allow.push(format!("connection:{name}"));
        meta = meta.insert_connection(Connection {
            name: name.into(),
            kind: "sql-postgres".into(),
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
            "the OSTRO_TEST_PG_URL password must be at least 8 bytes (templated secret minimum)"
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

impl Fx {
    /// One connection `db` on the test schema (plus `extra` connections).
    fn new(db: &PgTestDb, extra: Vec<(&str, serde_json::Value)>) -> Self {
        let mut conns = vec![("db", conn_config(db, db.schema(), serde_json::json!({})))];
        for (name, x) in extra {
            conns.push((name, conn_config(db, db.schema(), x)));
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
            now_nanos(),
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

fn effect(
    connection: &str,
    op: u64,
    stmt: &str,
    params: Vec<SqlValue>,
    exactly_once: bool,
    redrive: bool,
    scheduled: u64,
) -> LiveEffect {
    LiveEffect {
        op,
        request: EffectRequest::Sql(SqlRequest {
            connection: connection.into(),
            statement: stmt.into(),
            params,
            exactly_once,
        }),
        http_retry: None,
        scheduled_ts_nanos: scheduled,
        redrive,
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
    scheduled: u64,
) -> Result<OpOutput, Failure> {
    conn.execute(
        &ctx(run),
        &effect(
            connection,
            op,
            stmt,
            params,
            exactly_once,
            redrive,
            scheduled,
        ),
        &PROCEED,
    )
    .await
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

async fn count(db: &PgTestDb, q: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(q.to_owned()))
        .fetch_one(db.pool())
        .await
        .unwrap()
}

async fn ddl(db: &PgTestDb, q: &str) {
    sqlx::query(sqlx::AssertSqlSafe(q.to_owned()))
        .execute(db.pool())
        .await
        .unwrap();
}

async fn stored(db: &PgTestDb, run: &str, op: u64) -> Option<Option<Vec<u8>>> {
    sqlx::query_scalar::<_, Option<Vec<u8>>>(
        "SELECT result FROM ostro_effects WHERE effect_key = $1",
    )
    .bind(effect_key(CLUSTER, run, op).to_vec())
    .fetch_optional(db.pool())
    .await
    .unwrap()
}

// ---- no database needed ----

#[tokio::test]
async fn sql_policy_and_unknown_connection() {
    let conn = connector(
        vec![(
            "db",
            br#"{"v":1,"host":"127.0.0.1","port":1,"database":"d","user":"u"}"#.to_vec(),
        )],
        None,
    );
    // Not in the module's egress policy.
    let e = nonretryable(exec(&conn, "other", "r", 1, "SELECT 1", vec![], false, false, 0).await);
    assert_eq!(e.kind, kinds::EGRESS_DENIED);
    // Allowed but unknown: a fresh connector whose policy names a connection that is absent.
    let meta = Arc::new(StaticMetaView::new().insert_egress_policy(EgressPolicy {
        name: MODULE.into(),
        allow: vec!["connection:ghost".into()],
        ext: vec![],
    }));
    let resolver = SecretResolver::new(meta.clone(), ProviderChain::new(vec![]));
    let ghost = SqlConnector::new(meta, resolver, CLUSTER, SqlConfig::default());
    let e = nonretryable(exec(&ghost, "ghost", "r", 1, "SELECT 1", vec![], false, false, 0).await);
    assert_eq!(e.kind, kinds::CONNECTION_UNKNOWN);
    // The gate refuses before any connection attempt (port 1 would fail with sql_error).
    let e = nonretryable(
        exec(
            &conn,
            "db",
            "r",
            1,
            "ALTER TABLE t ADD c int",
            vec![],
            true,
            false,
            0,
        )
        .await,
    );
    assert_eq!(e.kind, kinds::SQL_NOT_TRANSACTIONAL);
    // Abandoned by the send gate: Cancelled, nothing sent.
    let r = conn
        .execute(
            &ctx("r"),
            &effect("db", 1, "SELECT 1", vec![], true, false, now_nanos()),
            &Gate(GateDecision::Abandon),
        )
        .await;
    assert_eq!(r, Err(Failure::Cancelled));
}

#[tokio::test]
async fn sql_redrive_past_horizon_never_connects() {
    // W12: a re-drive older than retention - 1h completes effect_outcome_unknown without a
    // database round trip (port 1 refuses connections: a connect would give sql_error).
    let conn = connector(
        vec![(
            "db",
            br#"{"v":1,"host":"127.0.0.1","port":1,"database":"d","user":"u","ssl_mode":"disable"}"#
                .to_vec(),
        )],
        None,
    );
    let old = now_nanos() - 168 * HOUR;
    let e = nonretryable(exec(&conn, "db", "r", 4, "SELECT 1", vec![], true, true, old).await);
    assert_eq!(e.kind, kinds::EFFECT_OUTCOME_UNKNOWN);
    assert_eq!(e.message, "re-drive older than the ostro_effects retention");
    assert_eq!(
        details(&e),
        serde_json::json!({"op": 4, "connection": "db"})
    );
    // Inside the horizon the connector does connect (and fails on port 1).
    let recent = now_nanos() - HOUR;
    let r = exec(&conn, "db", "r", 4, "SELECT 1", vec![], true, true, recent).await;
    match r {
        Err(Failure::Retryable(e)) | Err(Failure::NonRetryable(e)) => {
            assert_eq!(e.kind, kinds::SQL_ERROR)
        }
        other => panic!("{other:?}"),
    }
}

// ---- PostgreSQL ----

#[tokio::test]
async fn pg_token_first_redrive_returns_stored_result() {
    let Some(db) = db::pg().await else { return };
    let fx = Fx::new(&db, vec![]);
    ddl(&db, "CREATE TABLE items (id int PRIMARY KEY, v text)").await;
    let run = run_id("token");
    let stmt = "INSERT INTO items (id, v) VALUES ($1, $2) RETURNING id, v";
    let params = vec![SqlValue::Int8(1), SqlValue::Text("one".into())];
    let live = fx.exec("db", &run, 7, stmt, params.clone(), true).await;
    let first = sql(live.clone());
    assert_eq!(first.columns, vec!["id", "v"]);
    assert_eq!(
        first.rows,
        vec![vec![SqlValue::Int8(1), SqlValue::Text("one".into())]]
    );
    assert_eq!(first.rows_affected, 1);
    // The stored bytes equal the completion bytes.
    let bytes = stored(&db, &run, 7).await.unwrap().unwrap();
    assert_eq!(bytes, encode_stored(&encode_outcome(&live)));

    // Re-drive of the same (run, op): the stored result, verbatim; the insert is not re-run.
    let again = exec(
        &fx.conn,
        "db",
        &run,
        7,
        stmt,
        params,
        true,
        true,
        now_nanos(),
    )
    .await;
    assert_eq!(again, live);
    assert_eq!(encode_stored(&encode_outcome(&again)), bytes);
    assert_eq!(count(&db, "SELECT count(*) FROM items").await, 1);

    // A different op runs (and fails on the primary key, rolling its token back).
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
    assert_eq!(details(&e)["sqlstate"], "23505");
    assert_eq!(stored(&db, &run, 8).await, None, "token rolled back");
}

#[tokio::test]
async fn pg_null_result_is_violation() {
    let Some(db) = db::pg().await else { return };
    let fx = Fx::new(&db, vec![]);
    ddl(&db, "CREATE TABLE items (id int PRIMARY KEY)").await;
    let run = run_id("null");
    sql(fx.exec("db", &run, 1, "SELECT 1", vec![], true).await); // builds the token table
    sqlx::query("INSERT INTO ostro_effects (effect_key, run_id, op) VALUES ($1, $2, 2)")
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
async fn pg_alter_table_is_refused_and_nothing_runs() {
    let Some(db) = db::pg().await else { return };
    let fx = Fx::new(&db, vec![]);
    ddl(&db, "CREATE TABLE items (id int PRIMARY KEY)").await;
    let run = run_id("alter");
    sql(fx.exec("db", &run, 1, "SELECT 1", vec![], true).await);
    let e = nonretryable(
        fx.exec(
            "db",
            &run,
            2,
            "ALTER TABLE items ADD COLUMN x int",
            vec![],
            true,
        )
        .await,
    );
    assert_eq!(e.kind, kinds::SQL_NOT_TRANSACTIONAL);
    assert_eq!(e.message, "ALTER is not allowed in exactly-once mode");
    let cols = count(
        &db,
        "SELECT count(*) FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'items'",
    )
    .await;
    assert_eq!(cols, 1, "the column was not added");
    assert_eq!(stored(&db, &run, 2).await, None, "no token row");
}

#[tokio::test]
async fn pg_serialization_and_deadlock_are_retryable() {
    let Some(db) = db::pg().await else { return };
    let fx = Fx::new(&db, vec![]);
    let run = run_id("retry");
    // Raised codes (deterministic).
    for (op, code) in [(1u64, "40001"), (2, "40P01")] {
        ddl(
            &db,
            &format!(
                "CREATE FUNCTION boom_{op}() RETURNS int LANGUAGE plpgsql AS \
                 $$ BEGIN RAISE EXCEPTION USING ERRCODE = '{code}', MESSAGE = 'forced'; END $$"
            ),
        )
        .await;
        let r = fx
            .exec("db", &run, op, &format!("SELECT boom_{op}()"), vec![], true)
            .await;
        let Err(Failure::Retryable(e)) = r else {
            panic!("{code} must be retryable, got {r:?}")
        };
        assert_eq!(e.kind, kinds::SQL_ERROR);
        assert_eq!(details(&e)["sqlstate"], code);
        assert_eq!(stored(&db, &run, op).await, None, "token rolled back");
    }

    // A real deadlock between two transactions. Row 1 is locked by the test; the effect locks
    // row 2 then waits for row 1; the test then waits for row 2. The effect waited first, so
    // its deadlock check (deadlock_timeout) fires first and it is the one aborted.
    ddl(&db, "CREATE TABLE items (id int PRIMARY KEY, v text)").await;
    ddl(&db, "INSERT INTO items VALUES (1, 'x'), (2, 'x')").await;
    let mut a = db.pool().begin().await.unwrap();
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
            "UPDATE items SET v = 'b' WHERE id IN \
             (SELECT id FROM items WHERE id IN (1, 2) ORDER BY id DESC FOR UPDATE)",
            vec![],
            true,
            false,
            now_nanos(),
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
    assert_eq!(details(&e)["sqlstate"], "40P01");
    a.rollback().await.unwrap();
    assert_eq!(stored(&db, &run, 3).await, None);
}

#[tokio::test]
async fn pg_cap_is_response_too_large_without_token() {
    let Some(db) = db::pg().await else { return };
    let fx = Fx::new(
        &db,
        vec![
            ("small", serde_json::json!({"max_rows": 2})),
            ("tiny", serde_json::json!({"max_bytes": 64})),
        ],
    );
    let run = run_id("cap");
    sql(fx.exec("db", &run, 1, "SELECT 1", vec![], true).await);
    let e = nonretryable(
        fx.exec(
            "small",
            &run,
            2,
            "SELECT generate_series(1, 5) AS n",
            vec![],
            true,
        )
        .await,
    );
    assert_eq!(e.kind, kinds::RESPONSE_TOO_LARGE);
    assert_eq!(
        stored(&db, &run, 2).await,
        None,
        "rolled back with the token"
    );
    let ok = sql(fx
        .exec(
            "small",
            &run,
            3,
            "SELECT generate_series(1, 2) AS n",
            vec![],
            true,
        )
        .await);
    assert_eq!(ok.rows.len(), 2);
    let e = nonretryable(
        fx.exec(
            "tiny",
            &run,
            4,
            "SELECT repeat('x', 200) AS s",
            vec![],
            true,
        )
        .await,
    );
    assert_eq!(e.kind, kinds::RESPONSE_TOO_LARGE);
    assert_eq!(stored(&db, &run, 4).await, None);
}

#[tokio::test]
async fn pg_value_round_trip() {
    let Some(db) = db::pg().await else { return };
    let fx = Fx::new(&db, vec![]);
    ddl(
        &db,
        "CREATE TABLE vals (b bool, i int8, f float8, n numeric, t text, by bytea, j jsonb, \
         ts timestamptz, nul int4)",
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
            "INSERT INTO vals VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            values.clone(),
            true,
        )
        .await);
    assert_eq!((ins.rows_affected, ins.columns.len()), (1, 0));
    let out = sql(fx
        .exec("db", &run, 2, "SELECT * FROM vals", vec![], false)
        .await);
    assert_eq!(
        out.columns,
        vec!["b", "i", "f", "n", "t", "by", "j", "ts", "nul"]
    );
    assert_eq!(out.rows, vec![values]);

    let more = sql(fx
        .exec(
            "db",
            &run,
            3,
            "SELECT 1::int2 AS a, 2::int4 AS b, 1.5::float4 AS c, 'v'::varchar AS d, \
             'c'::char(2) AS e, '1970-01-02 00:00:00'::timestamp AS f, \
             '00000000-0000-0000-0000-000000000001'::uuid AS g, '{\"k\":1}'::json AS h, \
             $1::int8 AS i",
            vec![SqlValue::Null],
            false,
        )
        .await);
    assert_eq!(
        more.rows,
        vec![vec![
            SqlValue::Int8(1),
            SqlValue::Int8(2),
            SqlValue::Float8(1.5),
            SqlValue::Text("v".into()),
            SqlValue::Text("c ".into()),
            SqlValue::Timestamptz {
                seconds: 86_400,
                nanoseconds: 0
            },
            SqlValue::Text("00000000-0000-0000-0000-000000000001".into()),
            SqlValue::Json(r#"{"k":1}"#.into()),
            SqlValue::Null,
        ]]
    );
    let e = nonretryable(
        fx.exec("db", &run, 4, "SELECT current_date AS d", vec![], false)
            .await,
    );
    assert_eq!(e.kind, kinds::SQL_UNSUPPORTED_TYPE);
    assert_eq!(
        details(&e),
        serde_json::json!({"column": "d", "type": "DATE"})
    );
    // The same SQL with other parameter types does not reuse a stale prepared statement.
    for (op, p) in [
        (5u64, SqlValue::Null),
        (6, SqlValue::Int8(5)),
        (7, SqlValue::Null),
    ] {
        let r = sql(fx
            .exec(
                "db",
                &run,
                op,
                "SELECT $1::int8 AS x",
                vec![p.clone()],
                false,
            )
            .await);
        assert_eq!(r.rows, vec![vec![p]]);
    }
}

#[tokio::test]
async fn pg_gc_deletes_old_token_rows() {
    let Some(db) = db::pg().await else { return };
    let fx = Fx::new(&db, vec![]);
    let run = run_id("gc");
    sql(fx.exec("db", &run, 1, "SELECT 1", vec![], true).await);
    for op in 2..5u64 {
        sqlx::query(
            "INSERT INTO ostro_effects (effect_key, run_id, op, result, created_at) \
             VALUES ($1, $2, $3, '\\x01', now() - interval '10 days')",
        )
        .bind(effect_key(CLUSTER, &run, op).to_vec())
        .bind(&run)
        .bind(op as i64)
        .execute(db.pool())
        .await
        .unwrap();
    }
    assert_eq!(count(&db, "SELECT count(*) FROM ostro_effects").await, 4);
    let n = fx.conn.gc_once(Duration::from_secs(7 * 24 * 3600)).await;
    assert_eq!(n, 3);
    assert_eq!(count(&db, "SELECT count(*) FROM ostro_effects").await, 1);
    assert!(stored(&db, &run, 1).await.is_some(), "fresh row kept");
    // The spawned task is cancellable.
    let h = fx.conn.spawn_gc(Duration::from_secs(3600));
    tokio::time::timeout(Duration::from_secs(5), h.stop())
        .await
        .expect("GC task stops when cancelled");
}

#[tokio::test]
async fn pg_schema_isolates_connections() {
    // W3: two connections on two schemas see only their own tables.
    let Some(a) = db::pg().await else { return };
    let Some(b) = db::pg().await else { return };
    let conn = connector(
        vec![
            ("a", conn_config(&a, a.schema(), serde_json::json!({}))),
            ("b", conn_config(&a, b.schema(), serde_json::json!({}))),
        ],
        a.parts().password.as_deref(),
    );
    ddl(&a, "CREATE TABLE only_here (x int)").await;
    let run = run_id("schema");
    let ok = sql(exec(
        &conn,
        "a",
        &run,
        1,
        "SELECT count(*) AS n FROM only_here",
        vec![],
        false,
        false,
        0,
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
            0,
        )
        .await,
    );
    assert_eq!(details(&e)["sqlstate"], "42P01");
    // Each connection's token table lives in its own schema.
    sql(exec(
        &conn,
        "b",
        &run,
        3,
        "SELECT 1",
        vec![],
        true,
        false,
        now_nanos(),
    )
    .await);
    assert_eq!(count(&b, "SELECT count(*) FROM ostro_effects").await, 1);
    assert_eq!(
        count(
            &a,
            "SELECT count(*) FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_name = 'ostro_effects'"
        )
        .await,
        0
    );
}

#[cfg(feature = "failpoints")]
mod failpoints {
    use super::*;

    #[tokio::test]
    async fn pg_after_commit_error_reconciles_to_stored_bytes() {
        // B4: an error after COMMIT never completes Retryable; the connector's own reconcile
        // loop returns the stored result.
        let Some(db) = db::pg().await else { return };
        let fx = Fx::new(&db, vec![]);
        ddl(&db, "CREATE TABLE items (id int PRIMARY KEY)").await;
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
    async fn pg_before_user_stmt_error_rolls_back_token() {
        let Some(db) = db::pg().await else { return };
        let fx = Fx::new(&db, vec![]);
        let run = run_id("before");
        sql(fx.exec("db", &run, 1, "SELECT 1", vec![], true).await);
        let scenario = fail::FailScenario::setup();
        fail::cfg("bridge::sql::before_user_stmt", "return(error)").unwrap();
        let r = fx.exec("db", &run, 2, "SELECT 1", vec![], true).await;
        fail::remove("bridge::sql::before_user_stmt");
        scenario.teardown();
        assert!(matches!(r, Err(Failure::Retryable(_))), "{r:?}");
        assert_eq!(stored(&db, &run, 2).await, None);
    }

    #[tokio::test]
    async fn pg_pool_is_released_before_after_commit_failpoint() {
        // W4: 10 exactly-once effects hang at after_commit on a 2-connection pool. All 10
        // commit (only possible if each connection went back to the pool before the
        // failpoint), and an autocommit statement (which never passes the failpoint) still
        // completes.
        let Some(db) = db::pg().await else { return };
        let fx = Fx::new(&db, vec![("pool2", serde_json::json!({"pool_max": 2}))]);
        ddl(&db, "CREATE TABLE items (id int PRIMARY KEY)").await;
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
                    "INSERT INTO items VALUES ($1)",
                    vec![SqlValue::Int8(op as i64)],
                    true,
                    false,
                    now_nanos(),
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

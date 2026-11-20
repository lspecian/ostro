//! The exactly-once statement gate: an **allow-list**, not a deny-list (06-RESEARCH).
//!
//! An exactly-once statement runs in one transaction with its `ostro_effects` token row. A
//! statement that commits implicitly (MySQL DDL, `LOCK TABLES`, ...), controls the
//! transaction (`COMMIT`, `SAVEPOINT`, `XA`, ...) or writes outside it (`SELECT ... INTO
//! OUTFILE`) would split the token from the statement, so [`gate`] refuses it **before
//! anything runs**:
//!
//! 1. Leading whitespace and comments (`--`, MySQL `#`, `/* */`) are skipped. MySQL's `--`
//!    is a comment only when followed by whitespace (as the server parses it).
//! 2. A statement starting with `/*!` (MySQL executable comment) or `/*+` (optimizer hint) is
//!    refused: either can hide anything. On MySQL `/*!` is refused anywhere outside literals.
//! 3. The first keyword (case-insensitive) must be `SELECT`, `INSERT`, `UPDATE`, `DELETE`,
//!    `WITH`, `VALUES`, `TABLE`, `REPLACE` (MySQL only) or `MERGE` (PostgreSQL only). `CALL`
//!    needs the connection's `allow_call` (procedures can commit).
//! 4. `INTO OUTFILE` / `INTO DUMPFILE` outside string literals and quoted identifiers is
//!    refused. MySQL literals are scanned twice, with and without backslash escapes, so the
//!    server's `NO_BACKSLASH_ESCAPES` mode cannot hide the clause.
//!
//! Refusal is `NonRetryable{sql_not_transactional, "<keyword> is not allowed in exactly-once
//! mode"}`. An allow-list survives new server versions; [`IMPLICIT_COMMIT`] and the test
//! tables document what it must refuse.
//!
//! Not detectable from the statement (documented): non-transactional MySQL engines (MyISAM,
//! MEMORY) and PostgreSQL dblink/postgres_fdw writes.

use ostro_types::journal::Failure;

use super::config::Dialect;
use crate::kinds::SQL_NOT_TRANSACTIONAL;
use crate::outcome::non_retryable;

/// Every statement of the MySQL 8.4 Reference Manual §15.3.3 "Statements That Cause an
/// Implicit Commit" (plus the 8.0 replication synonyms), one sample each. The gate refuses all
/// of them; the tests iterate this table.
pub const IMPLICIT_COMMIT: &[&str] = &[
    // DDL
    "ALTER EVENT e ON SCHEDULE EVERY 1 HOUR",
    "ALTER FUNCTION f COMMENT 'x'",
    "ALTER PROCEDURE p COMMENT 'x'",
    "ALTER SERVER s OPTIONS (USER 'x')",
    "ALTER TABLE t ADD COLUMN c INT",
    "ALTER TABLESPACE ts RENAME TO ts2",
    "ALTER VIEW v AS SELECT 1",
    "CREATE DATABASE d",
    "CREATE EVENT e ON SCHEDULE EVERY 1 HOUR DO DELETE FROM t",
    "CREATE FUNCTION f() RETURNS INT DETERMINISTIC RETURN 1",
    "CREATE INDEX i ON t (c)",
    "CREATE PROCEDURE p() BEGIN SELECT 1; END",
    "CREATE ROLE r",
    "CREATE SERVER s FOREIGN DATA WRAPPER mysql OPTIONS (HOST 'h')",
    "CREATE SPATIAL REFERENCE SYSTEM 4120 NAME 'x' DEFINITION 'y'",
    "CREATE TABLE t (c INT)",
    "CREATE TABLE t2 SELECT * FROM t",
    "CREATE TABLESPACE ts ADD DATAFILE 'ts.ibd'",
    "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.c = 1",
    "CREATE VIEW v AS SELECT 1",
    "DROP DATABASE d",
    "DROP EVENT e",
    "DROP FUNCTION f",
    "DROP INDEX i ON t",
    "DROP PROCEDURE p",
    "DROP ROLE r",
    "DROP SERVER s",
    "DROP SPATIAL REFERENCE SYSTEM 4120",
    "DROP TABLE t",
    "DROP TABLESPACE ts",
    "DROP TRIGGER tr",
    "DROP VIEW v",
    "INSTALL PLUGIN p SONAME 'p.so'",
    "RENAME TABLE a TO b",
    "TRUNCATE TABLE t",
    "TRUNCATE t",
    "UNINSTALL PLUGIN p",
    // Temporary tables: no commit, but not rolled back either.
    "CREATE TEMPORARY TABLE tt (c INT)",
    "DROP TEMPORARY TABLE tt",
    // Statements that implicitly use or modify `mysql` tables.
    "ALTER USER u IDENTIFIED BY 'x'",
    "CREATE USER u IDENTIFIED BY 'x'",
    "DROP USER u",
    "GRANT SELECT ON *.* TO u",
    "RENAME USER a TO b",
    "REVOKE SELECT ON *.* FROM u",
    "SET PASSWORD = 'x'",
    // Transaction control and locking.
    "BEGIN",
    "BEGIN WORK",
    "LOCK TABLES t WRITE",
    "LOCK TABLE t READ",
    "SET autocommit = 1",
    "SET @@autocommit = 0",
    "START TRANSACTION",
    "START TRANSACTION WITH CONSISTENT SNAPSHOT",
    "UNLOCK TABLES",
    "COMMIT",
    "COMMIT WORK",
    "ROLLBACK",
    "ROLLBACK TO SAVEPOINT s",
    "SAVEPOINT s",
    "RELEASE SAVEPOINT s",
    "XA START 'x'",
    "XA END 'x'",
    "XA PREPARE 'x'",
    "XA COMMIT 'x'",
    "XA ROLLBACK 'x'",
    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    "SET SESSION TRANSACTION READ ONLY",
    // Data loading.
    "LOAD DATA INFILE 'f.csv' INTO TABLE t",
    "LOAD DATA LOCAL INFILE 'f.csv' INTO TABLE t",
    "LOAD XML INFILE 'f.xml' INTO TABLE t",
    // Administrative statements.
    "ANALYZE TABLE t",
    "CACHE INDEX t IN c",
    "CHECK TABLE t",
    "FLUSH TABLES",
    "FLUSH PRIVILEGES",
    "LOAD INDEX INTO CACHE t",
    "OPTIMIZE TABLE t",
    "REPAIR TABLE t",
    "RESET BINARY LOGS AND GTIDS",
    "RESET PERSIST",
    // Replication control (8.4 names, then the 8.0 synonyms).
    "START REPLICA",
    "STOP REPLICA",
    "RESET REPLICA",
    "CHANGE REPLICATION SOURCE TO SOURCE_HOST = 'h'",
    "CHANGE REPLICATION FILTER REPLICATE_DO_DB = (d)",
    "START SLAVE",
    "STOP SLAVE",
    "RESET SLAVE",
    "CHANGE MASTER TO MASTER_HOST = 'h'",
    "RESET MASTER",
    "START GROUP_REPLICATION",
    // Dynamic SQL can hide anything.
    "PREPARE s FROM 'DROP TABLE t'",
    "EXECUTE s",
    "DEALLOCATE PREPARE s",
    "DROP PREPARE s",
];

/// PostgreSQL statements that cannot run inside a transaction block, and the transaction
/// control statements that would end the effect transaction. The gate refuses all of them.
pub const PG_NO_TRANSACTION: &[&str] = &[
    "CREATE DATABASE d",
    "DROP DATABASE d",
    "CREATE TABLESPACE ts LOCATION '/x'",
    "DROP TABLESPACE ts",
    "ALTER SYSTEM SET work_mem = '1MB'",
    "VACUUM",
    "VACUUM FULL t",
    "CREATE INDEX CONCURRENTLY i ON t (c)",
    "CREATE UNIQUE INDEX CONCURRENTLY i ON t (c)",
    "DROP INDEX CONCURRENTLY i",
    "REINDEX TABLE CONCURRENTLY t",
    "REINDEX SYSTEM d",
    "REINDEX DATABASE d",
    "CLUSTER",
    "CREATE SUBSCRIPTION s CONNECTION 'host=h' PUBLICATION p",
    "ALTER SUBSCRIPTION s REFRESH PUBLICATION",
    "DROP SUBSCRIPTION s",
    "DISCARD ALL",
    // Transaction control.
    "COMMIT",
    "END",
    "END TRANSACTION",
    "ROLLBACK",
    "ABORT",
    "BEGIN",
    "START TRANSACTION",
    "SAVEPOINT s",
    "RELEASE SAVEPOINT s",
    "ROLLBACK TO SAVEPOINT s",
    "PREPARE TRANSACTION 'x'",
    "COMMIT PREPARED 'x'",
    "ROLLBACK PREPARED 'x'",
    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    // Other non-DML statements (refused by the allow-list).
    "ALTER TABLE t ADD COLUMN c int",
    "CREATE TABLE t (c int)",
    "TRUNCATE t",
    "COPY t FROM '/tmp/x'",
    "COPY t TO '/tmp/x'",
    "DO $$ BEGIN COMMIT; END $$",
    "PREPARE s AS SELECT 1",
    "EXECUTE s",
    "DEALLOCATE s",
    "LOCK TABLE t",
    "SET search_path = x",
    "RESET ALL",
    "LISTEN c",
    "NOTIFY c",
    "SHOW search_path",
    "EXPLAIN ANALYZE DELETE FROM t",
];

const ALLOWED: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "VALUES", "TABLE",
];

fn reject(keyword: &str) -> Failure {
    non_retryable(
        SQL_NOT_TRANSACTIONAL,
        format!("{keyword} is not allowed in exactly-once mode"),
        Some(serde_json::json!({ "keyword": keyword })),
    )
}

/// Refuses `stmt` unless it can run atomically with the token row in one transaction (see
/// the module docs).
pub fn gate(stmt: &str, dialect: Dialect, allow_call: bool) -> Result<(), Failure> {
    let s = stmt.as_bytes();
    let start = skip_leading(s, dialect)?;
    let rest = &s[start..];
    if rest.starts_with(b"/*!") {
        return Err(reject("/*!"));
    }
    if rest.starts_with(b"/*+") {
        return Err(reject("/*+"));
    }
    let kw_len = rest
        .iter()
        .take_while(|c| c.is_ascii_alphabetic() || **c == b'_')
        .count();
    if kw_len == 0 {
        let shown = match rest.first() {
            None => "an empty statement".to_owned(),
            Some(c) => format!("{:?}", char::from(*c)),
        };
        return Err(reject(&shown));
    }
    let kw = String::from_utf8_lossy(&rest[..kw_len]).to_ascii_uppercase();
    let allowed = ALLOWED.contains(&kw.as_str())
        || (kw == "REPLACE" && dialect == Dialect::Mysql)
        || (kw == "MERGE" && dialect == Dialect::Postgres)
        || (kw == "CALL" && allow_call);
    if !allowed {
        return Err(reject(&kw));
    }
    let modes: &[bool] = match dialect {
        Dialect::Mysql => &[true, false],
        Dialect::Postgres => &[false],
    };
    for &backslash in modes {
        let scan = scan(s, dialect, backslash);
        if scan.exec_comment {
            return Err(reject("/*!"));
        }
        if let Some(target) = scan.into_file {
            return Err(reject(&format!("INTO {target}")));
        }
    }
    Ok(())
}

/// Index of the first byte after leading whitespace and comments.
fn skip_leading(s: &[u8], dialect: Dialect) -> Result<usize, Failure> {
    let mut i = 0;
    loop {
        while i < s.len() && s[i].is_ascii_whitespace() {
            i += 1;
        }
        let rest = &s[i..];
        if rest.starts_with(b"/*!") || rest.starts_with(b"/*+") {
            return Ok(i);
        }
        if rest.starts_with(b"--") {
            if dialect == Dialect::Mysql && !rest.get(2).is_none_or(|c| is_mysql_dash_ws(*c)) {
                // `--x` is two minus signs to MySQL, not a comment.
                return Err(reject("--"));
            }
            i = line_end(s, i);
        } else if rest.starts_with(b"#") && dialect == Dialect::Mysql {
            i = line_end(s, i);
        } else if rest.starts_with(b"/*") {
            i = block_comment_end(s, i, dialect).ok_or_else(|| reject("/*"))?;
        } else {
            return Ok(i);
        }
    }
}

/// MySQL's `--` comment needs a whitespace or control character after it.
fn is_mysql_dash_ws(c: u8) -> bool {
    c.is_ascii_whitespace() || c.is_ascii_control()
}

fn line_end(s: &[u8], from: usize) -> usize {
    s[from..]
        .iter()
        .position(|c| *c == b'\n')
        .map_or(s.len(), |p| from + p + 1)
}

/// End (exclusive) of the block comment starting at `from`; PostgreSQL comments nest.
fn block_comment_end(s: &[u8], from: usize, dialect: Dialect) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = from;
    while i + 1 < s.len() {
        if s[i] == b'/' && s[i + 1] == b'*' {
            if depth == 0 || dialect == Dialect::Postgres {
                depth += 1;
            }
            i += 2;
        } else if s[i] == b'*' && s[i + 1] == b'/' {
            depth -= 1;
            i += 2;
            if depth == 0 {
                return Some(i);
            }
        } else {
            i += 1;
        }
    }
    None
}

#[derive(Default)]
struct Scan {
    exec_comment: bool,
    into_file: Option<&'static str>,
}

fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

/// Scans the words of `s` outside literals, quoted identifiers and comments.
fn scan(s: &[u8], dialect: Dialect, backslash: bool) -> Scan {
    let mut out = Scan::default();
    let mut prev_into = false;
    let mut i = 0;
    while i < s.len() {
        let c = s[i];
        let rest = &s[i..];
        match c {
            b'\'' => {
                // PostgreSQL honours backslashes only in E'..' strings.
                let esc = match dialect {
                    Dialect::Mysql => backslash,
                    Dialect::Postgres => {
                        i > 0
                            && matches!(s[i - 1], b'e' | b'E')
                            && (i < 2 || !is_ident_byte(s[i - 2]))
                    }
                };
                i = quoted_end(s, i, b'\'', esc);
                prev_into = false;
            }
            b'"' => {
                i = quoted_end(s, i, b'"', dialect == Dialect::Mysql && backslash);
                prev_into = false;
            }
            b'`' if dialect == Dialect::Mysql => {
                i = quoted_end(s, i, b'`', false);
                prev_into = false;
            }
            b'$' if dialect == Dialect::Postgres && (i == 0 || !is_ident_byte(s[i - 1])) => {
                match dollar_tag(rest) {
                    Some(tag) => {
                        let body = i + tag.len();
                        i = find(s, body, tag).map_or(s.len(), |p| p + tag.len());
                        prev_into = false;
                    }
                    None => i += 1,
                }
            }
            b'-' if rest.starts_with(b"--")
                && (dialect == Dialect::Postgres
                    || rest.get(2).is_none_or(|c| is_mysql_dash_ws(*c))) =>
            {
                i = line_end(s, i);
            }
            b'#' if dialect == Dialect::Mysql => i = line_end(s, i),
            b'/' if rest.starts_with(b"/*") => {
                if dialect == Dialect::Mysql && rest.starts_with(b"/*!") {
                    out.exec_comment = true;
                    return out;
                }
                i = block_comment_end(s, i, dialect).unwrap_or(s.len());
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let len = rest.iter().take_while(|b| is_ident_byte(**b)).count();
                let word = &rest[..len];
                if prev_into {
                    if word.eq_ignore_ascii_case(b"OUTFILE") {
                        out.into_file = Some("OUTFILE");
                        return out;
                    }
                    if word.eq_ignore_ascii_case(b"DUMPFILE") {
                        out.into_file = Some("DUMPFILE");
                        return out;
                    }
                }
                prev_into = word.eq_ignore_ascii_case(b"INTO");
                i += len;
            }
            c if c.is_ascii_whitespace() => i += 1,
            _ => {
                prev_into = false;
                i += 1;
            }
        }
    }
    out
}

/// End (exclusive) of the literal opened by `q` at `from`; a doubled quote is an escaped
/// quote, and so is a backslash-escaped one when `backslash`. Unterminated = end of input.
fn quoted_end(s: &[u8], from: usize, q: u8, backslash: bool) -> usize {
    let mut i = from + 1;
    while i < s.len() {
        let c = s[i];
        if backslash && c == b'\\' {
            i += 2;
        } else if c == q {
            if s.get(i + 1) == Some(&q) {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    s.len()
}

/// The `$tag$` opening a PostgreSQL dollar-quoted string at the start of `s` (`$1` is a
/// parameter, not a tag).
fn dollar_tag(s: &[u8]) -> Option<&[u8]> {
    let body = &s[1..];
    let len = body
        .iter()
        .take_while(|c| c.is_ascii_alphanumeric() || **c == b'_' || **c >= 0x80)
        .count();
    if body.first().is_some_and(u8::is_ascii_digit) {
        return None;
    }
    (body.get(len) == Some(&b'$')).then(|| &s[..len + 2])
}

fn find(s: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    s.get(from..)?
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| from + p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kinds::SQL_NOT_TRANSACTIONAL;
    use ostro_types::journal::ErrorInfo;

    fn refused(stmt: &str, d: Dialect) -> ErrorInfo {
        match gate(stmt, d, false) {
            Err(Failure::NonRetryable(e)) => {
                assert_eq!(e.kind, SQL_NOT_TRANSACTIONAL, "{stmt}");
                assert!(
                    e.message.ends_with(" is not allowed in exactly-once mode"),
                    "{}",
                    e.message
                );
                e
            }
            other => panic!("{d:?} must refuse {stmt:?}, got {other:?}"),
        }
    }

    fn allowed(stmt: &str, d: Dialect) {
        assert_eq!(gate(stmt, d, false), Ok(()), "{d:?} must allow {stmt:?}");
    }

    #[test]
    fn mysql_implicit_commit_table_is_refused() {
        assert!(IMPLICIT_COMMIT.len() > 90);
        for stmt in IMPLICIT_COMMIT {
            refused(stmt, Dialect::Mysql);
            refused(&stmt.to_lowercase(), Dialect::Mysql);
            refused(&format!("  /* lead */ -- c\n {stmt}"), Dialect::Mysql);
            refused(&format!("# c\n{stmt}"), Dialect::Mysql);
            // Refused on PostgreSQL too (no statement of the table is DML).
            refused(stmt, Dialect::Postgres);
        }
    }

    #[test]
    fn pg_no_transaction_and_control_table_is_refused() {
        for stmt in PG_NO_TRANSACTION {
            refused(stmt, Dialect::Postgres);
            refused(&format!("/* a /* nested */ b */ {stmt}"), Dialect::Postgres);
            refused(stmt, Dialect::Mysql);
        }
    }

    #[test]
    fn refusal_message_names_the_keyword() {
        let e = refused("alter table t add c int", Dialect::Mysql);
        assert_eq!(e.message, "ALTER is not allowed in exactly-once mode");
        let e = refused("", Dialect::Postgres);
        assert_eq!(
            e.message,
            "an empty statement is not allowed in exactly-once mode"
        );
        let e = refused("(SELECT 1)", Dialect::Postgres);
        assert_eq!(e.message, "'(' is not allowed in exactly-once mode");
    }

    #[test]
    fn bypass_forms_are_refused() {
        let my = Dialect::Mysql;
        let pg = Dialect::Postgres;
        for (stmt, d, kw) in [
            ("/*!50000 DROP TABLE x */", my, "/*!"),
            ("/*!50000 DROP TABLE x */", pg, "/*!"),
            ("  /*! DROP TABLE x */ SELECT 1", my, "/*!"),
            ("/*+ SET_VAR(autocommit=1) */ SELECT 1", my, "/*+"),
            ("/*+ SeqScan(t) */ SELECT 1", pg, "/*+"),
            ("-- c /*!\n/*!50000 DROP TABLE x */", my, "/*!"),
            ("SELECT 1 /*!50000 , 2 */", my, "/*!"),
            ("SELECT 1 /*!INTO OUTFILE '/x'*/", my, "/*!"),
            ("/* c */ DROP TABLE x", my, "DROP"),
            (" -- c\n DROP TABLE x", pg, "DROP"),
            ("# c\nDROP TABLE x", my, "DROP"),
            ("# c\nSELECT 1", pg, "'#'"),
            ("--x\nSELECT 1", my, "--"),
            ("/* unterminated SELECT 1", pg, "/*"),
            ("SELECT * FROM t INTO OUTFILE '/tmp/x'", my, "INTO OUTFILE"),
            (
                "select * into dumpfile '/tmp/x' from t",
                my,
                "INTO DUMPFILE",
            ),
            ("SELECT 1 INTO/*c*/outfile '/x'", my, "INTO OUTFILE"),
            ("SELECT 1 INTO\n  OUTFILE '/x'", my, "INTO OUTFILE"),
            ("TABLE t INTO OUTFILE '/x'", my, "INTO OUTFILE"),
            (
                "WITH a AS (SELECT 1) SELECT * FROM a INTO OUTFILE '/x'",
                my,
                "INTO OUTFILE",
            ),
            // NO_BACKSLASH_ESCAPES: the server sees 'a\' as a complete literal.
            ("SELECT 'a\\' INTO OUTFILE '/x' -- '", my, "INTO OUTFILE"),
            // Backslash escapes on: the server sees 'a\'' + ' INTO ...
            ("SELECT 'a\\'' INTO OUTFILE '/x' -- '", my, "INTO OUTFILE"),
            (
                "SELECT \"a\\\" INTO DUMPFILE '/x' -- \"",
                my,
                "INTO DUMPFILE",
            ),
            ("CALL p()", my, "CALL"),
            ("CALL p()", pg, "CALL"),
            (
                "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE",
                my,
                "MERGE",
            ),
            ("REPLACE INTO t VALUES (1)", pg, "REPLACE"),
        ] {
            let e = refused(stmt, d);
            assert_eq!(
                e.message,
                format!("{kw} is not allowed in exactly-once mode"),
                "{stmt}"
            );
        }
    }

    #[test]
    fn dml_is_allowed() {
        let both = [Dialect::Postgres, Dialect::Mysql];
        for d in both {
            for stmt in [
                "SELECT 1",
                "select * from t where a = 1",
                "INSERT INTO t (a) VALUES (1)",
                "insert into t select * from s",
                "UPDATE t SET a = a + 1 WHERE id = 3",
                "DELETE FROM t WHERE id = 3",
                "WITH x AS (SELECT 1 AS a) INSERT INTO t SELECT a FROM x",
                "WITH x AS (SELECT 1) SELECT * FROM x",
                "VALUES (1, 2)",
                "TABLE t",
                "-- leading comment\n/* block */ SELECT 1",
                "  \n\t SELECT 1",
                "SELECT 'into outfile', \"INTO DUMPFILE\" FROM t",
                "SELECT 1 -- INTO OUTFILE '/x'",
                "SELECT 1 /* INTO OUTFILE '/x' */",
                "SELECT a INTO x FROM t",
                "SELECT * FROM t FOR UPDATE",
            ] {
                allowed(stmt, d);
            }
        }
        for stmt in [
            "INSERT INTO t VALUES ($1, $2)",
            "SELECT $$ INTO OUTFILE $$",
            "SELECT $tag$ it's INTO DUMPFILE $tag$",
            "/* outer /* nested */ still comment */ SELECT 1",
            "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN UPDATE SET b = s.b",
            "SELECT outfile FROM t",
            "SELECT E'a\\' INTO OUTFILE' FROM t",
            "WITH moved AS (DELETE FROM a RETURNING *) INSERT INTO b SELECT * FROM moved",
        ] {
            allowed(stmt, Dialect::Postgres);
        }
        for stmt in [
            "INSERT INTO t VALUES (?, ?)",
            "REPLACE INTO t VALUES (1)",
            "# comment\nINSERT INTO t VALUES (1)",
            "SELECT `outfile` FROM t",
            "SELECT 'it''s INTO OUTFILE' FROM t",
            "SELECT 1 INTO @x",
            "--\tSELECT\nSELECT 1",
            "-- \nSELECT 1",
        ] {
            allowed(stmt, Dialect::Mysql);
        }
    }

    #[test]
    fn call_needs_allow_call() {
        assert!(gate("CALL p(1)", Dialect::Mysql, false).is_err());
        assert_eq!(gate("CALL p(1)", Dialect::Mysql, true), Ok(()));
        assert_eq!(gate("call p()", Dialect::Postgres, true), Ok(()));
        // allow_call opens CALL only.
        assert!(gate("DROP TABLE t", Dialect::Mysql, true).is_err());
    }

    #[test]
    fn dollar_tags() {
        assert_eq!(dollar_tag(b"$$ x"), Some(&b"$$"[..]));
        assert_eq!(dollar_tag(b"$ab$ x"), Some(&b"$ab$"[..]));
        assert_eq!(dollar_tag(b"$1"), None);
        assert_eq!(dollar_tag(b"$a b$"), None);
    }
}

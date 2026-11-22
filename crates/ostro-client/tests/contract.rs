//! Route-coverage contract (SDK-07, 10-09): every `(method, path)` a client calls must be
//! an operation of the server's OpenAPI document (`ostro openapi`, the built binary).
//!
//! Four clients are checked:
//! - Rust: [`ostro_client::typed::ROUTES`];
//! - Python `clients/python/ostro_client/routes.py`: `ROUTES = [("GET", "/v1/runs/{id}"), ...]`;
//! - TS `clients/ts/src/routes.js`: `export const ROUTES = [["GET", "/v1/runs/{id}"], ...]`;
//! - Go `clients/go/ostro/routes.go`: `var Routes = [][2]string{{"GET", "/v1/runs/{id}"}, ...}`.
//!
//! The three tables are read as text: every `"METHOD", "/path"` pair (any bracket) is a
//! route. Path parameters compare by position only (`{id}`, `{run_id}` and `{sha}` are all
//! `{}`). A table file that does not exist yet (10-14 writes them) is reported "pending" and
//! does not fail, unless `OSTRO_REQUIRE_ALL_CLIENTS=1` (the 10-20 gate).

mod common_p10;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde_json::Value;

/// `(METHOD, normalised path)`.
type Route = (String, String);

/// `(METHOD, path)` as a client table writes it.
type RawRoute = (String, String);

const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH"];

/// The polyglot clients' ROUTES tables, relative to the repository root.
const CLIENT_TABLES: &[(&str, &str)] = &[
    ("python", "clients/python/ostro_client/routes.py"),
    ("ts", "clients/ts/src/routes.js"),
    ("go", "clients/go/ostro/routes.go"),
];

/// `{anything}` → `{}`.
fn normalise(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut in_param = false;
    for c in path.chars() {
        match c {
            '{' => {
                in_param = true;
                out.push('{');
            }
            '}' => {
                in_param = false;
                out.push('}');
            }
            _ if in_param => {}
            _ => out.push(c),
        }
    }
    out
}

fn route(method: &str, path: &str) -> Route {
    (method.to_ascii_uppercase(), normalise(path))
}

/// Every operation of the OpenAPI document.
fn openapi_routes(doc: &Value) -> BTreeSet<Route> {
    let paths = doc["paths"]
        .as_object()
        .expect("OpenAPI document has paths");
    let mut out = BTreeSet::new();
    for (path, item) in paths {
        for method in item.as_object().into_iter().flat_map(|o| o.keys()) {
            if METHODS.contains(&method.to_ascii_uppercase().as_str()) {
                out.insert(route(method, path));
            }
        }
    }
    out
}

/// The routes of `client` absent from `api`, as `METHOD path` strings.
fn missing(client: &[(String, String)], api: &BTreeSet<Route>) -> Vec<String> {
    client
        .iter()
        .filter(|(m, p)| !api.contains(&route(m, p)))
        .map(|(m, p)| format!("{m} {p}"))
        .collect()
}

/// A quoted string starting at `s[0]` (`"` or `'`): (content, rest).
fn quoted(s: &str) -> Option<(&str, &str)> {
    let q = s.chars().next().filter(|c| *c == '"' || *c == '\'')?;
    let end = s[1..].find(q)?;
    Some((&s[1..1 + end], &s[end + 2..]))
}

/// Every `"METHOD", "/path"` pair in `text` (any bracket around it, either quote char).
fn quoted_pairs(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(i) = rest.find(['"', '\'']) {
        let here = &rest[i..];
        let Some((first, after)) = quoted(here) else {
            break;
        };
        if METHODS.contains(&first)
            && let Some(tail) = after.trim_start().strip_prefix(',')
            && let Some((path, after_path)) = quoted(tail.trim_start())
            && path.starts_with('/')
        {
            out.push((first.to_owned(), path.to_owned()));
            rest = after_path;
            continue;
        }
        rest = after;
    }
    out
}

fn rust_routes() -> Vec<(String, String)> {
    ostro_client::typed::ROUTES
        .iter()
        .map(|(m, p)| ((*m).to_owned(), (*p).to_owned()))
        .collect()
}

#[test]
fn normalise_and_parse() {
    assert_eq!(
        normalise("/v1/runs/{run_id}/signals/{name}"),
        "/v1/runs/{}/signals/{}"
    );
    assert_eq!(
        route("get", "/v1/runs/{id}"),
        route("GET", "/v1/runs/{run_id}")
    );
    let py = r#"ROUTES = [("GET", "/v1/runs/{id}"), ( "POST" , '/v1/tokens/complete' ),]"#;
    let ts =
        r#"export const ROUTES = [["GET", "/v1/runs/{id}"], ["PUT", "/v1/modules/{sha256}"]];"#;
    let go = "var Routes = [][2]string{\n\t{\"GET\", \"/v1/runs/{id}\"},\n\t{\"DELETE\", \"/v1/secrets/{name}\"},\n}";
    assert_eq!(
        quoted_pairs(py),
        vec![
            ("GET".into(), "/v1/runs/{id}".into()),
            ("POST".into(), "/v1/tokens/complete".into())
        ]
    );
    assert_eq!(quoted_pairs(ts).len(), 2);
    assert_eq!(
        quoted_pairs(go)[1],
        ("DELETE".into(), "/v1/secrets/{name}".into())
    );
    // A method string not followed by a path is not a route.
    assert!(quoted_pairs(r#"m = "GET"; p = "x""#).is_empty());
}

#[test]
fn rust_client_routes_are_in_openapi() {
    let api = openapi_routes(&common_p10::openapi_doc());
    let rust = rust_routes();
    assert!(!rust.is_empty());
    let miss = missing(&rust, &api);
    assert!(
        miss.is_empty(),
        "Rust client routes missing from `ostro openapi`: {miss:?}"
    );
    // No duplicates in the table.
    let set: BTreeSet<Route> = rust.iter().map(|(m, p)| route(m, p)).collect();
    assert_eq!(set.len(), rust.len(), "duplicate entries in typed::ROUTES");
}

#[test]
fn checker_reports_a_bogus_route_by_name() {
    let api = openapi_routes(&common_p10::openapi_doc());
    let mut rust = rust_routes();
    rust.push(("GET".into(), "/v1/nope".into()));
    rust.push(("DELETE".into(), "/v1/runs/{id}".into()));
    assert_eq!(
        missing(&rust, &api),
        vec!["GET /v1/nope", "DELETE /v1/runs/{id}"]
    );
}

#[test]
fn polyglot_client_routes_are_in_openapi() {
    let doc = common_p10::openapi_doc();
    let api = openapi_routes(&doc);
    let require_all = std::env::var("OSTRO_REQUIRE_ALL_CLIENTS").is_ok_and(|v| v == "1");
    let root = common_p10::repo_root();

    let mut tables: Vec<(&str, Option<Vec<RawRoute>>)> = vec![("rust", Some(rust_routes()))];
    let mut failures = Vec::new();
    for (name, rel) in CLIENT_TABLES {
        let path = root.join(rel);
        if !Path::new(&path).is_file() {
            println!("client {name}: pending (10-14 writes {rel}; 10-04 is only the scaffold)");
            if require_all {
                failures.push(format!("{name}: {rel} does not exist"));
            }
            tables.push((name, None));
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let routes = quoted_pairs(&text);
        if routes.is_empty() {
            failures.push(format!(
                "{name}: {rel} has no (\"METHOD\", \"/path\") entries"
            ));
        }
        let miss = missing(&routes, &api);
        if !miss.is_empty() {
            failures.push(format!(
                "{name} ({rel}) routes missing from `ostro openapi`: {miss:?}"
            ));
        }
        println!(
            "client {name}: {} routes, {} missing",
            routes.len(),
            miss.len()
        );
        tables.push((name, Some(routes)));
    }

    // Coverage table: every /v1 operation and which clients call it.
    let mut cover: BTreeMap<Route, Vec<&str>> = api
        .iter()
        .filter(|(_, p)| p.starts_with("/v1/"))
        .map(|r| (r.clone(), Vec::new()))
        .collect();
    for (name, routes) in &tables {
        for (m, p) in routes.iter().flatten() {
            if let Some(v) = cover.get_mut(&route(m, p))
                && !v.contains(name)
            {
                v.push(name);
            }
        }
    }
    let header: Vec<String> = tables
        .iter()
        .map(|(n, r)| match r {
            Some(_) => (*n).to_owned(),
            None => format!("{n} (pending)"),
        })
        .collect();
    println!(
        "route coverage ({} /v1 operations; clients: {})",
        cover.len(),
        header.join(", ")
    );
    for ((m, p), clients) in &cover {
        let cells: Vec<&str> = tables
            .iter()
            .map(|(n, _)| if clients.contains(n) { "x" } else { "." })
            .collect();
        println!("  {:<6} {:<45} {}", m, p, cells.join(" "));
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

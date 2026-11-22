//! The run pages (11-15, UI-01) against the 11-09 fakes: run list with filters, tags and
//! pagination, the DLQ tab, run detail with published status and lineage, the timeline
//! with replay markers and rerun controls, and the logs tab. HTML snapshots are insta
//! (`tests/snapshots/`); the fakes are deterministic, so nothing is redacted.

mod common;

use common::{EVIL_REASON, EVIL_TAG, Slots, get_ok, seed, ui, ui_with};
use ostro_api::ControlPlane;
use ostro_api::domain::{ListOrder, RunStatus, StartRunRequest};
use ostro_api::phase11::{RerunCut, RerunCutsView};

/// The `<main>` element: the page minus the shared layout head.
fn main_of(html: &str) -> &str {
    let start = html.find("<main").expect("main");
    let end = html.rfind("</main>").expect("/main") + "</main>".len();
    &html[start..end]
}

fn assert_hostile_escaped(html: &str) {
    assert!(!html.contains(EVIL_TAG), "raw hostile tag in {html}");
    assert!(!html.contains(EVIL_REASON), "raw hostile reason in {html}");
    assert!(!html.contains("<script>"), "{html}");
    assert!(!html.contains("<img"), "{html}");
}

#[tokio::test]
async fn run_list_renders_rows_tags_and_escapes() {
    let ui = ui();
    let s = seed(&ui).await;
    let r = get_ok(&ui, "/ui/").await;
    let html = main_of(&r.body);
    insta::assert_snapshot!("run_list", html);
    assert_hostile_escaped(&r.body);
    assert!(html.contains("&#60;script&#62;alert(1)&#60;/script&#62;"));
    // Tag chips link to the filtered list (hostile tags percent-encoded).
    assert!(html.contains(r#"href="/ui/runs?tags=env%3Dprod""#));
    assert!(html.contains("/ui/runs?tags=%3Cscript%3Ealert%281%29%3C%2Fscript%3E"));
    assert!(html.contains(&format!(r#"href="/ui/runs/{}""#, s.completed)));
    // /ui/runs is the same page.
    let r2 = get_ok(&ui, "/ui/runs").await;
    assert_eq!(main_of(&r2.body), html);
}

#[tokio::test]
async fn filters_reach_list_runs() {
    let ui = ui();
    let s = seed(&ui).await;
    let r = get_ok(
        &ui,
        "/ui/runs?workflow=greet&queue=default&status=running&tags=env%3Dprod&tags=team%3Da&tags=&order=newest",
    )
    .await;
    let req = ui.cp.last_list_runs().expect("list_runs called");
    assert_eq!(req.workflow.as_deref(), Some("greet"));
    assert_eq!(req.queue.as_deref(), Some("default"));
    assert_eq!(req.status, Some(RunStatus::Running));
    assert_eq!(req.tags, ["env=prod", "team=a"]);
    assert_eq!(req.tag.as_deref(), Some("env=prod"));
    assert_eq!(req.order, ListOrder::Newest);
    // No run is running with both tags (`running` carries env=prod only).
    assert!(!r.body.contains(&s.running));
    insta::assert_snapshot!("run_list_filtered_empty", main_of(&r.body));
    assert!(r.body.contains("No runs match these filters."));
    // The form echoes the filters.
    assert!(r.body.contains(r#"<option value="running" selected>"#));
    assert!(r.body.contains(r#"<option value="newest" selected>"#));
    assert!(r.body.contains(r#"<input name="tags" value="team=a">"#));

    let r = get_ok(&ui, "/ui/runs?tags=env%3Dprod&order=newest").await;
    let req = ui.cp.last_list_runs().expect("list_runs");
    assert_eq!(req.tags, ["env=prod"]);
    assert_eq!(req.order, ListOrder::Newest);
    let (a, b) = (
        r.body.find(&s.rerun).expect("rerun row"),
        r.body.find(&s.completed).expect("completed row"),
    );
    assert!(a < b, "newest first");
}

#[tokio::test]
async fn bad_filters_are_400() {
    let ui = ui();
    let r = common::send(&ui.router, common::get("/ui/runs?status=bogus")).await;
    assert_eq!(r.status, http::StatusCode::BAD_REQUEST);
    let nine: String = (0..9).map(|i| format!("tags=t{i}&")).collect();
    let r = common::send(&ui.router, common::get(&format!("/ui/runs?{nine}"))).await;
    assert_eq!(r.status, http::StatusCode::BAD_REQUEST, "{}", r.body);
}

#[tokio::test]
async fn pagination_follows_the_page_token() {
    let ui = ui();
    seed(&ui).await;
    for _ in 0..52 {
        ui.cp
            .start_run(StartRunRequest {
                workflow: "greet".to_owned(),
                input_json: b"{}".to_vec(),
                tags: vec!["bulk=yes".to_owned()],
                ..StartRunRequest::default()
            })
            .await
            .expect("start");
    }
    let r = get_ok(&ui, "/ui/runs?tags=bulk%3Dyes").await;
    let next = r
        .body
        .split(r#"<a href=""#)
        .find_map(|s| s.split_once(r#"" rel="next""#).map(|(h, _)| h.to_owned()))
        .expect("next link");
    assert_eq!(next, "/ui/runs?tags=bulk%3Dyes&#38;page_token=run-000055");
    assert!(!r.body.contains("First page"));
    let r = get_ok(&ui, &next.replace("&#38;", "&")).await;
    let req = ui.cp.last_list_runs().expect("list_runs");
    assert_eq!(req.page_token.as_deref(), Some("run-000055"));
    assert!(r.body.contains("run-000056") && r.body.contains("run-000057"));
    assert!(!r.body.contains(r#"rel="next""#));
    assert!(
        r.body
            .contains(r#"<a href="/ui/runs?tags=bulk%3Dyes">First page</a>"#)
    );
}

#[tokio::test]
async fn empty_state() {
    let ui = ui();
    let r = get_ok(&ui, "/ui/").await;
    assert!(r.body.contains("No runs match these filters."));
}

#[tokio::test]
async fn dlq_tab_lists_entries_escaped() {
    let ui = ui();
    let s = seed(&ui).await;
    let r = get_ok(&ui, "/ui/dlq").await;
    insta::assert_snapshot!("dlq", main_of(&r.body));
    assert_hostile_escaped(&r.body);
    assert!(
        r.body.contains(&format!(r#"href="/ui/runs/{}""#, s.hook)),
        "hook link"
    );
    // Without the ops slot: "not available".
    let ui = ui_with(Slots {
        ops: false,
        logs: true,
    });
    let r = get_ok(&ui, "/ui/dlq").await;
    assert!(
        r.body
            .contains("The dead-letter queue is not available on this server.")
    );
}

#[tokio::test]
async fn run_detail_completed() {
    let ui = ui();
    let s = seed(&ui).await;
    ui.ops.set_cuts(RerunCutsView {
        run_id: s.completed.clone(),
        cuts: vec![
            RerunCut {
                seq: 2,
                step: Some("greet".to_owned()),
                occurrence: 1,
                safe: true,
                reason: None,
                nearest_safe_seq: None,
            },
            RerunCut {
                seq: 3,
                step: Some("<b>ship</b>".to_owned()),
                occurrence: 1,
                safe: false,
                reason: Some("an effect <b>is</b> in flight".to_owned()),
                nearest_safe_seq: Some(2),
            },
        ],
    });
    let r = get_ok(&ui, &format!("/ui/runs/{}", s.completed)).await;
    let html = main_of(&r.body);
    insta::assert_snapshot!("run_detail_completed", html);
    // Terminal: the status card does not poll.
    assert!(!html.contains("hx-trigger"));
    // Published status and result pretty-printed and escaped.
    assert!(html.contains("&#60;i&#62;done&#60;/i&#62;"));
    assert!(html.contains("hello &#60;b&#62;world&#60;/b&#62;"));
    // Exact replay marker.
    assert!(html.contains("attempt 1.2 replayed seq 0..2"));
    // Rerun controls: enabled link and the escaped analyzer reason.
    assert!(html.contains(&format!(
        r#"href="/ui/runs/{}/rerun?from_seq=2">Rerun from here</a>"#,
        s.completed
    )));
    assert!(html.contains(
        "Rerun unavailable: an effect &#60;b&#62;is&#60;/b&#62; in flight (nearest safe cut: seq 2)"
    ));
}

#[tokio::test]
async fn running_status_fragment_polls_and_terminal_does_not() {
    let ui = ui();
    let s = seed(&ui).await;
    let r = get_ok(&ui, &format!("/ui/runs/{}/status", s.running)).await;
    insta::assert_snapshot!("status_running", r.body);
    assert!(r.body.contains(&format!(
        r#"hx-get="/ui/runs/{}/status" hx-trigger="every 2s" hx-swap="outerHTML""#,
        s.running
    )));
    assert!(!r.body.contains("<html"), "a fragment, not a page");
    let r = get_ok(&ui, &format!("/ui/runs/{}/status", s.completed)).await;
    assert!(!r.body.contains("hx-trigger"));
    assert!(!r.body.contains("hx-get"));
    // The detail page of a running run embeds the polling card and no rerun control.
    let r = get_ok(&ui, &format!("/ui/runs/{}", s.running)).await;
    assert!(r.body.contains(r#"hx-trigger="every 2s""#));
    assert!(
        r.body
            .contains("Rerun from a step is available once the run is terminal.")
    );
}

#[tokio::test]
async fn lineage_rerun_and_hook() {
    let ui = ui();
    let s = seed(&ui).await;
    let r = get_ok(&ui, &format!("/ui/runs/{}", s.rerun)).await;
    let html = main_of(&r.body);
    assert!(html.contains("rerun from seq 3"), "{html}");
    // The copied prefix (seq 1 < cut 3).
    assert!(html.contains("copied from the source run"));
    let t = html.find(r#"<section id="timeline""#).expect("timeline");
    let e = html[t..].find("</section>").expect("end") + t + "</section>".len();
    let section = &html[t..e];
    insta::assert_snapshot!("timeline_rerun", section);
    let r = get_ok(&ui, &format!("/ui/runs/{}", s.hook)).await;
    assert!(
        r.body.contains(&format!(
            r#"on_failure hook for <a class="mono" href="/ui/runs/{0}">{0}</a>"#,
            s.failed
        )),
        "{}",
        r.body
    );
    // The failed run: failure escaped.
    let r = get_ok(&ui, &format!("/ui/runs/{}", s.failed)).await;
    assert_hostile_escaped(&r.body);
    assert!(r.body.contains("<h2>Failure</h2>"));
    assert!(r.body.contains("An original run."));
}

#[tokio::test]
async fn timeline_fragment_matches_the_page() {
    let ui = ui();
    let s = seed(&ui).await;
    let r = get_ok(&ui, &format!("/ui/runs/{}/timeline", s.completed)).await;
    insta::assert_snapshot!("timeline_completed", r.body);
    assert!(r.body.starts_with(r#"<section id="timeline""#));
    assert!(r.body.contains("Replay markers are exact"));
    assert!(r.body.contains(r#"<rect class="span" x=""#));
    assert!(!r.body.contains("style="));
    // Without the ops slot there are no controls and a note says why.
    let ui = ui_with(Slots {
        ops: false,
        logs: true,
    });
    let s = seed(&ui).await;
    let r = get_ok(&ui, &format!("/ui/runs/{}/timeline", s.completed)).await;
    assert!(r.body.contains("Rerun is not available on this server."));
    assert!(!r.body.contains("Rerun from here"));
}

#[tokio::test]
async fn logs_tab_and_fragment() {
    let ui = ui();
    let s = seed(&ui).await;
    let r = get_ok(&ui, &format!("/ui/runs/{}/logs", s.running)).await;
    insta::assert_snapshot!("logs_running", r.body);
    assert!(r.body.contains("charging &#60;card&#62;"));
    assert!(!r.body.contains("<card>"));
    assert!(!r.body.contains("<b>x</b>"));
    let r = get_ok(&ui, &format!("/ui/runs/{}?tab=logs", s.running)).await;
    assert!(r.body.contains(r#"<section id="logs""#));
    assert!(!r.body.contains(r#"<section id="timeline""#));
    assert!(r.body.contains(r#"?tab=logs" aria-current="page""#));
    let r = get_ok(&ui, &format!("/ui/runs/{}/logs", s.completed)).await;
    assert!(r.body.contains("No log lines for this run."));
    // Slot absent: "not available".
    let ui = ui_with(Slots {
        ops: true,
        logs: false,
    });
    let s = seed(&ui).await;
    let r = get_ok(&ui, &format!("/ui/runs/{}/logs", s.running)).await;
    assert!(r.body.contains("Logs not available"));
}

#[tokio::test]
async fn unknown_run_is_a_404_page() {
    let ui = ui();
    for path in [
        "/ui/runs/run-424242",
        "/ui/runs/run-424242/status",
        "/ui/runs/run-424242/timeline",
        "/ui/runs/run-424242/logs",
    ] {
        let r = common::send(&ui.router, common::get(path)).await;
        assert_eq!(r.status, http::StatusCode::NOT_FOUND, "{path}");
        assert!(
            r.body.contains("run run-424242 not found"),
            "{path}: {}",
            r.body
        );
        assert!(r.body.contains("<title>404 Not Found - Ostro</title>"));
    }
}

#[tokio::test]
async fn api_errors_render_with_their_status() {
    let ui = ui();
    seed(&ui).await;
    ui.ops.fail(
        "list_dlq",
        Some(ostro_api::ApiError::unavailable(
            "partition <leader> moving",
        )),
    );
    let r = common::send(&ui.router, common::get("/ui/dlq")).await;
    assert_eq!(r.status, http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(r.body.contains("partition &#60;leader&#62; moving"));
}

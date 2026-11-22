//! Owned by plan 11-15: the run list, run detail and published-status pages.
//!
//! 11-03 only compiles `templates/ui/layout.html` through one askama template so the
//! engine (and its auto-escaping) is exercised from the start; 11-15 replaces the probe.

use askama::Template;

/// The page shell. `title` is auto-escaped (`.html` template).
#[derive(Template)]
#[template(path = "ui/layout.html")]
#[cfg_attr(not(test), allow(dead_code))] // probe only; 11-15 renders real pages
pub(crate) struct LayoutProbe<'a> {
    pub(crate) title: &'a str,
}

/// Renders the shell with `title`.
#[cfg_attr(not(test), allow(dead_code))] // probe only; 11-15 renders real pages
pub(crate) fn render_layout_probe(title: &str) -> askama::Result<String> {
    LayoutProbe { title }.render()
}

//! `ostro limits` (11-14, OPS-07): the published engine and API limits.

use clap::Args;

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s, table};

/// `ostro limits`.
#[derive(Debug, Clone, Args)]
pub struct LimitsArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro limits`.
pub async fn limits(c: &Client, out: Out, _a: &LimitsArgs) -> Result<Outcome, CallError> {
    let r = c.limits().await?;
    if out.json {
        out.json(&r.body);
        return Ok(Outcome::Ok);
    }
    let rows: Vec<Vec<String>> = r.body["limits"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .map(|l| {
            vec![
                s(&l["name"]),
                s(&l["value"]),
                s(&l["unit"]),
                if l["configurable"].as_bool() == Some(true) {
                    "yes".to_owned()
                } else {
                    "no".to_owned()
                },
                s(&l["enforced"]),
                s(&l["error"]),
            ]
        })
        .collect();
    println!(
        "{}",
        table(
            &["NAME", "VALUE", "UNIT", "CONFIGURABLE", "ENFORCED", "ERROR"],
            &rows
        )
    );
    Ok(Outcome::Ok)
}

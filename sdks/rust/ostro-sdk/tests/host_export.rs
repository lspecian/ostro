//! `export!` on a host target (followups1): it expands to a type check only, so a workflow
//! crate that calls it unguarded links for a host `cargo test`. This file compiling and
//! linking as a native test binary is the test; the wasm32 expansion is covered by every
//! guest under `guests/`.

use ostro_sdk::prelude::*;
use ostro_sdk::registry::Workflow;

#[derive(Debug, Deserialize, Serialize)]
pub struct Input {
    pub n: u64,
}

#[workflow(name = "host-double", version = 2)]
pub fn double(_ctx: &Ctx, input: Input) -> Result<Input, WorkflowError> {
    Ok(Input { n: input.n * 2 })
}

struct Manual;
impl Workflow for Manual {
    const NAME: &'static str = "manual";
    type Input = u32;
    type Output = u32;
    fn run(_ctx: &Ctx, n: u32) -> Result<u32, WorkflowError> {
        Ok(n)
    }
}

// Unguarded, at module level, exactly as in a user project.
ostro_sdk::export!(DoubleWorkflow, Manual);

#[test]
fn export_is_a_host_no_op_and_the_workflows_stay_usable() {
    assert_eq!(<DoubleWorkflow as Workflow>::NAME, "host-double");
    assert_eq!(<DoubleWorkflow as Workflow>::VERSION, 2);
    assert_eq!(<Manual as Workflow>::NAME, "manual");
    let d = ostro_sdk::registry::__describe::<DoubleWorkflow>();
    assert_eq!((d.name.as_str(), d.version), ("host-double", 2));
}

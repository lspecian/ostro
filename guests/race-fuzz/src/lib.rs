//! Race-ordering fuzz guest (plan 05-10, WF-03): workflow `race-fuzz` v1.
//!
//! Input `{"seed": u64, "sleeps_ms": [u64], "signals": [String], "nest": bool}`.
//!
//! Ops are created up front, in order: one durable sleep per `sleeps_ms` entry (labels
//! `t0`, `t1`, ...), then one signal wait per `signals` entry (labels `s0:<name>`, ...)
//! with a timeout drawn from the seed (50..500 ms), so every op eventually completes.
//! A splitmix64 generator seeded with `seed` (not the run's journaled RNG: the shape
//! depends on the input only) then consumes the pending ops in rounds:
//!
//! - `wait`: one op, `Op::wait`;
//! - `select`: `select!` over two ops; the loser is cancelled with `ctx.cancel` and then
//!   waited, so it reports `cancelled`, or its own result if it completed earlier in
//!   journal order (first completion wins);
//! - `join` (`nest` only): `join!` over two ops;
//! - `select_join` (`nest` only): `select!` over two ops, then `join!` of the loser
//!   (not cancelled) with a third op.
//!
//! Output `{"rounds": [String], "order": [label], "ops": [{"label", "op", "outcome"}]}`
//! where `order`/`ops` list ops in the order the guest observed their results and
//! `outcome` is `ok` (timer), `ok:<payload JSON>` (signal), `cancelled`, `timeout` or
//! `err:<kind>`. Every wait observes run cancellation (D11); a cancel ends the run
//! Cancelled.

use std::time::Duration;

use ostro_sdk::prelude::*;
use ostro_sdk::{Completion, StepError};

type Value = serde_json::Value;

/// `race-fuzz` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub seed: u64,
    #[serde(default)]
    pub sleeps_ms: Vec<u64>,
    #[serde(default)]
    pub signals: Vec<String>,
    #[serde(default)]
    pub nest: bool,
}

/// One observed op result.
#[derive(Debug, Serialize)]
pub struct Observed {
    pub label: String,
    pub op: u64,
    pub outcome: String,
}

/// `race-fuzz` output.
#[derive(Debug, Serialize)]
pub struct Output {
    pub rounds: Vec<String>,
    pub order: Vec<String>,
    pub ops: Vec<Observed>,
}

/// splitmix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

enum Kind {
    Sleep(Op<()>),
    Signal(Op<Value>),
}

struct Pending {
    label: String,
    kind: Kind,
}

fn describe<T>(r: Result<T, StepError>, show: impl FnOnce(T) -> String) -> String {
    match r {
        Ok(v) => show(v),
        Err(e) if e.is_cancelled() => "cancelled".into(),
        Err(e) if e.is_timeout() => "timeout".into(),
        Err(e) => format!("err:{}", e.kind),
    }
}

fn unit(_: ()) -> String {
    "ok".into()
}

fn payload(v: Value) -> String {
    format!("ok:{v}")
}

impl Pending {
    fn id(&self) -> u64 {
        match &self.kind {
            Kind::Sleep(o) => o.id(),
            Kind::Signal(o) => o.id(),
        }
    }

    fn decode(&self, c: Completion) -> String {
        match &self.kind {
            Kind::Sleep(o) => describe(o.decode(c), unit),
            Kind::Signal(o) => describe(o.decode(c), payload),
        }
    }

    fn wait(self) -> String {
        match self.kind {
            Kind::Sleep(o) => describe(o.wait(), unit),
            Kind::Signal(o) => describe(o.wait(), payload),
        }
    }

    fn cancel(&self, ctx: &Ctx) {
        match &self.kind {
            Kind::Sleep(o) => ctx.cancel(o),
            Kind::Signal(o) => ctx.cancel(o),
        }
    }
}

/// `select!` over two pending ops (the macro needs concrete op types).
fn select2(a: &Pending, b: &Pending) -> (usize, Completion) {
    match (&a.kind, &b.kind) {
        (Kind::Sleep(x), Kind::Sleep(y)) => ostro_sdk::select!(*x, *y),
        (Kind::Sleep(x), Kind::Signal(y)) => ostro_sdk::select!(*x, *y),
        (Kind::Signal(x), Kind::Sleep(y)) => ostro_sdk::select!(*x, *y),
        (Kind::Signal(x), Kind::Signal(y)) => ostro_sdk::select!(*x, *y),
    }
}

/// `join!` over two pending ops; outcomes in argument order.
fn join2(a: Pending, b: Pending) -> (String, String) {
    match (a.kind, b.kind) {
        (Kind::Sleep(x), Kind::Sleep(y)) => {
            let (rx, ry) = ostro_sdk::join!(x, y);
            (describe(rx, unit), describe(ry, unit))
        }
        (Kind::Sleep(x), Kind::Signal(y)) => {
            let (rx, ry) = ostro_sdk::join!(x, y);
            (describe(rx, unit), describe(ry, payload))
        }
        (Kind::Signal(x), Kind::Sleep(y)) => {
            let (rx, ry) = ostro_sdk::join!(x, y);
            (describe(rx, payload), describe(ry, unit))
        }
        (Kind::Signal(x), Kind::Signal(y)) => {
            let (rx, ry) = ostro_sdk::join!(x, y);
            (describe(rx, payload), describe(ry, payload))
        }
    }
}

struct Recorder {
    out: Output,
}

impl Recorder {
    fn record(&mut self, label: String, op: u64, outcome: String) {
        self.out.order.push(label.clone());
        self.out.ops.push(Observed { label, op, outcome });
    }
}

#[workflow(name = "race-fuzz", version = 1)]
pub fn race_fuzz(ctx: &Ctx, input: Input) -> Result<Output, WorkflowError> {
    let mut rng = Rng(input.seed);
    let mut pending: Vec<Pending> = Vec::new();
    for (i, ms) in input.sleeps_ms.iter().enumerate() {
        pending.push(Pending {
            label: format!("t{i}"),
            kind: Kind::Sleep(ctx.sleep(Duration::from_millis(*ms))),
        });
    }
    for (i, name) in input.signals.iter().enumerate() {
        let timeout = Duration::from_millis(50 + rng.next() % 450);
        pending.push(Pending {
            label: format!("s{i}:{name}"),
            kind: Kind::Signal(ctx.wait_signal::<Value>(name, Some(timeout))),
        });
    }
    let mut rec = Recorder {
        out: Output {
            rounds: Vec::new(),
            order: Vec::new(),
            ops: Vec::new(),
        },
    };
    while !pending.is_empty() {
        let shapes = if input.nest { 4 } else { 2 };
        let mut shape = rng.below(shapes);
        if shape == 3 && pending.len() < 3 {
            shape = 1;
        }
        if shape != 0 && pending.len() < 2 {
            shape = 0;
        }
        match shape {
            0 => {
                let a = pending.remove(rng.below(pending.len()));
                rec.out.rounds.push(format!("wait({})", a.label));
                let (label, id) = (a.label.clone(), a.id());
                let o = a.wait();
                if o == "cancelled" {
                    return Err(StepError::cancelled().into());
                }
                rec.record(label, id, o);
            }
            1 | 3 => {
                let a = pending.remove(rng.below(pending.len()));
                let b = pending.remove(rng.below(pending.len()));
                let (idx, c) = select2(&a, &b);
                if ostro_sdk::is_cancellation(&c) {
                    return Err(StepError::cancelled().into());
                }
                let (winner, loser) = if idx == 0 { (a, b) } else { (b, a) };
                let o = winner.decode(c);
                if shape == 1 {
                    rec.out
                        .rounds
                        .push(format!("select({},{})", winner.label, loser.label));
                    rec.record(winner.label.clone(), winner.id(), o);
                    loser.cancel(ctx);
                    let (label, id) = (loser.label.clone(), loser.id());
                    let lo = loser.wait();
                    rec.record(label, id, lo);
                } else {
                    let c3 = pending.remove(rng.below(pending.len()));
                    rec.out.rounds.push(format!(
                        "select_join({},{},{})",
                        winner.label, loser.label, c3.label
                    ));
                    rec.record(winner.label.clone(), winner.id(), o);
                    let (l1, i1, l2, i2) =
                        (loser.label.clone(), loser.id(), c3.label.clone(), c3.id());
                    let (o1, o2) = join2(loser, c3);
                    if o1 == "cancelled" || o2 == "cancelled" {
                        return Err(StepError::cancelled().into());
                    }
                    rec.record(l1, i1, o1);
                    rec.record(l2, i2, o2);
                }
            }
            _ => {
                let a = pending.remove(rng.below(pending.len()));
                let b = pending.remove(rng.below(pending.len()));
                rec.out
                    .rounds
                    .push(format!("join({},{})", a.label, b.label));
                let (l1, i1, l2, i2) = (a.label.clone(), a.id(), b.label.clone(), b.id());
                let (o1, o2) = join2(a, b);
                if o1 == "cancelled" || o2 == "cancelled" {
                    return Err(StepError::cancelled().into());
                }
                rec.record(l1, i1, o1);
                rec.record(l2, i2, o2);
            }
        }
    }
    Ok(rec.out)
}

ostro_sdk::export!(RaceFuzzWorkflow);

//! Deterministic WASI 0.2 shim built on [`GuestEnv`] (ENG-02).
//!
//! Ostro does not use `wasmtime-wasi`: its sleeps are real time and it brings ambient
//! filesystem/socket authority. This shim implements exactly the 18 `wasi:*@0.2.12`
//! interfaces in `wit-wasi/shim.wit` (vendored WIT, see `wit-wasi/README.md`), with
//! time, randomness and stdio taken only from the store's [`GuestEnv`]:
//!
//! - clocks: `wall_now` / `monotonic_now_ns`; timer pollables are always ready (no
//!   real sleep).
//! - random: `fill_random(WasiRandom | WasiInsecure)`, `insecure_seed()`.
//! - cli: empty environment/args/cwd, stdin at EOF, no terminals, stdout/stderr into the
//!   bounded `write_stdio` sink, `exit` traps with [`GuestExit`](crate::error::GuestExit).
//!
//! Imports are sync + trappable: nothing in the shim awaits. They coexist with the async
//! `ostro:runtime` imports in one `Linker`.

mod cli;
mod clocks;
mod io;
mod random;

use std::marker::PhantomData;

use wasmtime::component::{HasData, Linker};

use crate::env::GuestEnv;

pub use io::{InputStream, IoError, OutputStream, Pollable};

/// Host rep of `wasi:cli/terminal-input.terminal-input` (never handed out).
#[derive(Debug)]
pub struct TerminalInput;

/// Host rep of `wasi:cli/terminal-output.terminal-output` (never handed out).
#[derive(Debug)]
pub struct TerminalOutput;

/// `bindgen!` output for the `ostro:wasi-shim/shim` world.
pub mod generated {
    wasmtime::component::bindgen!({
        world: "shim",
        path: "wit-wasi",
        imports: { default: trappable },
        with: {
            "wasi:io/poll.pollable": crate::host::wasi::Pollable,
            "wasi:io/streams.input-stream": crate::host::wasi::InputStream,
            "wasi:io/streams.output-stream": crate::host::wasi::OutputStream,
            "wasi:io/error.error": crate::host::wasi::IoError,
            "wasi:cli/terminal-input.terminal-input": crate::host::wasi::TerminalInput,
            "wasi:cli/terminal-output.terminal-output": crate::host::wasi::TerminalOutput,
        },
    });
}

/// View of the store data the shim's `Host` traits are implemented on.
///
/// A local wrapper (rather than `impl Host for T`) so the blanket impls do not collide
/// with bindgen's `impl Host for &mut _T` forwarding impls.
pub struct WasiImpl<'a, T>(pub &'a mut T);

struct ShimData<T>(PhantomData<T>);

impl<T: 'static> HasData for ShimData<T> {
    type Data<'a> = WasiImpl<'a, T>;
}

/// Add every shim interface to `linker`.
pub fn add_to_linker<T: GuestEnv>(linker: &mut Linker<T>) -> wasmtime::Result<()> {
    generated::Shim::add_to_linker::<T, ShimData<T>>(linker, |t| WasiImpl(t))
}

/// Per-store marker recording that the "timer pollables do not sleep" warning was
/// emitted (kept in the store's resource table; never handed to the guest).
struct SleepWarned;

impl<T: GuestEnv> WasiImpl<'_, T> {
    fn warn_sleep_once(&mut self) {
        let table = self.0.table();
        if table.iter_mut().any(|(_, v)| v.is::<SleepWarned>()) {
            return;
        }
        if table.push(SleepWarned).is_ok() {
            tracing::warn!(
                "guest subscribed to a WASI timer (e.g. std::thread::sleep); Ostro's WASI \
                 shim never sleeps in real time, use ostro effects timers for durable sleeps"
            );
        }
    }
}

//! `wasi:clocks/{wall-clock,monotonic-clock}` from the deterministic `GuestEnv` clock.
//! Timer pollables are ready immediately: guests never block in real time.

use wasmtime::component::Resource;

use super::WasiImpl;
use super::generated::wasi::clocks::{monotonic_clock, wall_clock};
use super::io::Pollable;
use crate::env::GuestEnv;

impl<T: GuestEnv> wall_clock::Host for WasiImpl<'_, T> {
    fn now(&mut self) -> wasmtime::Result<wall_clock::Datetime> {
        let (seconds, nanoseconds) = self.0.wall_now();
        Ok(wall_clock::Datetime {
            seconds,
            nanoseconds,
        })
    }

    fn resolution(&mut self) -> wasmtime::Result<wall_clock::Datetime> {
        Ok(wall_clock::Datetime {
            seconds: 0,
            nanoseconds: 1_000,
        })
    }
}

impl<T: GuestEnv> monotonic_clock::Host for WasiImpl<'_, T> {
    fn now(&mut self) -> wasmtime::Result<monotonic_clock::Instant> {
        Ok(self.0.monotonic_now_ns())
    }

    fn resolution(&mut self) -> wasmtime::Result<monotonic_clock::Duration> {
        Ok(1)
    }

    fn subscribe_instant(
        &mut self,
        _when: monotonic_clock::Instant,
    ) -> wasmtime::Result<Resource<Pollable>> {
        self.warn_sleep_once();
        self.ready_pollable()
    }

    fn subscribe_duration(
        &mut self,
        _when: monotonic_clock::Duration,
    ) -> wasmtime::Result<Resource<Pollable>> {
        self.warn_sleep_once();
        self.ready_pollable()
    }
}

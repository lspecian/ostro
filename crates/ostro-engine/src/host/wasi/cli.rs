//! `wasi:cli/*`: empty environment, stdin at EOF, stdout/stderr into the bounded sink,
//! no terminals, and `exit` as a typed [`GuestExit`] trap.

use wasmtime::component::Resource;

use super::generated::wasi::cli::{
    environment, exit, stderr, stdin, stdout, terminal_input, terminal_output, terminal_stderr,
    terminal_stdin, terminal_stdout,
};
use super::io::{InputStream, OutputStream};
use super::{TerminalInput, TerminalOutput, WasiImpl};
use crate::env::{GuestEnv, StdStream};
use crate::error::GuestExit;

impl<T: GuestEnv> environment::Host for WasiImpl<'_, T> {
    fn get_environment(&mut self) -> wasmtime::Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }

    fn get_arguments(&mut self) -> wasmtime::Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn initial_cwd(&mut self) -> wasmtime::Result<Option<String>> {
        Ok(None)
    }
}

impl<T: GuestEnv> exit::Host for WasiImpl<'_, T> {
    fn exit(&mut self, status: Result<(), ()>) -> wasmtime::Result<()> {
        let code = if status.is_ok() { 0 } else { 1 };
        Err(wasmtime::Error::new(GuestExit(code)))
    }

    fn exit_with_code(&mut self, status_code: u8) -> wasmtime::Result<()> {
        Err(wasmtime::Error::new(GuestExit(u32::from(status_code))))
    }
}

impl<T: GuestEnv> stdin::Host for WasiImpl<'_, T> {
    fn get_stdin(&mut self) -> wasmtime::Result<Resource<InputStream>> {
        Ok(self.0.table().push(InputStream)?)
    }
}

impl<T: GuestEnv> stdout::Host for WasiImpl<'_, T> {
    fn get_stdout(&mut self) -> wasmtime::Result<Resource<OutputStream>> {
        Ok(self.0.table().push(OutputStream {
            stream: StdStream::Stdout,
        })?)
    }
}

impl<T: GuestEnv> stderr::Host for WasiImpl<'_, T> {
    fn get_stderr(&mut self) -> wasmtime::Result<Resource<OutputStream>> {
        Ok(self.0.table().push(OutputStream {
            stream: StdStream::Stderr,
        })?)
    }
}

impl<T: GuestEnv> terminal_input::Host for WasiImpl<'_, T> {}

impl<T: GuestEnv> terminal_input::HostTerminalInput for WasiImpl<'_, T> {
    fn drop(&mut self, rep: Resource<TerminalInput>) -> wasmtime::Result<()> {
        self.0.table().delete(rep)?;
        Ok(())
    }
}

impl<T: GuestEnv> terminal_output::Host for WasiImpl<'_, T> {}

impl<T: GuestEnv> terminal_output::HostTerminalOutput for WasiImpl<'_, T> {
    fn drop(&mut self, rep: Resource<TerminalOutput>) -> wasmtime::Result<()> {
        self.0.table().delete(rep)?;
        Ok(())
    }
}

impl<T: GuestEnv> terminal_stdin::Host for WasiImpl<'_, T> {
    fn get_terminal_stdin(&mut self) -> wasmtime::Result<Option<Resource<TerminalInput>>> {
        Ok(None)
    }
}

impl<T: GuestEnv> terminal_stdout::Host for WasiImpl<'_, T> {
    fn get_terminal_stdout(&mut self) -> wasmtime::Result<Option<Resource<TerminalOutput>>> {
        Ok(None)
    }
}

impl<T: GuestEnv> terminal_stderr::Host for WasiImpl<'_, T> {
    fn get_terminal_stderr(&mut self) -> wasmtime::Result<Option<Resource<TerminalOutput>>> {
        Ok(None)
    }
}

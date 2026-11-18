//! `wasi:io/{error,poll,streams}`: everything is ready immediately, stdin is at EOF and
//! stdout/stderr write into the bounded `GuestEnv` stdio sink.

use wasmtime::component::Resource;

use super::WasiImpl;
use super::generated::wasi::io::{error, poll, streams};
use crate::env::{GuestEnv, StdStream};

/// Minimum `check-write` permit so a guest past its budget still makes progress (the
/// excess bytes are silently dropped).
const MIN_WRITE_PERMIT: u64 = 4096;

/// Host rep of `wasi:io/poll.pollable`. Every pollable is ready.
#[derive(Debug)]
pub struct Pollable;

/// Host rep of `wasi:io/streams.input-stream` (stdin; always at EOF).
#[derive(Debug)]
pub struct InputStream;

/// Host rep of `wasi:io/streams.output-stream` (stdout or stderr).
#[derive(Debug)]
pub struct OutputStream {
    pub stream: StdStream,
}

/// Host rep of `wasi:io/error.error`. The shim never fails a stream operation with
/// `last-operation-failed`, so none are ever created.
#[derive(Debug)]
pub struct IoError;

impl<T: GuestEnv> WasiImpl<'_, T> {
    pub(super) fn ready_pollable(&mut self) -> wasmtime::Result<Resource<Pollable>> {
        Ok(self.0.table().push(Pollable)?)
    }

    fn output_stream(&mut self, s: &Resource<OutputStream>) -> wasmtime::Result<StdStream> {
        Ok(self.0.table().get(s)?.stream)
    }

    /// Write `bytes` within the remaining budget; the excess is dropped (never traps).
    fn write_bounded(&mut self, stream: StdStream, bytes: &[u8]) {
        let remaining = self.0.stdio_budget(stream);
        let n = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        if n > 0 {
            self.0.write_stdio(stream, &bytes[..n]);
        }
    }

    fn write_zeroes_bounded(&mut self, stream: StdStream, len: u64) {
        let n = len.min(self.0.stdio_budget(stream));
        let mut left = usize::try_from(n).unwrap_or(usize::MAX);
        let zeroes = [0u8; 4096];
        while left > 0 {
            let chunk = left.min(zeroes.len());
            self.0.write_stdio(stream, &zeroes[..chunk]);
            left -= chunk;
        }
    }
}

impl<T: GuestEnv> error::Host for WasiImpl<'_, T> {}

impl<T: GuestEnv> error::HostError for WasiImpl<'_, T> {
    fn to_debug_string(&mut self, _self_: Resource<IoError>) -> wasmtime::Result<String> {
        Ok(String::new())
    }

    fn drop(&mut self, rep: Resource<IoError>) -> wasmtime::Result<()> {
        self.0.table().delete(rep)?;
        Ok(())
    }
}

impl<T: GuestEnv> poll::Host for WasiImpl<'_, T> {
    fn poll(&mut self, in_: Vec<Resource<Pollable>>) -> wasmtime::Result<Vec<u32>> {
        if in_.is_empty() {
            // Matches the spec: polling nothing is a guest bug.
            return Err(wasmtime::Error::msg(
                "wasi:io/poll.poll called with an empty list",
            ));
        }
        let n = u32::try_from(in_.len())?;
        Ok((0..n).collect())
    }
}

impl<T: GuestEnv> poll::HostPollable for WasiImpl<'_, T> {
    fn ready(&mut self, _self_: Resource<Pollable>) -> wasmtime::Result<bool> {
        Ok(true)
    }

    fn block(&mut self, _self_: Resource<Pollable>) -> wasmtime::Result<()> {
        Ok(())
    }

    fn drop(&mut self, rep: Resource<Pollable>) -> wasmtime::Result<()> {
        self.0.table().delete(rep)?;
        Ok(())
    }
}

impl<T: GuestEnv> streams::Host for WasiImpl<'_, T> {}

impl<T: GuestEnv> streams::HostInputStream for WasiImpl<'_, T> {
    fn read(
        &mut self,
        _self_: Resource<InputStream>,
        _len: u64,
    ) -> wasmtime::Result<Result<Vec<u8>, streams::StreamError>> {
        Ok(Err(streams::StreamError::Closed))
    }

    fn blocking_read(
        &mut self,
        _self_: Resource<InputStream>,
        _len: u64,
    ) -> wasmtime::Result<Result<Vec<u8>, streams::StreamError>> {
        Ok(Err(streams::StreamError::Closed))
    }

    fn skip(
        &mut self,
        _self_: Resource<InputStream>,
        _len: u64,
    ) -> wasmtime::Result<Result<u64, streams::StreamError>> {
        Ok(Err(streams::StreamError::Closed))
    }

    fn blocking_skip(
        &mut self,
        _self_: Resource<InputStream>,
        _len: u64,
    ) -> wasmtime::Result<Result<u64, streams::StreamError>> {
        Ok(Err(streams::StreamError::Closed))
    }

    fn subscribe(&mut self, _self_: Resource<InputStream>) -> wasmtime::Result<Resource<Pollable>> {
        self.ready_pollable()
    }

    fn drop(&mut self, rep: Resource<InputStream>) -> wasmtime::Result<()> {
        self.0.table().delete(rep)?;
        Ok(())
    }
}

impl<T: GuestEnv> streams::HostOutputStream for WasiImpl<'_, T> {
    fn check_write(
        &mut self,
        self_: Resource<OutputStream>,
    ) -> wasmtime::Result<Result<u64, streams::StreamError>> {
        let stream = self.output_stream(&self_)?;
        Ok(Ok(self.0.stdio_budget(stream).max(MIN_WRITE_PERMIT)))
    }

    fn write(
        &mut self,
        self_: Resource<OutputStream>,
        contents: Vec<u8>,
    ) -> wasmtime::Result<Result<(), streams::StreamError>> {
        let stream = self.output_stream(&self_)?;
        self.write_bounded(stream, &contents);
        Ok(Ok(()))
    }

    fn blocking_write_and_flush(
        &mut self,
        self_: Resource<OutputStream>,
        contents: Vec<u8>,
    ) -> wasmtime::Result<Result<(), streams::StreamError>> {
        self.write(self_, contents)
    }

    fn flush(
        &mut self,
        _self_: Resource<OutputStream>,
    ) -> wasmtime::Result<Result<(), streams::StreamError>> {
        Ok(Ok(()))
    }

    fn blocking_flush(
        &mut self,
        _self_: Resource<OutputStream>,
    ) -> wasmtime::Result<Result<(), streams::StreamError>> {
        Ok(Ok(()))
    }

    fn subscribe(
        &mut self,
        _self_: Resource<OutputStream>,
    ) -> wasmtime::Result<Resource<Pollable>> {
        self.ready_pollable()
    }

    fn write_zeroes(
        &mut self,
        self_: Resource<OutputStream>,
        len: u64,
    ) -> wasmtime::Result<Result<(), streams::StreamError>> {
        let stream = self.output_stream(&self_)?;
        self.write_zeroes_bounded(stream, len);
        Ok(Ok(()))
    }

    fn blocking_write_zeroes_and_flush(
        &mut self,
        self_: Resource<OutputStream>,
        len: u64,
    ) -> wasmtime::Result<Result<(), streams::StreamError>> {
        self.write_zeroes(self_, len)
    }

    fn splice(
        &mut self,
        _self_: Resource<OutputStream>,
        _src: Resource<InputStream>,
        _len: u64,
    ) -> wasmtime::Result<Result<u64, streams::StreamError>> {
        // The only input stream is stdin, which is at EOF.
        Ok(Err(streams::StreamError::Closed))
    }

    fn blocking_splice(
        &mut self,
        _self_: Resource<OutputStream>,
        _src: Resource<InputStream>,
        _len: u64,
    ) -> wasmtime::Result<Result<u64, streams::StreamError>> {
        Ok(Err(streams::StreamError::Closed))
    }

    fn drop(&mut self, rep: Resource<OutputStream>) -> wasmtime::Result<()> {
        self.0.table().delete(rep)?;
        Ok(())
    }
}

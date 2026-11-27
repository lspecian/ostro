//! Fuzz target `evil_guest` (Phase 14, 14-08; SEC-05, D-P14-17): malformed WIT values
//! through every `ostro:runtime@0.1.0` import, from a real component, against the real host.
//!
//! Once per process the `workflow` world is parsed with wit-parser and
//! `wit_component::dummy_module` gives the exact lowered core imports and required exports
//! ([`Template`]). Per input a core module is emitted with wasm-encoder: those imports, one
//! memory pre-filled with fixed regions plus fuzz bytes, a `cabi_realloc` returning
//! fuzz-chosen (possibly out-of-bounds) pointers, and every exported function (`run`,
//! `describe`, their post-returns, `_initialize`) a fuzz-chosen sequence of at most
//! [`MAX_CALLS`] import calls (optionally followed by an endless loop) with arbitrary
//! canonical-ABI arguments ([`Arg`]: pointers near
//! the memory end, huge lengths, invalid discriminants, invalid UTF-8, host results fed
//! back). The component-type metadata is embedded and `ComponentEncoder` (validation on)
//! wraps it.
//!
//! The component then runs through the executor's path ([`run_component`]): the engine
//! validates and AOT-compiles it, instantiates it, calls `describe` and runs one activation
//! against an in-memory sink, at low limits ([`fuzz_engine_config`]). Invariant: no panic or
//! abort (any outcome, including a trap or a failed attempt, is fine), and afterwards the
//! node budgets are intact ([`assert_budgets_intact`]: no live activation, no admission
//! weight held, no parked instance, no charged memory, no live pool slot, no cached module,
//! fuel within the limit, exactly one outcome reported).
//!
//! The input format is total and stable ([`Program::from_bytes`] / [`Program::to_bytes`]),
//! so the seed corpus under `fuzz/corpus/evil_guest/` keeps its meaning.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use arbitrary::Unstructured;
use ostro_engine::config::{EngineConfig, PoolConfig};
use ostro_engine::core_budget::PoolWeights;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::sink::{
    Activate, ActivationOutcome, ActivationSink, AppendError, GuestLogRecord, RunRef,
};
use ostro_types::journal::{Event, EventBody, Payload, RngSeed, RunStarted};
use wasm_encoder::{
    CodeSection, ConstExpr, DataSection, EntityType, ExportKind, ExportSection, Function,
    FunctionSection, GlobalSection, GlobalType, Ieee32, Ieee64, ImportSection, Instruction, MemArg,
    MemorySection, MemoryType, Module, TypeSection, ValType,
};
use wit_component::{ComponentEncoder, StringEncoding};
use wit_parser::abi::AbiVariant;
use wit_parser::{LiftLowerAbi, ManglingAndAbi, Resolve, WasmImport, WorldId, WorldItem};

/// The frozen WIT 0.1.0 contract (read only).
const WIT: &str = include_str!("../../../wit/ostro-runtime-0.1.0/runtime.wit");

/// Most import calls in one exported function.
pub const MAX_CALLS: usize = 64;
/// Most fuzz bytes copied into the guest memory (at [`DATA`]).
pub const DATA_MAX: usize = 4096;

/// Memory layout of a generated guest: `[ZERO, TEXT)` stays zero (every string or list of
/// length 0 and every all-zero record there is a valid value); `[TEXT, RET)` is ASCII (valid
/// UTF-8, a JSON object at [`JSON_AT`]); `[RET, DATA)` receives import results (return
/// areas); `[DATA, DATA + DATA_MAX)` holds the fuzz bytes; the bump allocator starts at
/// [`BUMP`].
pub const ZERO: u32 = 0;
/// Start of the ASCII region.
pub const TEXT: u32 = 1024;
/// A valid JSON object (`{"a":1}`, 7 bytes) inside the ASCII region.
pub const JSON_AT: u32 = TEXT + 1000;
/// Start of the import result region.
pub const RET: u32 = 4096;
/// Start of the fuzz bytes.
pub const DATA: u32 = 8192;
/// Initial bump pointer of the generated `cabi_realloc`.
pub const BUMP: u32 = 16384;
const PAGE: u32 = 65_536;
const JSON: &[u8] = b"{\"a\":1}";

const HUGE32: [u32; 4] = [0x7fff_ffff, 0x8000_0000, 0xffff_fff0, 0xffff_ffff];
const HUGE64: [i64; 4] = [i64::MAX, i64::MIN, -1, 1 << 32];

/// The core export name of the realloc function.
pub const REALLOC: &str = "cabi_realloc";
/// The core export name of the memory.
pub const MEMORY: &str = "memory";

const MANGLING: ManglingAndAbi = ManglingAndAbi::Legacy(LiftLowerAbi::Sync);

/// One lowered core function: an import of the template, or an export it requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreFunc {
    /// Import module (empty for exports).
    pub module: String,
    /// Import or export name.
    pub name: String,
    /// Core parameter types.
    pub params: Vec<ValType>,
    /// Core result types.
    pub results: Vec<ValType>,
    /// The last parameter is a return pointer the host writes the result to.
    pub retptr: bool,
}

/// The lowered shape of the `ostro:runtime@0.1.0` `workflow` world, from `dummy_module`.
#[derive(Debug)]
pub struct Template {
    /// The resolved WIT package.
    pub resolve: Resolve,
    /// The `workflow` world.
    pub world: WorldId,
    /// Every lowered import, in the dummy module's order.
    pub imports: Vec<CoreFunc>,
    /// Every required function export (`cabi_realloc` and `_initialize` included), in order.
    pub exports: Vec<CoreFunc>,
    /// The `component-type` custom section (id byte included) to append to a core module.
    pub metadata: Vec<u8>,
    /// The dummy module itself.
    pub dummy: Vec<u8>,
}

impl Template {
    /// The process-wide template (built once).
    pub fn get() -> &'static Template {
        static T: OnceLock<Template> = OnceLock::new();
        T.get_or_init(|| Template::build().expect("the frozen 0.1.0 world yields a template"))
    }

    fn build() -> Result<Template, String> {
        let mut resolve = Resolve::default();
        let pkg = resolve
            .push_source("runtime.wit", WIT)
            .map_err(|e| format!("{e:#}"))?;
        let world = resolve
            .select_world(&[pkg], Some("workflow"))
            .map_err(|e| format!("{e:#}"))?;
        let retptrs = retptr_imports(&resolve, world);
        let dummy = wit_component::dummy_module(&resolve, world, MANGLING);
        let mut metadata = Vec::new();
        wit_component::embed_component_metadata(
            &mut metadata,
            &resolve,
            world,
            StringEncoding::UTF8,
            false,
        )
        .map_err(|e| format!("{e:#}"))?;
        let (imports, exports) = parse_template(&dummy, &retptrs)?;
        Ok(Template {
            resolve,
            world,
            imports,
            exports,
            metadata,
            dummy,
        })
    }

    /// Index of the import `module`/`name`.
    pub fn import(&self, module: &str, name: &str) -> Option<usize> {
        self.imports
            .iter()
            .position(|f| f.module == module && f.name == name)
    }

    /// Index of the import named `name` (any module).
    pub fn import_named(&self, name: &str) -> Option<usize> {
        self.imports.iter().position(|f| f.name == name)
    }

    /// The function exports that get a generated body (every one except `cabi_realloc`).
    pub fn bodies(&self) -> impl Iterator<Item = &CoreFunc> {
        self.exports.iter().filter(|e| e.name != REALLOC)
    }

    /// Index (among [`Self::bodies`]) of the export whose name ends with `suffix` (post-return
    /// functions excluded).
    pub fn body_index(&self, suffix: &str) -> Option<usize> {
        self.bodies()
            .position(|e| e.name.ends_with(suffix) && !e.name.starts_with("cabi_post_"))
    }

    /// Append the component-type metadata to `module` and encode it as a component
    /// (validation on). `None` when the encoder rejects it.
    pub fn encode(&self, mut module: Vec<u8>) -> Option<Vec<u8>> {
        module.extend_from_slice(&self.metadata);
        ComponentEncoder::default()
            .validate(true)
            .module(&module)
            .ok()?
            .encode()
            .ok()
    }
}

/// `(module, name)` of every imported WIT function, mapped to whether its lowering takes a
/// return pointer.
fn retptr_imports(resolve: &Resolve, world: WorldId) -> HashMap<(String, String), bool> {
    let mut out = HashMap::new();
    for (key, item) in &resolve.worlds[world].imports {
        let WorldItem::Interface { id, .. } = item else {
            continue;
        };
        for func in resolve.interfaces[*id].functions.values() {
            let names = resolve.wasm_import_name(
                MANGLING,
                WasmImport::Func {
                    interface: Some(key),
                    func,
                },
            );
            let sig = resolve.wasm_signature(AbiVariant::GuestImport, func);
            out.insert(names, sig.retptr);
        }
    }
    out
}

fn num(t: wasmparser::ValType) -> Result<ValType, String> {
    match t {
        wasmparser::ValType::I32 => Ok(ValType::I32),
        wasmparser::ValType::I64 => Ok(ValType::I64),
        wasmparser::ValType::F32 => Ok(ValType::F32),
        wasmparser::ValType::F64 => Ok(ValType::F64),
        other => Err(format!("template uses non-numeric type {other:?}")),
    }
}

type Sig = (Vec<ValType>, Vec<ValType>);

fn sig(ft: &wasmparser::FuncType) -> Result<Sig, String> {
    let params = ft
        .params()
        .iter()
        .map(|t| num(*t))
        .collect::<Result<_, _>>()?;
    let results = ft
        .results()
        .iter()
        .map(|t| num(*t))
        .collect::<Result<_, _>>()?;
    Ok((params, results))
}

fn parse_template(
    dummy: &[u8],
    retptrs: &HashMap<(String, String), bool>,
) -> Result<(Vec<CoreFunc>, Vec<CoreFunc>), String> {
    use wasmparser::{ExternalKind, Parser, Payload as P, TypeRef};
    let err = |e: wasmparser::BinaryReaderError| format!("dummy module: {e}");
    let mut types: Vec<Sig> = Vec::new();
    let mut imports = Vec::new();
    let mut defined: Vec<u32> = Vec::new();
    let mut exports = Vec::new();
    for payload in Parser::new(0).parse_all(dummy) {
        match payload.map_err(err)? {
            P::TypeSection(r) => {
                for ft in r.into_iter_err_on_gc_types() {
                    types.push(sig(&ft.map_err(err)?)?);
                }
            }
            P::ImportSection(r) => {
                for imp in r.into_imports() {
                    let imp = imp.map_err(err)?;
                    let TypeRef::Func(ty) = imp.ty else {
                        return Err(format!("non-function import {}", imp.name));
                    };
                    let (params, results) = types
                        .get(ty as usize)
                        .cloned()
                        .ok_or("import type out of range")?;
                    let key = (imp.module.to_string(), imp.name.to_string());
                    imports.push(CoreFunc {
                        retptr: retptrs.get(&key).copied().unwrap_or(false),
                        module: key.0,
                        name: key.1,
                        params,
                        results,
                    });
                }
            }
            P::FunctionSection(r) => {
                for ty in r {
                    defined.push(ty.map_err(err)?);
                }
            }
            P::ExportSection(r) => {
                for e in r {
                    let e = e.map_err(err)?;
                    match e.kind {
                        ExternalKind::Func => {
                            let ty = (e.index as usize)
                                .checked_sub(imports.len())
                                .and_then(|local| defined.get(local))
                                .ok_or("export of an unknown function")?;
                            let (params, results) = types
                                .get(*ty as usize)
                                .cloned()
                                .ok_or("export type out of range")?;
                            exports.push(CoreFunc {
                                module: String::new(),
                                name: e.name.to_string(),
                                params,
                                results,
                                retptr: false,
                            });
                        }
                        ExternalKind::Memory => {}
                        other => return Err(format!("unexpected export kind {other:?}")),
                    }
                }
            }
            _ => {}
        }
    }
    if !exports.iter().any(|e| e.name == REALLOC) {
        return Err("the dummy module exports no cabi_realloc".into());
    }
    Ok((imports, exports))
}

/// Where one canonical-ABI argument (or returned value) comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arg {
    /// `0` (a valid pointer to zeros, an empty list, the first case).
    Zero,
    /// `n % 16`.
    Small(u8),
    /// Raw bits, truncated to the parameter type.
    Const(u64),
    /// `memory size - n` (straddles the memory end).
    NearEnd(u8),
    /// An edge value (`2^31 - 1`, `2^31`, `0xFFFF_FFF0`, `u32::MAX`; i64 extremes).
    Huge(u8),
    /// A pointer into the ASCII region (`TEXT + 4n`).
    Text(u8),
    /// An `i32.load` (`i64.load`) of the import result region (`RET + 4 (n % 64)`): host
    /// results fed back (e.g. resource handles).
    Load(u8),
    /// A return area (`RET + 256 (n % 16)`).
    Ret(u8),
    /// A pointer into the fuzz bytes (`DATA + n % DATA_MAX`).
    Fuzz(u16),
    /// A length (`n`).
    Len(u16),
}

const ARG_TAGS: u8 = 10;

/// Reads with zero padding once the input is exhausted (so the input format is total).
pub struct Reader<'a, 'b>(pub &'b mut Unstructured<'a>);

impl Reader<'_, '_> {
    /// One byte.
    pub fn u8(&mut self) -> u8 {
        let mut b = [0u8; 1];
        let _ = self.0.fill_buffer(&mut b);
        b[0]
    }

    /// Two bytes, little endian.
    pub fn u16(&mut self) -> u16 {
        let mut b = [0u8; 2];
        let _ = self.0.fill_buffer(&mut b);
        u16::from_le_bytes(b)
    }

    /// Four bytes, little endian.
    pub fn u32(&mut self) -> u32 {
        let mut b = [0u8; 4];
        let _ = self.0.fill_buffer(&mut b);
        u32::from_le_bytes(b)
    }

    /// Eight bytes, little endian.
    pub fn u64(&mut self) -> u64 {
        let mut b = [0u8; 8];
        let _ = self.0.fill_buffer(&mut b);
        u64::from_le_bytes(b)
    }

    /// One [`Arg`].
    pub fn arg(&mut self) -> Arg {
        match self.u8() % ARG_TAGS {
            0 => Arg::Zero,
            1 => Arg::Small(self.u8()),
            2 => Arg::Const(self.u64()),
            3 => Arg::NearEnd(self.u8()),
            4 => Arg::Huge(self.u8()),
            5 => Arg::Text(self.u8()),
            6 => Arg::Load(self.u8()),
            7 => Arg::Ret(self.u8()),
            8 => Arg::Fuzz(self.u16()),
            _ => Arg::Len(self.u16()),
        }
    }
}

impl Arg {
    /// Append the encoding [`Reader::arg`] reads back.
    pub fn write(self, out: &mut Vec<u8>) {
        match self {
            Arg::Zero => out.push(0),
            Arg::Small(n) => out.extend([1, n]),
            Arg::Const(c) => {
                out.push(2);
                out.extend(c.to_le_bytes());
            }
            Arg::NearEnd(n) => out.extend([3, n]),
            Arg::Huge(n) => out.extend([4, n]),
            Arg::Text(n) => out.extend([5, n]),
            Arg::Load(n) => out.extend([6, n]),
            Arg::Ret(n) => out.extend([7, n]),
            Arg::Fuzz(n) => {
                out.push(8);
                out.extend(n.to_le_bytes());
            }
            Arg::Len(n) => {
                out.push(9);
                out.extend(n.to_le_bytes());
            }
        }
    }

    /// The i32 this argument stands for (loads excluded).
    fn value32(self, mem_bytes: u32) -> u32 {
        match self {
            Arg::Zero | Arg::Load(_) => 0,
            Arg::Small(n) => u32::from(n % 16),
            Arg::Const(c) => c as u32,
            Arg::NearEnd(n) => mem_bytes.wrapping_sub(u32::from(n)),
            Arg::Huge(n) => HUGE32[usize::from(n % 4)],
            Arg::Text(n) => TEXT + 4 * u32::from(n),
            Arg::Ret(n) => RET + 256 * u32::from(n % 16),
            Arg::Fuzz(n) => DATA + u32::from(n) % DATA_MAX as u32,
            Arg::Len(n) => u32::from(n),
        }
    }

    fn load_addr(n: u8) -> i32 {
        (RET + 4 * u32::from(n % 64)) as i32
    }

    /// Push this argument as a value of type `ty` (memory 0 of `mem_bytes` bytes).
    pub fn emit(self, f: &mut Function, ty: ValType, mem_bytes: u32) {
        let v = self.value32(mem_bytes);
        match (ty, self) {
            (ValType::I32, Arg::Load(n)) => {
                f.instruction(&Instruction::I32Const(Arg::load_addr(n)));
                f.instruction(&Instruction::I32Load(memarg(2)));
            }
            (ValType::I32, _) => {
                f.instruction(&Instruction::I32Const(v as i32));
            }
            (ValType::I64, Arg::Load(n)) => {
                f.instruction(&Instruction::I32Const(Arg::load_addr(n)));
                f.instruction(&Instruction::I64Load(memarg(3)));
            }
            (ValType::I64, Arg::Const(c)) => {
                f.instruction(&Instruction::I64Const(c as i64));
            }
            (ValType::I64, Arg::Huge(n)) => {
                f.instruction(&Instruction::I64Const(HUGE64[usize::from(n % 4)]));
            }
            (ValType::I64, _) => {
                f.instruction(&Instruction::I64Const(i64::from(v)));
            }
            (ValType::F32, _) => {
                f.instruction(&Instruction::F32Const(Ieee32::new(v)));
            }
            (ValType::F64, Arg::Const(c)) => {
                f.instruction(&Instruction::F64Const(Ieee64::new(c)));
            }
            (ValType::F64, _) => {
                f.instruction(&Instruction::F64Const(Ieee64::new(u64::from(v))));
            }
            (ValType::V128, _) => {
                f.instruction(&Instruction::V128Const(i128::from(v)));
            }
            (ValType::Ref(_), _) => {
                // Never produced: the template and the generated modules use numeric types
                // only (wasm-smith runs with reference types off).
                f.instruction(&Instruction::Unreachable);
            }
        }
    }
}

fn memarg(align: u32) -> MemArg {
    MemArg {
        offset: 0,
        align,
        memory_index: 0,
    }
}

/// One import call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// Index into [`Template::imports`] (reduced modulo the import count).
    pub import: u16,
    /// One per core parameter (missing ones are [`Arg::Zero`]).
    pub args: Vec<Arg>,
}

/// The generated body of one exported function.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Body {
    /// Import calls, in order (results dropped).
    pub calls: Vec<Call>,
    /// The returned values, one per core result (missing ones are [`Arg::Zero`]).
    pub results: Vec<Arg>,
    /// After the calls, loop forever (only fuel and the epoch deadline end it).
    pub spin: bool,
}

/// The generated `cabi_realloc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Realloc {
    /// Aligned bump allocation from [`BUMP`] (never checks the memory end).
    Bump,
    /// Always returns this pointer (e.g. `0xFFFF_FFF0`).
    Const(u32),
    /// Traps.
    Unreachable,
}

/// One generated guest (the decoded fuzz input).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    /// Extra memory pages over the first (`% 4`).
    pub extra_pages: u8,
    /// The allocator.
    pub realloc: Realloc,
    /// One per [`Template::bodies`], in order (missing ones are empty and return zeros).
    pub bodies: Vec<Body>,
    /// Copied to [`DATA`] (at most [`DATA_MAX`] bytes).
    pub data: Vec<u8>,
}

impl Program {
    /// A program whose every export does nothing and returns zeros (valid results).
    pub fn empty(t: &Template) -> Program {
        Program {
            extra_pages: 0,
            realloc: Realloc::Bump,
            bodies: t
                .bodies()
                .map(|e| Body {
                    calls: Vec::new(),
                    results: vec![Arg::Zero; e.results.len()],
                    spin: false,
                })
                .collect(),
            data: Vec::new(),
        }
    }

    /// Decode a fuzz input (total: every byte string is a program).
    pub fn from_bytes(t: &Template, data: &[u8]) -> Program {
        let mut u = Unstructured::new(data);
        let mut r = Reader(&mut u);
        let extra_pages = r.u8() % 4;
        let realloc = match r.u8() % 3 {
            0 => Realloc::Bump,
            1 => Realloc::Const(r.u32()),
            _ => Realloc::Unreachable,
        };
        let n = t.imports.len().max(1);
        let mut bodies = Vec::new();
        for export in t.bodies() {
            // Bit 7: spin; bits 0-6: the call count.
            let head = r.u8();
            let spin = head & 0x80 != 0;
            let ncalls = usize::from(head & 0x7f) % (MAX_CALLS + 1);
            let mut calls = Vec::with_capacity(ncalls);
            for _ in 0..ncalls {
                let import = r.u16() % n as u16;
                let params = t
                    .imports
                    .get(usize::from(import))
                    .map_or(0, |f| f.params.len());
                let args = (0..params).map(|_| r.arg()).collect();
                calls.push(Call { import, args });
            }
            let results = export.results.iter().map(|_| r.arg()).collect();
            bodies.push(Body {
                calls,
                results,
                spin,
            });
        }
        let rest = u.take_rest();
        Program {
            extra_pages,
            realloc,
            bodies,
            data: rest[..rest.len().min(DATA_MAX)].to_vec(),
        }
    }

    /// The fuzz input [`Self::from_bytes`] decodes to this program.
    pub fn to_bytes(&self, t: &Template) -> Vec<u8> {
        let mut out = vec![self.extra_pages % 4];
        match self.realloc {
            Realloc::Bump => out.push(0),
            Realloc::Const(p) => {
                out.push(1);
                out.extend(p.to_le_bytes());
            }
            Realloc::Unreachable => out.push(2),
        }
        let empty = Body::default();
        let n = t.imports.len().max(1) as u16;
        for (i, export) in t.bodies().enumerate() {
            let body = self.bodies.get(i).unwrap_or(&empty);
            let calls = &body.calls[..body.calls.len().min(MAX_CALLS)];
            out.push(calls.len() as u8 | if body.spin { 0x80 } else { 0 });
            for c in calls {
                let import = c.import % n;
                out.extend(import.to_le_bytes());
                let params = t
                    .imports
                    .get(usize::from(import))
                    .map_or(0, |f| f.params.len());
                for k in 0..params {
                    c.args.get(k).copied().unwrap_or(Arg::Zero).write(&mut out);
                }
            }
            for k in 0..export.results.len() {
                body.results
                    .get(k)
                    .copied()
                    .unwrap_or(Arg::Zero)
                    .write(&mut out);
            }
        }
        out.extend_from_slice(&self.data[..self.data.len().min(DATA_MAX)]);
        out
    }

    /// The memory size in bytes.
    pub fn mem_bytes(&self) -> u32 {
        PAGE * (1 + u32::from(self.extra_pages % 4))
    }

    /// The core module (without component metadata).
    pub fn module(&self, t: &Template) -> Vec<u8> {
        let n_imports = t.imports.len() as u32;
        let mut types = TypeSection::new();
        let mut imports = ImportSection::new();
        for (i, f) in t.imports.iter().enumerate() {
            types
                .ty()
                .function(f.params.iter().copied(), f.results.iter().copied());
            imports.import(&f.module, &f.name, EntityType::Function(i as u32));
        }
        let mut functions = FunctionSection::new();
        let mut exports = ExportSection::new();
        let mut code = CodeSection::new();
        exports.export(MEMORY, ExportKind::Memory, 0);
        let mem_bytes = self.mem_bytes();
        let empty = Body::default();
        let mut body_ix = 0;
        for (k, e) in t.exports.iter().enumerate() {
            let index = n_imports + k as u32;
            types
                .ty()
                .function(e.params.iter().copied(), e.results.iter().copied());
            functions.function(index);
            exports.export(&e.name, ExportKind::Func, index);
            if e.name == REALLOC {
                code.function(&realloc_body(self.realloc, 0));
            } else {
                let body = self.bodies.get(body_ix).unwrap_or(&empty);
                body_ix += 1;
                code.function(&call_body(t, body, &e.results, mem_bytes));
            }
        }
        let mut memories = MemorySection::new();
        memories.memory(MemoryType {
            minimum: u64::from(mem_bytes / PAGE),
            maximum: None,
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        let mut globals = GlobalSection::new();
        globals.global(
            GlobalType {
                val_type: ValType::I32,
                mutable: true,
                shared: false,
            },
            &ConstExpr::i32_const(BUMP as i32),
        );
        let mut data = DataSection::new();
        data.active(0, &ConstExpr::i32_const(TEXT as i32), text_region());
        if !self.data.is_empty() {
            data.active(
                0,
                &ConstExpr::i32_const(DATA as i32),
                self.data[..self.data.len().min(DATA_MAX)].iter().copied(),
            );
        }
        let mut m = Module::new();
        m.section(&types)
            .section(&imports)
            .section(&functions)
            .section(&memories)
            .section(&globals)
            .section(&exports)
            .section(&code)
            .section(&data);
        m.finish()
    }
}

/// The ASCII region: `a..z` repeated, with the JSON object at [`JSON_AT`].
fn text_region() -> Vec<u8> {
    let mut v: Vec<u8> = (0..(RET - TEXT)).map(|i| b'a' + (i % 26) as u8).collect();
    let at = (JSON_AT - TEXT) as usize;
    v[at..at + JSON.len()].copy_from_slice(JSON);
    v
}

/// `cabi_realloc(old, old_size, align, new_size) -> ptr`; the bump pointer is global
/// `bump_global`.
pub fn realloc_body(mode: Realloc, bump_global: u32) -> Function {
    use Instruction as I;
    let mut f = Function::new([(1, ValType::I32)]);
    match mode {
        Realloc::Bump => {
            // p = (bump + align - 1) & -align; bump = p + new_size; p
            f.instruction(&I::GlobalGet(bump_global))
                .instruction(&I::LocalGet(2))
                .instruction(&I::I32Add)
                .instruction(&I::I32Const(1))
                .instruction(&I::I32Sub)
                .instruction(&I::I32Const(0))
                .instruction(&I::LocalGet(2))
                .instruction(&I::I32Sub)
                .instruction(&I::I32And)
                .instruction(&I::LocalTee(4))
                .instruction(&I::LocalGet(3))
                .instruction(&I::I32Add)
                .instruction(&I::GlobalSet(bump_global))
                .instruction(&I::LocalGet(4));
        }
        Realloc::Const(p) => {
            f.instruction(&I::I32Const(p as i32));
        }
        Realloc::Unreachable => {
            f.instruction(&I::Unreachable);
        }
    }
    f.instruction(&I::End);
    f
}

/// Emit `calls` (the imports are functions `0..`; results dropped).
pub fn emit_calls(f: &mut Function, t: &Template, calls: &[Call], mem_bytes: u32) {
    let n = t.imports.len().max(1);
    for c in calls.iter().take(MAX_CALLS) {
        let ix = usize::from(c.import) % n;
        let Some(import) = t.imports.get(ix) else {
            continue;
        };
        emit_args(f, &import.params, &c.args, mem_bytes);
        f.instruction(&Instruction::Call(ix as u32));
        for _ in &import.results {
            f.instruction(&Instruction::Drop);
        }
    }
}

/// Push one value per type of `types` from `args` (missing ones are [`Arg::Zero`]).
pub fn emit_args(f: &mut Function, types: &[ValType], args: &[Arg], mem_bytes: u32) {
    for (k, ty) in types.iter().enumerate() {
        args.get(k)
            .copied()
            .unwrap_or(Arg::Zero)
            .emit(f, *ty, mem_bytes);
    }
}

/// A function body making `body.calls`, then returning `body.results` as `results`.
pub fn call_body(t: &Template, body: &Body, results: &[ValType], mem_bytes: u32) -> Function {
    let mut f = Function::new(Vec::<(u32, ValType)>::new());
    emit_calls(&mut f, t, &body.calls, mem_bytes);
    if body.spin {
        f.instruction(&Instruction::Loop(wasm_encoder::BlockType::Empty))
            .instruction(&Instruction::Br(0))
            .instruction(&Instruction::End);
    }
    emit_args(&mut f, results, &body.results, mem_bytes);
    f.instruction(&Instruction::End);
    f
}

/// Engine limits for fuzzing: fuel 10^6, a 100 ms wall deadline (10 ms epoch ticks), 16 MiB
/// per memory, a pool of one component instance, a 64 MiB node memory budget.
pub fn fuzz_engine_config() -> EngineConfig {
    EngineConfig {
        pool: PoolConfig {
            total_instances: 1,
            max_memory_bytes: 16 * 1024 * 1024,
            max_unused_warm_slots: 1,
            total_core_instances: Some(16),
            ..PoolConfig::default()
        },
        activation_timeout: Duration::from_millis(100),
        activation_fuel: 1_000_000,
        hot_wait: Duration::from_millis(1),
        node_memory_budget: 64 * 1024 * 1024,
        compile_permits: 1,
        ..EngineConfig::default()
    }
}

/// The process-wide engine (one runtime; one input at a time holds it).
pub struct Harness {
    tokio: tokio::runtime::Runtime,
    /// The engine runtime.
    pub rt: Runtime,
}

/// Lock the process-wide harness (built on first use; poison-tolerant).
pub fn harness() -> MutexGuard<'static, Harness> {
    static H: OnceLock<Mutex<Harness>> = OnceLock::new();
    H.get_or_init(|| {
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let rt = tokio
            .block_on(async {
                Runtime::new(
                    fuzz_engine_config(),
                    RuntimeOptions {
                        token_key: [7; 32],
                        ..RuntimeOptions::default()
                    },
                )
            })
            .expect("fuzz engine runtime");
        Mutex::new(Harness { tokio, rt })
    })
    .lock()
    .unwrap_or_else(|p| p.into_inner())
}

/// What happened to one component.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// `ComponentEncoder` accepted the module.
    pub encoded: bool,
    /// The engine validated and compiled the component.
    pub prepared: bool,
    /// One instantiation (the activation's store setup) succeeded.
    pub instantiated: bool,
    /// `describe()`: the workflow count or the error.
    pub describe: Option<Result<usize, String>>,
    /// The activation outcome.
    pub outcome: Option<ActivationOutcome>,
    /// Events the activation appended.
    pub appended: u64,
}

/// In-memory sink: accepts appends in order, keeps no blobs, records the outcome and fuel.
#[derive(Debug, Default)]
pub struct FuzzSink {
    state: Mutex<SinkState>,
}

#[derive(Debug, Default)]
struct SinkState {
    next: u64,
    appended: u64,
    done: Vec<ActivationOutcome>,
    fuel: Option<u64>,
}

/// Events one fuzz activation may append before the sink reports itself unavailable.
const SINK_EVENT_CAP: u64 = 100_000;

impl FuzzSink {
    fn new(journal_len: u64) -> Self {
        Self {
            state: Mutex::new(SinkState {
                next: journal_len,
                ..SinkState::default()
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, SinkState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// `ActivationSink` is an `async_trait` trait; this crate does not depend on the macro, so
// the async methods are written in their expanded form.
impl ActivationSink for FuzzSink {
    fn append<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _run: &'life1 RunRef,
        _attempt: u32,
        from_seq: u64,
        events: Vec<Event>,
    ) -> BoxFut<'async_trait, Result<u64, AppendError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let mut st = self.lock();
            if from_seq != st.next {
                return Err(AppendError::Conflict { expected: st.next });
            }
            let n = events.len() as u64;
            if st.appended + n > SINK_EVENT_CAP {
                return Err(AppendError::Unavailable("fuzz sink full".into()));
            }
            st.next += n;
            st.appended += n;
            Ok(st.next)
        })
    }

    fn fetch_blob<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _sha: &'life1 [u8; 32],
    ) -> BoxFut<'async_trait, Result<Vec<u8>, AppendError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Err(AppendError::Unavailable("fuzz sink stores no blobs".into())) })
    }

    fn done<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _run: &'life1 RunRef,
        _attempt: u32,
        outcome: ActivationOutcome,
    ) -> BoxFut<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.lock().done.push(outcome);
        })
    }

    fn guest_log(&self, _rec: GuestLogRecord) {}

    fn handles_guest_logs(&self) -> bool {
        true
    }

    fn fuel_consumed(&self, fuel: u64) {
        self.lock().fuel = Some(fuel);
    }
}

/// The run's `RunStarted` (seq 0) for the module with hash `hash`.
fn run_started(hash: &str) -> Event {
    const AT: u64 = 1_700_000_000_000_000_000;
    Event {
        seq: 0,
        ts_nanos: AT,
        body: EventBody::RunStarted(RunStarted {
            workflow: "fuzz".to_string(),
            workflow_version: 1,
            input: Payload {
                content_type: "application/octet-stream".to_string(),
                data: b"fuzz input".to_vec(),
            },
            rng_seed: RngSeed([0x5a; 32]),
            accepted_at_nanos: AT,
            deadline_nanos: None,
            module_hash: hash.to_string(),
            tenant: None,
            parent_run_id: None,
            continued_from: None,
        }),
    }
}

/// Panic unless every node budget is back to idle (the targets' budget invariant).
pub fn assert_budgets_intact(rt: &Runtime, stage: &str) {
    assert_eq!(rt.live_activations(), 0, "{stage}: live activation leaked");
    assert_eq!(
        rt.pool_budget().in_use(),
        PoolWeights::default(),
        "{stage}: admission weight leaked"
    );
    assert_eq!(
        rt.parked().parked_count(),
        0,
        "{stage}: parked instance leaked"
    );
    assert_eq!(
        rt.engine().budget().used(),
        0,
        "{stage}: charged memory leaked"
    );
    assert_eq!(rt.instance_pre_len(), 0, "{stage}: prepared module cached");
    if let Some(m) = rt.engine().pooling_metrics() {
        assert_eq!(m.component_instances(), 0, "{stage}: pool instance leaked");
        assert_eq!(m.core_instances(), 0, "{stage}: pool core instance leaked");
        assert_eq!(m.memories(), 0, "{stage}: pool memory leaked");
        assert_eq!(m.tables(), 0, "{stage}: pool table leaked");
    }
}

/// How far [`run_component`] goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Depth {
    /// Validate, compile and instantiate once.
    Instantiate,
    /// Also call `describe()` and run one activation.
    Full,
}

/// Run `component` through the executor's path under [`fuzz_engine_config`], checking
/// [`assert_budgets_intact`] after every stage. Modules are prepared uncached (no L2 entry),
/// so iterations never pollute each other.
pub fn run_component(component: &[u8], depth: Depth) -> Report {
    let h = harness();
    let mut report = Report {
        encoded: true,
        ..Report::default()
    };
    let rt = &h.rt;
    h.tokio.block_on(async {
        let Ok(p) = rt.prepare(component) else {
            assert_budgets_intact(rt, "prepare");
            return;
        };
        report.prepared = true;
        let run = RunRef {
            partition: 0,
            run_id: "fuzz".to_string(),
        };
        let journal = vec![run_started(&p.module_hash_hex)];
        report.instantiated = rt
            .instantiate_probe(&p, run.clone(), journal.clone())
            .await
            .is_ok();
        assert_budgets_intact(rt, "instantiate");
        if depth == Depth::Instantiate {
            return;
        }
        report.describe = Some(
            rt.describe(&p)
                .await
                .map(|m| m.len())
                .map_err(|e| e.to_string()),
        );
        assert_budgets_intact(rt, "describe");
        let sink = Arc::new(FuzzSink::new(journal.len() as u64));
        let outcome = rt
            .activate(
                &p,
                Activate {
                    run,
                    attempt: 1,
                    module_sha: p.sha,
                    journal,
                    inputs: None,
                    cancel: Default::default(),
                },
                sink.clone(),
            )
            .await;
        assert_budgets_intact(rt, "activate");
        let st = sink.lock();
        assert_eq!(
            st.done,
            vec![outcome.clone()],
            "the outcome is reported exactly once"
        );
        if let Some(fuel) = st.fuel {
            assert!(
                fuel <= rt.engine().config().activation_fuel,
                "fuel accounting: {fuel} consumed, over the limit"
            );
        }
        report.appended = st.appended;
        drop(st);
        report.outcome = Some(outcome);
    });
    report
}

/// The component for fuzz input `data`, or `None` when `ComponentEncoder` rejects it.
pub fn build_component(data: &[u8]) -> Option<Vec<u8>> {
    let t = Template::get();
    t.encode(Program::from_bytes(t, data).module(t))
}

/// Run fuzz input `data` and report what happened (the fuzz entry point's body).
pub fn exec(data: &[u8]) -> Report {
    match build_component(data) {
        Some(c) => run_component(&c, Depth::Full),
        None => Report::default(),
    }
}

/// Fuzz entry point.
pub fn evil_guest(data: &[u8]) {
    let _ = exec(data);
}

/// The program whose `run` calls every import once with well-formed arguments (zeros:
/// empty strings and lists, first cases, `none`; short ASCII names; return areas in the
/// result region; the step handle `begin-step` returned), the step calls last. Every export
/// returns a valid zero value.
pub fn valid_calls_program(t: &Template) -> Program {
    let mut p = Program::empty(t);
    // Journaled commands inside an open step are a protocol violation: every other import
    // first, then begin-step, the step methods and the handle drop.
    let is_step = |i: &usize| {
        let n = t.imports[*i].name.as_str();
        n.starts_with("[method]step.") || n.starts_with("[resource-drop]step")
    };
    let begin = t.import_named("begin-step");
    let mut order: Vec<usize> = (0..t.imports.len())
        .filter(|i| Some(*i) != begin && !is_step(i))
        .collect();
    order.extend(begin);
    let (drops, methods): (Vec<usize>, Vec<usize>) = (0..t.imports.len())
        .filter(is_step)
        .partition(|i| t.imports[*i].name.starts_with("[resource-drop]"));
    order.extend(methods);
    order.extend(drops);
    let calls = order
        .into_iter()
        .map(|i| {
            let f = &t.imports[i];
            let mut args = vec![Arg::Zero; f.params.len()];
            if f.retptr
                && let Some(last) = args.last_mut()
            {
                // begin-step writes its step-start (the handle at +4) and create-await-token
                // its await-token (the op id at +0, cancelled by cancel-op next) to RET; the
                // other results go to RET + 256.
                *last = if matches!(f.name.as_str(), "begin-step" | "create-await-token") {
                    Arg::Ret(0)
                } else {
                    Arg::Ret(1)
                };
            }
            let name = f.name.as_str();
            if name.starts_with("[method]step.") || name.starts_with("[resource-drop]step") {
                args[0] = Arg::Load(1);
            } else if matches!(
                name,
                "begin-step" | "patched" | "wait-signal" | "start-child"
            ) && args.len() >= 2
            {
                args[0] = Arg::Text(0);
                args[1] = Arg::Small(5);
            } else if name == "set-status" && args.len() >= 2 {
                args[0] = Arg::Text(((JSON_AT - TEXT) / 4) as u8);
                args[1] = Arg::Small(JSON.len() as u8);
            } else if name == "emit" && args.len() >= 3 {
                // Level info, message "abcde", no fields.
                args[0] = Arg::Small(2);
                args[1] = Arg::Text(0);
                args[2] = Arg::Small(5);
            } else if name == "cancel-op" && !args.is_empty() {
                args[0] = Arg::Load(0);
            } else if name == "random-bytes" && !args.is_empty() {
                args[0] = Arg::Small(8);
            }
            Call {
                import: i as u16,
                args,
            }
        })
        .collect();
    if let Some(run) = t.body_index("#run") {
        p.bodies[run].calls = calls;
    }
    p
}

/// A program whose `run` makes the single call `call`, then returns a valid result.
pub fn single_call_program(t: &Template, call: Call) -> Program {
    let mut p = Program::empty(t);
    if let Some(run) = t.body_index("#run") {
        p.bodies[run].calls = vec![call];
    }
    p
}

/// A program whose `_initialize` loops forever (it runs during instantiation): the
/// regression input of the unbounded calibration instantiation found by 14-08.
pub fn spinning_initialize_program(t: &Template) -> Program {
    let mut p = Program::empty(t);
    if let Some(init) = t.bodies().position(|e| e.name == "_initialize") {
        p.bodies[init].spin = true;
    }
    p
}

/// The hand-written malformed cases `(name, program)`; each must fail the attempt.
pub fn malformed_programs(t: &Template) -> Vec<(&'static str, Program)> {
    let emit = t
        .import("ostro:runtime/log@0.1.0", "emit")
        .expect("log.emit is imported") as u16;
    let uuid = t
        .import("ostro:runtime/context@0.1.0", "uuid-v7")
        .expect("context.uuid-v7 is imported") as u16;
    // emit(level, message ptr, message len, fields ptr, fields len)
    let emit_call = |args: Vec<Arg>| Call { import: emit, args };
    let mut out = vec![
        (
            "oob_string_pointer",
            single_call_program(
                t,
                emit_call(vec![
                    Arg::Small(2),
                    Arg::NearEnd(16),
                    Arg::Len(4096),
                    Arg::Zero,
                    Arg::Zero,
                ]),
            ),
        ),
        (
            "invalid_discriminant",
            single_call_program(
                t,
                emit_call(vec![
                    Arg::Small(7),
                    Arg::Text(0),
                    Arg::Small(5),
                    Arg::Zero,
                    Arg::Zero,
                ]),
            ),
        ),
        (
            "huge_list_length",
            single_call_program(
                t,
                emit_call(vec![
                    Arg::Small(2),
                    Arg::Text(0),
                    Arg::Small(5),
                    Arg::Zero,
                    Arg::Huge(1),
                ]),
            ),
        ),
    ];
    let mut utf8 = single_call_program(
        t,
        emit_call(vec![
            Arg::Small(2),
            Arg::Fuzz(0),
            Arg::Small(4),
            Arg::Zero,
            Arg::Zero,
        ]),
    );
    utf8.data = vec![0xff, 0xfe, 0xed, 0x80];
    out.push(("invalid_utf8", utf8));
    let mut realloc = single_call_program(
        t,
        Call {
            import: uuid,
            args: vec![Arg::Ret(1)],
        },
    );
    realloc.realloc = Realloc::Const(0xFFFF_FFF0);
    out.push(("realloc_out_of_bounds", realloc));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_covers_the_world() {
        let t = Template::get();
        for name in [
            "emit",
            "begin-step",
            "http-fetch",
            "wait",
            "uuid-v7",
            "info",
        ] {
            assert!(t.import_named(name).is_some(), "{name} imported");
        }
        assert!(t.body_index("#run").is_some());
        assert!(t.body_index("#describe").is_some());
        assert!(t.imports.iter().any(|f| f.retptr));
    }

    #[test]
    fn program_bytes_round_trip() {
        let t = Template::get();
        let mut p = valid_calls_program(t);
        p.data = vec![1, 2, 3];
        p.bodies[0].spin = true;
        assert_eq!(Program::from_bytes(t, &p.to_bytes(t)), p);
        let spin = spinning_initialize_program(t);
        assert!(spin.bodies.iter().any(|b| b.spin));
        assert_eq!(Program::from_bytes(t, &spin.to_bytes(t)), spin);
        for (_, p) in malformed_programs(t) {
            assert_eq!(Program::from_bytes(t, &p.to_bytes(t)), p);
        }
    }

    #[test]
    fn every_input_decodes_to_a_valid_module() {
        let t = Template::get();
        for data in [&[][..], &[0xff; 64][..], &[0x07; 300][..]] {
            let m = Program::from_bytes(t, data).module(t);
            wasmparser::validate(&m).expect("generated core module validates");
        }
    }
}

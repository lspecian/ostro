//! Fuzz target `smith_component` (Phase 14, 14-08; SEC-05, D-P14-17): wasm-smith core modules
//! wrapped as `ostro:runtime@0.1.0` components, through validation, AOT compilation,
//! instantiation, `describe` and `run` under fuel and epoch limits.
//!
//! wasm-smith `=0.261.0` is built without its `wasmparser` feature in this workspace (the
//! manifests are 14-01's), so its `available_imports` / `exports` options are unavailable.
//! The world's shape is applied after generation instead: the smith module is generated
//! without imports or exports, then re-encoded (`wasm_encoder::reencode`) with the
//! template's lowered imports prepended (every smith function index shifted past them), a
//! memory added when smith made none, and every required export bound either to a smith
//! function of exactly the right core type (fuzz choice) or to a generated function that
//! makes fuzz-chosen import calls (as in `evil_guest`), calls a smith function with fuzz
//! arguments and returns fuzz values. `ComponentEncoder` then wraps it (validation on).
//!
//! wasm-smith runs with the proposals the engine enables (no threads, GC, exceptions,
//! memory64, reference types; at most one memory of at most 16 MiB, one table). Invariant
//! as for `evil_guest`: any outcome but a host panic, abort or budget leak is acceptable.

use std::convert::Infallible;

use arbitrary::Unstructured;
use wasm_encoder::reencode::{Error as ReencodeError, Reencode};
use wasm_encoder::{
    CodeSection, ConstExpr, DataCountSection, DataSection, ElementSection, EntityType, ExportKind,
    ExportSection, Function, FunctionSection, GlobalSection, GlobalType, ImportSection,
    Instruction, MemorySection, MemoryType, Module, StartSection, TableSection, TypeSection,
    ValType,
};

use crate::evil_guest::{
    BUMP, Call, Depth, MEMORY, REALLOC, Reader, Realloc, Report, Template, emit_args, emit_calls,
    realloc_body, run_component,
};

/// Most import calls in one generated export (smith code is the main subject here).
pub const MAX_SMITH_CALLS: u8 = 8;
const PAGE: u64 = 65_536;
const MAX_MEMORY: u64 = 16 * 1024 * 1024;

/// The wasm-smith configuration for one input.
pub fn smith_config(u: &mut Unstructured<'_>) -> arbitrary::Result<wasm_smith::Config> {
    let mut c = wasm_smith::Config::default();
    c.available_imports = None;
    c.exports = None;
    c.module_shape = None;
    c.max_imports = 0;
    c.min_imports = 0;
    c.max_exports = 0;
    c.min_exports = 0;
    c.export_everything = false;
    c.allow_start_export = false;
    c.min_funcs = 1;
    c.max_funcs = u.int_in_range(1..=32)?;
    c.max_instructions = u.int_in_range(0..=512)?;
    c.max_types = 32;
    c.max_globals = 16;
    c.max_data_segments = 8;
    c.max_element_segments = 4;
    c.max_elements = 64;
    c.max_tags = 0;
    c.max_memories = 1;
    c.min_memories = u32::from(u.arbitrary::<bool>()?);
    c.max_memory32_bytes = MAX_MEMORY;
    c.memory64_enabled = false;
    c.max_tables = 1;
    c.max_table_elements = 1000;
    c.max_nesting_depth = u.int_in_range(0..=10)?;
    c.reference_types_enabled = false;
    c.gc_enabled = false;
    c.exceptions_enabled = false;
    c.threads_enabled = false;
    c.shared_everything_threads_enabled = false;
    c.custom_page_sizes_enabled = false;
    c.wide_arithmetic_enabled = false;
    c.custom_descriptors_enabled = false;
    c.compact_imports_enabled = false;
    c.bulk_memory_enabled = true;
    c.multi_value_enabled = true;
    c.simd_enabled = u.arbitrary()?;
    c.relaxed_simd_enabled = c.simd_enabled && u.arbitrary()?;
    c.tail_call_enabled = u.arbitrary()?;
    c.disallow_traps = u.arbitrary()?;
    c.canonicalize_nans = false;
    c.generate_custom_sections = false;
    c.allow_invalid_funcs = false;
    c.min_uleb_size = 0;
    Ok(c)
}

/// Shifts every function index past the prepended imports.
struct Shift(u32);

impl Reencode for Shift {
    type Error = Infallible;

    fn function_index(&mut self, func: u32) -> Result<u32, ReencodeError<Infallible>> {
        Ok(func + self.0)
    }
}

type Sig = (Vec<ValType>, Vec<ValType>);

fn re(e: ReencodeError<Infallible>) -> String {
    e.to_string()
}

fn wp(e: wasmparser::BinaryReaderError) -> String {
    e.to_string()
}

/// Re-encode the smith module `smith` with the template's imports and exports (see the
/// module docs); choices come from `u`.
pub fn merge(t: &Template, smith: &[u8], u: &mut Unstructured<'_>) -> Result<Vec<u8>, String> {
    use wasmparser::{Parser, Payload as P};
    let n_imports = t.imports.len() as u32;
    let mut sh = Shift(n_imports);
    let mut types = TypeSection::new();
    let mut sigs: Vec<Sig> = Vec::new();
    let mut func_types: Vec<u32> = Vec::new();
    let mut tables: Option<TableSection> = None;
    let mut memories: Option<MemorySection> = None;
    let mut mem_bytes = PAGE;
    let mut globals = GlobalSection::new();
    let mut n_globals = 0u32;
    let mut start: Option<u32> = None;
    let mut elements: Option<ElementSection> = None;
    let mut data_count: Option<u32> = None;
    let mut code = CodeSection::new();
    let mut data: Option<DataSection> = None;
    for payload in Parser::new(0).parse_all(smith) {
        match payload.map_err(wp)? {
            P::TypeSection(r) => {
                for ft in r.clone().into_iter_err_on_gc_types() {
                    let ft = ft.map_err(wp)?;
                    let conv = |ts: &[wasmparser::ValType], sh: &mut Shift| {
                        ts.iter()
                            .map(|v| sh.val_type(*v))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(re)
                    };
                    let params = conv(ft.params(), &mut sh)?;
                    let results = conv(ft.results(), &mut sh)?;
                    sigs.push((params, results));
                }
                sh.parse_type_section(&mut types, r).map_err(re)?;
            }
            P::ImportSection(r) => {
                if r.count() != 0 {
                    return Err("wasm-smith generated imports".into());
                }
            }
            P::TagSection(_) => return Err("wasm-smith generated tags".into()),
            P::FunctionSection(r) => {
                for ty in r {
                    func_types.push(ty.map_err(wp)?);
                }
            }
            P::TableSection(r) => {
                let mut s = TableSection::new();
                sh.parse_table_section(&mut s, r).map_err(re)?;
                tables = Some(s);
            }
            P::MemorySection(r) => {
                if let Some(m) = r.clone().into_iter().next() {
                    let m = m.map_err(wp)?;
                    if m.memory64 || m.shared {
                        return Err("unsupported memory".into());
                    }
                    mem_bytes = m.initial.saturating_mul(PAGE).min(MAX_MEMORY);
                }
                let mut s = MemorySection::new();
                sh.parse_memory_section(&mut s, r).map_err(re)?;
                memories = Some(s);
            }
            P::GlobalSection(r) => {
                n_globals = r.count();
                sh.parse_global_section(&mut globals, r).map_err(re)?;
            }
            P::StartSection { func, .. } => start = Some(func + n_imports),
            P::ElementSection(r) => {
                let mut s = ElementSection::new();
                sh.parse_element_section(&mut s, r).map_err(re)?;
                elements = Some(s);
            }
            P::DataCountSection { count, .. } => data_count = Some(count),
            P::CodeSectionEntry(body) => sh.parse_function_body(&mut code, body).map_err(re)?,
            P::DataSection(r) => {
                let mut s = DataSection::new();
                sh.parse_data_section(&mut s, r).map_err(re)?;
                data = Some(s);
            }
            _ => {}
        }
    }
    let mem_bytes = u32::try_from(mem_bytes).unwrap_or(u32::MAX);
    let n_smith_types = sigs.len() as u32;
    let n_smith_funcs = func_types.len() as u32;

    let mut imports = ImportSection::new();
    for (i, f) in t.imports.iter().enumerate() {
        types
            .ty()
            .function(f.params.iter().copied(), f.results.iter().copied());
        imports.import(
            &f.module,
            &f.name,
            EntityType::Function(n_smith_types + i as u32),
        );
    }
    let mut next_type = n_smith_types + n_imports;
    let mut functions = FunctionSection::new();
    for ty in &func_types {
        functions.function(*ty);
    }
    let memories = memories.unwrap_or_else(|| {
        let mut s = MemorySection::new();
        s.memory(MemoryType {
            minimum: 1,
            maximum: None,
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        s
    });
    let bump_global = n_globals;
    globals.global(
        GlobalType {
            val_type: ValType::I32,
            mutable: true,
            shared: false,
        },
        &ConstExpr::i32_const(BUMP as i32),
    );
    let mut exports = ExportSection::new();
    exports.export(MEMORY, ExportKind::Memory, 0);
    let mut next_func = n_imports + n_smith_funcs;
    let mut r = Reader(u);
    let n = t.imports.len().max(1) as u16;
    for e in &t.exports {
        let want: Sig = (e.params.clone(), e.results.clone());
        let candidates: Vec<u32> = func_types
            .iter()
            .enumerate()
            .filter(|(_, ty)| sigs.get(**ty as usize) == Some(&want))
            .map(|(i, _)| i as u32)
            .collect();
        let pick = r.u8();
        if !candidates.is_empty() && pick % 2 == 0 {
            let f = candidates[usize::from(pick / 2) % candidates.len()];
            exports.export(&e.name, ExportKind::Func, n_imports + f);
            continue;
        }
        types
            .ty()
            .function(e.params.iter().copied(), e.results.iter().copied());
        functions.function(next_type);
        next_type += 1;
        exports.export(&e.name, ExportKind::Func, next_func);
        next_func += 1;
        let body = if e.name == REALLOC {
            let mode = match r.u8() % 4 {
                0 | 1 => Realloc::Bump,
                2 => Realloc::Const(r.u32()),
                _ => Realloc::Unreachable,
            };
            realloc_body(mode, bump_global)
        } else {
            let mut f = Function::new(Vec::<(u32, ValType)>::new());
            let ncalls = r.u8() % (MAX_SMITH_CALLS + 1);
            let calls: Vec<Call> = (0..ncalls)
                .map(|_| {
                    let import = r.u16() % n;
                    let params = t.imports[usize::from(import)].params.len();
                    Call {
                        import,
                        args: (0..params).map(|_| r.arg()).collect(),
                    }
                })
                .collect();
            emit_calls(&mut f, t, &calls, mem_bytes);
            if n_smith_funcs > 0 && r.u8() % 4 != 0 {
                let fi = u32::from(r.u16()) % n_smith_funcs;
                let (params, results) = &sigs[func_types[fi as usize] as usize];
                let args: Vec<_> = params.iter().map(|_| r.arg()).collect();
                emit_args(&mut f, params, &args, mem_bytes);
                f.instruction(&Instruction::Call(n_imports + fi));
                for _ in results {
                    f.instruction(&Instruction::Drop);
                }
            }
            let rets: Vec<_> = e.results.iter().map(|_| r.arg()).collect();
            emit_args(&mut f, &e.results, &rets, mem_bytes);
            f.instruction(&Instruction::End);
            f
        };
        code.function(&body);
    }

    let mut m = Module::new();
    m.section(&types).section(&imports).section(&functions);
    if let Some(s) = &tables {
        m.section(s);
    }
    m.section(&memories).section(&globals).section(&exports);
    if let Some(function_index) = start {
        m.section(&StartSection { function_index });
    }
    if let Some(s) = &elements {
        m.section(s);
    }
    if let Some(count) = data_count {
        m.section(&DataCountSection { count });
    }
    m.section(&code);
    if let Some(s) = &data {
        m.section(s);
    }
    Ok(m.finish())
}

/// The component for fuzz input `data`, or `None` when generation fails or
/// `ComponentEncoder` rejects the module.
pub fn build_component(data: &[u8]) -> Option<Vec<u8>> {
    let t = Template::get();
    let mut u = Unstructured::new(data);
    let config = smith_config(&mut u).ok()?;
    let module = wasm_smith::Module::new(config, &mut u).ok()?;
    let merged = merge(t, &module.to_bytes(), &mut u).ok()?;
    t.encode(merged)
}

/// Run fuzz input `data` down to `depth` and report what happened.
pub fn exec_to(data: &[u8], depth: Depth) -> Report {
    match build_component(data) {
        Some(c) => run_component(&c, depth),
        None => Report::default(),
    }
}

/// Run fuzz input `data` fully (validate, compile, instantiate, `describe`, `run`).
pub fn exec(data: &[u8]) -> Report {
    exec_to(data, Depth::Full)
}

/// Fuzz entry point.
pub fn smith_component(data: &[u8]) {
    let _ = exec(data);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merged_modules_validate() {
        let t = Template::get();
        let mut ok = 0;
        for seed in 0u64..40 {
            let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
            let bytes: Vec<u8> = (0..2048)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect();
            let mut u = Unstructured::new(&bytes);
            let Ok(config) = smith_config(&mut u) else {
                continue;
            };
            let Ok(module) = wasm_smith::Module::new(config, &mut u) else {
                continue;
            };
            let merged = merge(t, &module.to_bytes(), &mut u).expect("merge");
            wasmparser::validate(&merged).expect("merged module validates");
            ok += 1;
        }
        assert!(ok >= 30, "only {ok}/40 inputs generated a module");
    }
}

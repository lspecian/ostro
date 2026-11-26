# Fuzz regressions

Minimised crash inputs found by the fuzz targets in `fuzz/fuzz_targets/` (Phase 14,
D-P14-17). Policy:

- One directory per target, named like the target binary (`fuzz/regressions/<target>/`).
- Each file is a **minimised** input (`cargo fuzz tmin <target> <crash>`), named
  `<short-description>-<sha256-prefix>`; never a raw, unminimised crash file.
- A regression is added together with the fix and stays forever: removing one needs a
  reason in the commit message.
- Every input is replayed on **stable** Rust in the normal test suite through
  `ostro_fuzz_targets::run` (no libFuzzer, no nightly needed), so a fixed crash can never
  come back unnoticed. The replay harness and the first inputs land with 14-07.
- Inputs never contain secrets or production data.

#!/usr/bin/env bash
# SC1 clean-machine e2e (plan 10-19): `ostro init -> ostro dev -> ostro deploy -> ostro run`
# on a machine with no pre-installed language toolchain, in under --timeout seconds per
# language (default 600).
#
# Usage: bash scripts/clean-machine.sh <rust|python|ts|all> (--image IMG | --local)
#                                      [--docker] [--no-cc] [--ostro PATH]
#                                      [--timeout SECS] [--cache DIR] [--print-plan]
#
# Modes:
#   --image IMG   a fresh `docker run --rm` container of IMG (ubuntu:24.04, debian:12) with
#                 only ca-certificates installed. The static musl ostro is mounted
#                 read-only at /usr/local/bin/ostro; no cc, no node, no cargo, no uv. The
#                 network is on (toolchains download hermetically into a fresh cache).
#   --image IMG --docker
#                 the same container, but the build runs as `ostro deploy --docker`: the
#                 host's Docker socket and docker CLI are mounted, and the work dir is a host
#                 dir mounted at the same path (the Docker daemon resolves bind mounts on the
#                 host). For a quick check of the path macOS / musl hosts take.
#   --local       no Docker: `env -i` with PATH=/usr/bin:/bin (after checking that cargo,
#                 rustup, node, npm, uv, go and python3.12 are not on it), a fresh HOME,
#                 OSTRO_TOOLCHAIN_DIR and TMPDIR. `--local --docker` builds with
#                 `ostro deploy --docker` (the macOS path: a host with Docker, no toolchain;
#                 the docker CLI's directory is added to PATH).
#   --no-cc       mask cc/gcc/clang with failing stubs (a PATH shim dir) and set
#                 OSTRO_NO_CC=1, so the Rust build takes the zig cc fallback even when the
#                 host has a C compiler.
#   --cache DIR   (--local) use DIR as OSTRO_TOOLCHAIN_DIR and keep it: a second run with
#                 the same DIR measures the warm path (toolchains already downloaded).
#   --print-plan  print the commands of each language run without running anything.
#
# Per language, inside the environment:
#   ostro doctor --json; ostro init <work>/app --lang L;
#   ostro dev --ephemeral --time-skipping --listen 127.0.0.1:0 (background, ready line parsed);
#   ostro deploy <work>/app --json [--docker];
#   ostro run hello --input '{"name":"ada"}' --json; ostro signal <run> approve --payload '{"ok":true}';
#   ostro result <run> --wait=120s --json  (auto time skipping covers the 3-day sleep);
#   assert completed + approved == true.
# Prints `CLEAN <lang> <image|local> OK total=<s> doctor=<s> init=<s> dev=<s> deploy=<s> run=<s>`
# and fails when total > --timeout.
#
# The container modes need Docker (CI: the `clean-machine` job of .github/workflows/ci.yml).
set -euo pipefail

SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
ROOT="$(cd "$(dirname "$SELF")/.." && pwd)"
MUSL_TARGET="x86_64-unknown-linux-musl"

die() { echo "clean-machine: $*" >&2; exit 2; }
now_ms() { local t; t=$(date +%s%N); echo $((t / 1000000)); }
secs() { local ms=$1; printf '%d.%d' $((ms / 1000)) $(((ms % 1000) / 100)); }

# --------------------------------------------------------------------------------------
# Inner mode: the steps of one language, run inside the prepared environment.
# Environment: CM_LANG, CM_WORK, CM_LABEL, CM_TIMEOUT, CM_DOCKER (0/1).
# --------------------------------------------------------------------------------------
inner() {
  local lang="$CM_LANG" work="$CM_WORK" label="$CM_LABEL" timeout="$CM_TIMEOUT"
  local app="$work/app" devlog="$work/dev.log" t0 t addr out run_id
  local d_doctor d_init d_dev d_deploy d_run
  mkdir -p "$work"
  DEV_PID=""
  cleanup_dev() {
    local dev_pid="${DEV_PID:-}"
    if [[ -n "$dev_pid" ]] && kill -0 "$dev_pid" 2>/dev/null; then
      kill "$dev_pid" 2>/dev/null || true
      wait "$dev_pid" 2>/dev/null || true
    fi
  }
  trap cleanup_dev EXIT
  trap 'echo "CLEAN $lang $label FAIL: line $LINENO: $BASH_COMMAND" >&2' ERR
  fail() {
    echo "CLEAN $lang $label FAIL: $*" >&2
    if [[ -f "$devlog" ]]; then
      echo "--- last 40 lines of ostro dev output ---" >&2
      tail -n 40 "$devlog" >&2
    fi
    exit 1
  }
  echo "== [$lang/$label] ostro $(ostro --version 2>/dev/null || echo '?'); PATH=$PATH"
  local t_start; t_start=$(now_ms)

  t0=$(now_ms)
  ostro doctor --json >"$work/doctor.json" || fail "ostro doctor --json failed"
  d_doctor=$(($(now_ms) - t0))
  echo "== [$lang/$label] doctor: $(sed -nE 's/^  "(platform|glibc|musl|offline)": (.*),?$/\1=\2/p' "$work/doctor.json" | tr -d ',"' | tr '\n' ' ')"

  t0=$(now_ms)
  ostro init "$app" --lang "$lang" || fail "ostro init failed"
  d_init=$(($(now_ms) - t0))

  t0=$(now_ms)
  ostro dev --ephemeral --time-skipping --listen 127.0.0.1:0 >"$devlog" 2>"$work/dev.err" &
  DEV_PID=$!
  local dev_pid=$DEV_PID
  addr=""
  for _ in $(seq 1 600); do
    addr=$(sed -nE 's/^.+ listening on http:\/\/([^ ]+) \(data: .*\) ready in [0-9]+ ms$/\1/p' "$devlog" | head -n1)
    [[ -n "$addr" ]] && break
    kill -0 "$dev_pid" 2>/dev/null || { cat "$work/dev.err" >&2; fail "ostro dev exited before the ready line"; }
    sleep 0.1
  done
  [[ -n "$addr" ]] || fail "no ready line from ostro dev within 60 s"
  export OSTRO_SERVER="http://$addr"
  d_dev=$(($(now_ms) - t0))
  echo "== [$lang/$label] ostro dev ready at $OSTRO_SERVER"

  t0=$(now_ms)
  local deploy=(ostro deploy "$app" --json)
  [[ "${CM_DOCKER:-0}" == 1 ]] && deploy+=(--docker)
  "${deploy[@]}" >"$work/deploy.json" || fail "${deploy[*]} failed"
  d_deploy=$(($(now_ms) - t0))
  grep -q '"kind": *"ostro.deploy.result"' "$work/deploy.json" || fail "unexpected deploy output: $(head -c 400 "$work/deploy.json")"
  grep -q '"name": *"hello"' "$work/deploy.json" || fail "deploy did not register workflow hello"

  t0=$(now_ms)
  out=$(ostro run hello --input '{"name":"ada"}' --json) || fail "ostro run failed"
  run_id=$(grep -oE '"run_id": *"[^"]+"' <<<"$out" | head -n1 | sed -E 's/.*"([^"]+)"$/\1/')
  [[ -n "$run_id" ]] || fail "no run_id in: $out"
  ostro signal "$run_id" approve --payload '{"ok":true}' --json >"$work/signal.json" || fail "ostro signal failed"
  out=$(ostro result "$run_id" --wait=120s --json) || fail "ostro result failed: $out"
  d_run=$(($(now_ms) - t0))
  echo "$out" >"$work/result.json"
  grep -qE '"status": *"completed"' <<<"$out" || fail "run $run_id not completed: $out"
  grep -qE '"approved": *true' <<<"$out" || fail "run $run_id not approved: $out"
  grep -qE '"greeting": *"hello ada"' <<<"$out" || fail "unexpected greeting: $out"

  cleanup_dev
  DEV_PID=""
  t=$(($(now_ms) - t_start))
  if [[ -n "${OSTRO_ZIG_SHIM_LOG:-}" && -s "${OSTRO_ZIG_SHIM_LOG}" ]]; then
    echo "== [$lang/$label] zig cc fallback exercised: $(wc -l <"$OSTRO_ZIG_SHIM_LOG") shim calls"
  elif [[ "$lang" == rust ]]; then
    echo "== [$lang/$label] zig cc fallback not exercised (host cc used)"
  fi
  local line="CLEAN $lang $label OK total=$(secs "$t") doctor=$(secs "$d_doctor") init=$(secs "$d_init") dev=$(secs "$d_dev") deploy=$(secs "$d_deploy") run=$(secs "$d_run")"
  if ((t > timeout * 1000)); then
    echo "CLEAN $lang $label FAIL: total $(secs "$t") s > ${timeout} s (${line#CLEAN * OK })" >&2
    exit 1
  fi
  echo "$line"
}

if [[ "${1:-}" == "--inner" ]]; then
  inner
  exit 0
fi

# --------------------------------------------------------------------------------------
# Outer mode: argument parsing and environment setup.
# --------------------------------------------------------------------------------------
LANGS_ARG="" IMAGE="" LOCAL=0 DOCKER_BUILD=0 NO_CC=0 OSTRO_BIN="" TIMEOUT=600 PRINT_PLAN=0 CACHE_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    rust|python|ts|all) LANGS_ARG="$1" ;;
    --image) IMAGE="${2:?--image needs a value}"; shift ;;
    --image=*) IMAGE="${1#--image=}" ;;
    --local) LOCAL=1 ;;
    --docker) DOCKER_BUILD=1 ;;
    --no-cc) NO_CC=1 ;;
    --ostro) OSTRO_BIN="${2:?--ostro needs a value}"; shift ;;
    --ostro=*) OSTRO_BIN="${1#--ostro=}" ;;
    --timeout) TIMEOUT="${2:?--timeout needs a value}"; shift ;;
    --timeout=*) TIMEOUT="${1#--timeout=}" ;;
    --cache) CACHE_DIR="${2:?--cache needs a value}"; shift ;;
    --cache=*) CACHE_DIR="${1#--cache=}" ;;
    --print-plan) PRINT_PLAN=1 ;;
    -h|--help) sed -n '2,40p' "$SELF"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
  shift
done
[[ -n "$LANGS_ARG" ]] || die "missing <rust|python|ts|all>"
if [[ -n "$IMAGE" && "$LOCAL" == 1 ]] || [[ -z "$IMAGE" && "$LOCAL" == 0 ]]; then
  die "exactly one of --image IMG or --local is required"
fi
[[ "$TIMEOUT" =~ ^[0-9]+$ ]] || die "--timeout must be a number of seconds"
if [[ -n "$CACHE_DIR" ]]; then
  [[ "$LOCAL" == 1 ]] || die "--cache is only supported with --local"
  mkdir -p "$CACHE_DIR"
  CACHE_DIR="$(cd "$CACHE_DIR" && pwd)"
fi
if [[ "$LANGS_ARG" == all ]]; then LANGS=(rust python ts); else LANGS=("$LANGS_ARG"); fi

if [[ -z "$OSTRO_BIN" ]]; then
  for c in "${CARGO_TARGET_DIR:-$ROOT/target}/$MUSL_TARGET/release/ostro" "$ROOT/target/$MUSL_TARGET/release/ostro"; do
    [[ -x "$c" ]] && { OSTRO_BIN="$c"; break; }
  done
  if [[ -z "$OSTRO_BIN" && "$LOCAL" == 1 ]]; then
    for c in "${CARGO_TARGET_DIR:-$ROOT/target}/debug/ostro" "$ROOT/target/debug/ostro"; do
      [[ -x "$c" ]] && { OSTRO_BIN="$c"; break; }
    done
  fi
  if [[ -z "$OSTRO_BIN" ]]; then
    [[ "$PRINT_PLAN" == 1 ]] || die "no ostro binary found; pass --ostro PATH (container modes need the static musl build)"
    OSTRO_BIN="$ROOT/target/$MUSL_TARGET/release/ostro"
  fi
fi
if [[ "$PRINT_PLAN" == 0 ]]; then
  [[ -x "$OSTRO_BIN" ]] || die "ostro binary not executable: $OSTRO_BIN"
  OSTRO_BIN="$(cd "$(dirname "$OSTRO_BIN")" && pwd)/$(basename "$OSTRO_BIN")"
fi

print_cmd() { printf '%q ' "$@"; echo; }

# ---- container modes ----
APT='apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ca-certificates >/dev/null'

container_cmd() { # <lang> <workdir-on-host-or-empty>
  local lang="$1" hostwork="$2" docker_bin
  # --shm-size (optional since eph-fix): `ostro dev --ephemeral` uses /dev/shm only with
  # >= 512 MiB free and otherwise falls back to the temp dir, so Docker's 64 MB default works.
  # 1g keeps the ephemeral data on tmpfs, so the dev timings compare with a host run.
  CMD=(docker run --rm --network host --shm-size 1g
    -v "$OSTRO_BIN:/usr/local/bin/ostro:ro"
    -v "$SELF:/clean-machine.sh:ro"
    -e CM_LANG="$lang" -e CM_LABEL="$IMAGE" -e CM_TIMEOUT="$TIMEOUT")
  if [[ "$DOCKER_BUILD" == 1 ]]; then
    docker_bin="$(command -v "${OSTRO_DOCKER:-docker}" 2>/dev/null || echo /usr/bin/docker)"
    docker_bin="$(readlink -f "$docker_bin" 2>/dev/null || echo "$docker_bin")"
    # The daemon resolves bind mounts on the host: the work dir, the toolchain cache and the
    # ostro binary (OSTRO_LINUX_BIN) are visible at the same path inside and outside.
    CMD+=(-v /var/run/docker.sock:/var/run/docker.sock
      -v "$docker_bin:/usr/local/bin/docker:ro"
      -v "$OSTRO_BIN:$OSTRO_BIN:ro"
      -v "$hostwork:$hostwork"
      -e CM_WORK="$hostwork/w" -e CM_DOCKER=1 -e OSTRO_LINUX_BIN="$OSTRO_BIN"
      -e OSTRO_TOOLCHAIN_DIR="$hostwork/toolchains" -e HOME="$hostwork/home"
      -e CM_CHOWN="$(id -u):$(id -g)" -e CM_HOSTWORK="$hostwork")
  else
    CMD+=(-e CM_WORK=/work -e HOME=/root -e OSTRO_TOOLCHAIN_DIR=/root/.ostro-toolchains)
  fi
  [[ "$NO_CC" == 1 ]] && CMD+=(-e OSTRO_NO_CC=1)
  CMD+=(-e OSTRO_ZIG_SHIM_LOG=/tmp/zig-shim.log)
  CMD+=("$IMAGE" bash -c "set -e; $APT; for t in cc gcc clang cargo rustup node npm uv go; do if command -v \$t >/dev/null; then echo \"clean-machine: \$t present in the image\"; fi; done; rc=0; bash /clean-machine.sh --inner || rc=\$?; if [ -n \"\${CM_CHOWN:-}\" ]; then chown -R \"\$CM_CHOWN\" \"\$CM_HOSTWORK\"; fi; exit \$rc")
}

# ---- local mode ----
FORBIDDEN=(cargo rustup node npm uv go python3.12)

local_env() { # <lang> <scratch>: fills CMD with the env -i invocation
  local lang="$1" scratch="$2" path="/usr/bin:/bin" t found=()
  # fs <cmd...>: filesystem setup, skipped by --print-plan (which only prints).
  fs() { if [[ "$PRINT_PLAN" == 0 ]]; then "$@"; fi; }
  for t in "${FORBIDDEN[@]}"; do
    if PATH="$path" command -v "$t" >/dev/null 2>&1; then found+=("$t=$(PATH="$path" command -v "$t")"); fi
  done
  if [[ "$PRINT_PLAN" == 0 && ${#found[@]} -gt 0 ]]; then
    die "--local needs a PATH without language toolchains, but /usr/bin:/bin has: ${found[*]}"
  fi
  if [[ "$NO_CC" == 1 ]]; then
    fs mkdir -p "$scratch/nocc"
    for t in cc gcc clang c++ g++ clang++; do
      if [[ "$PRINT_PLAN" == 0 ]]; then
        printf '#!/bin/sh\necho "clean-machine --no-cc: %s is masked" >&2\nexit 127\n' "$t" >"$scratch/nocc/$t"
        chmod +x "$scratch/nocc/$t"
      fi
    done
    path="$scratch/nocc:$path"
  fi
  if [[ "$DOCKER_BUILD" == 1 ]]; then
    local d; d="$(command -v "${OSTRO_DOCKER:-docker}" 2>/dev/null)" || d=docker
    [[ "$PRINT_PLAN" == 1 || "$d" == /* ]] || die "--docker: docker not on PATH"
    fs mkdir -p "$scratch/dockerbin"
    fs ln -sf "$d" "$scratch/dockerbin/docker"
    path="$path:$scratch/dockerbin"
  fi
  fs mkdir -p "$scratch/bin" "$scratch/home" "$scratch/toolchains" "$scratch/tmp"
  fs ln -sf "$OSTRO_BIN" "$scratch/bin/ostro"
  CMD=(env -i "HOME=$scratch/home" "PATH=$scratch/bin:$path" "TMPDIR=$scratch/tmp"
    "OSTRO_TOOLCHAIN_DIR=${CACHE_DIR:-$scratch/toolchains}" "TERM=dumb" "LANG=C.UTF-8"
    "CM_LANG=$lang" "CM_WORK=$scratch/w" "CM_LABEL=local" "CM_TIMEOUT=$TIMEOUT"
    "CM_DOCKER=$DOCKER_BUILD" "OSTRO_ZIG_SHIM_LOG=$scratch/zig-shim.log")
  [[ "$NO_CC" == 1 ]] && CMD+=("OSTRO_NO_CC=1")
  [[ -n "${OSTRO_TOOLCHAIN_MIRROR:-}" ]] && CMD+=("OSTRO_TOOLCHAIN_MIRROR=$OSTRO_TOOLCHAIN_MIRROR")
  if [[ "$DOCKER_BUILD" == 1 ]]; then
    CMD+=("OSTRO_LINUX_BIN=$OSTRO_BIN")
    [[ -n "${DOCKER_HOST:-}" ]] && CMD+=("DOCKER_HOST=$DOCKER_HOST")
  fi
  CMD+=(bash "$SELF" --inner)
}

if [[ "$LOCAL" == 1 && "$PRINT_PLAN" == 0 ]]; then
  if [[ -x /usr/bin/python3 ]]; then
    echo "clean-machine: informational: /usr/bin/python3 exists ($(/usr/bin/python3 --version 2>&1)); the hermetic build uses uv-managed CPython, not it"
  fi
  if [[ "$NO_CC" == 0 ]] && PATH=/usr/bin:/bin command -v cc >/dev/null 2>&1; then
    echo "clean-machine: note: cc is on /usr/bin; the Rust build uses it (pass --no-cc for the zig fallback)"
  fi
fi
if [[ -n "$IMAGE" && "$PRINT_PLAN" == 0 ]]; then
  command -v "${OSTRO_DOCKER:-docker}" >/dev/null 2>&1 && "${OSTRO_DOCKER:-docker}" info >/dev/null 2>&1 \
    || die "docker not available (human_needed: run in CI or on a Docker host)"
  file "$OSTRO_BIN" 2>/dev/null | grep -Eq 'statically linked|static-pie linked' \
    || echo "clean-machine: warning: $OSTRO_BIN is not a static binary; the container may not run it" >&2
fi

rc=0
results=()
for lang in "${LANGS[@]}"; do
  scratch=""
  if [[ "$PRINT_PLAN" == 1 ]]; then
    scratch="<scratch-$lang>"
  else
    scratch="$(mktemp -d "${TMPDIR:-/tmp}/ostro-clean-$lang.XXXXXX")"
  fi
  if [[ -n "$IMAGE" ]]; then
    container_cmd "$lang" "$scratch"
  else
    local_env "$lang" "$scratch"
  fi
  if [[ "$PRINT_PLAN" == 1 ]]; then
    echo "# $lang ($([[ -n "$IMAGE" ]] && echo "image $IMAGE" || echo local)$([[ "$DOCKER_BUILD" == 1 ]] && echo ', deploy --docker'))"
    print_cmd "${CMD[@]}"
    echo "#   inner: ostro doctor --json; ostro init <work>/app --lang $lang;"
    echo "#          ostro dev --ephemeral --time-skipping --listen 127.0.0.1:0 & (ready line -> OSTRO_SERVER);"
    echo "#          ostro deploy <work>/app --json$([[ "$DOCKER_BUILD" == 1 ]] && echo ' --docker');"
    echo "#          ostro run hello --input '{\"name\":\"ada\"}' --json; ostro signal <run> approve --payload '{\"ok\":true}';"
    echo "#          ostro result <run> --wait=120s --json (completed, approved == true)"
    continue
  fi
  log="$scratch/out.log"
  if "${CMD[@]}" 2>&1 | tee "$log"; then
    results+=("$(grep -E '^CLEAN ' "$log" | tail -n1)")
  else
    rc=1
    results+=("$(grep -E '^CLEAN ' "$log" | tail -n1 || echo "CLEAN $lang FAIL")")
  fi
  # The cache holds about 1 GB per language; keep it only on failure for inspection.
  if [[ "$rc" == 0 ]]; then
    chmod -R u+w "$scratch" 2>/dev/null || true
    rm -rf "$scratch" 2>/dev/null || echo "clean-machine: note: could not fully remove $scratch" >&2
  else
    echo "clean-machine: kept $scratch for inspection" >&2
  fi
done

if [[ "$PRINT_PLAN" == 0 ]]; then
  echo "================ clean-machine summary ================"
  printf '  %s\n' "${results[@]}"  # indented: `^CLEAN` matches each run once
fi
exit "$rc"

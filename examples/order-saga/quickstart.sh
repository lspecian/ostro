#!/usr/bin/env bash
# Clean-machine quickstart timing for examples/order-saga (plan 05-17).
#
# Clones the Ostro repository from a LOCAL path (default: the repository this script is
# in; never a remote) into a temporary directory and runs the README quickstart there:
#
#   rustup target add wasm32-wasip2
#   cargo build --release --locked -p ostro          (from the repository root)
#   cargo build --release --target wasm32-wasip2     (from examples/order-saga)
#   ostro dev &
#   ostro deploy target/wasm32-wasip2/release/order_saga.wasm --activate
#   ostro run order-saga --input '{"order_id":"o-1","amount":4999,"sku":"BOOK-42","qty":1}'
#   ostro signal <run> approve --payload true
#   ostro result <run> --wait
#
# With a working docker (`docker info` succeeds), the steps run in a fresh `rust:1.100`
# container: a clean machine with only the Rust toolchain. This is the success-criterion
# gate. The script prints exactly one result line:
#
#   QUICKSTART_RESULT: within_budget wall=<N>s   (exit 0, N <= 600)
#   QUICKSTART_RESULT: over_budget wall=<N>s     (exit 1, N > 600)
#   QUICKSTART_RESULT: failed wall=<N>s          (exit 1, a step failed)
#
# Without docker, the same steps run locally on the fresh clone with an empty target
# directory (a cold build, but with this machine's cargo registry cache and toolchain).
# That time is printed as the "local cold-clone equivalent (NOT the clean-machine gate)"
# and the script ends with `HUMAN_NEEDED: run with docker on a clean machine` (exit 0),
# or with `QUICKSTART_RESULT: failed ...` (exit 1) when a step failed.
#
# Usage: examples/order-saga/quickstart.sh [<local repository path>]
# Env:   OSTRO_QUICKSTART_MODE=docker|local  force a mode (default: docker if usable)
#        OSTRO_QUICKSTART_IMAGE              container image (default rust:1.100)
#        OSTRO_QUICKSTART_PORT               port for `ostro dev` (default 7700)
#        OSTRO_QUICKSTART_KEEP=1             keep the temporary directory
set -euo pipefail

BUDGET_S=600
IMAGE="${OSTRO_QUICKSTART_IMAGE:-rust:1.100}"
PORT="${OSTRO_QUICKSTART_PORT:-7700}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
src="${1:-$(git -C "$here" rev-parse --show-toplevel)}"
case "$src" in
  *://* | *@*:*)
    echo "quickstart: the source must be a local path, not a URL: $src" >&2
    exit 2
    ;;
esac
if [[ ! -d "$src" ]]; then
  echo "quickstart: not a directory: $src" >&2
  exit 2
fi
src="$(cd "$src" && pwd)"

docker_usable() {
  command -v docker >/dev/null 2>&1 && timeout 30 docker info >/dev/null 2>&1
}

mode="${OSTRO_QUICKSTART_MODE:-}"
if [[ -z "$mode" ]]; then
  if docker_usable; then mode=docker; else mode=local; fi
fi
case "$mode" in
  docker | local) ;;
  *)
    echo "quickstart: OSTRO_QUICKSTART_MODE must be docker or local, not '$mode'" >&2
    exit 2
    ;;
esac

work="$(mktemp -d "${TMPDIR:-/tmp}/ostro-quickstart.XXXXXX")"
cleanup() {
  if [[ "${OSTRO_QUICKSTART_KEEP:-0}" == 1 ]]; then
    echo "quickstart: kept $work"
    return
  fi
  if [[ "$mode" == docker && -d "$work/ostro" ]]; then
    # Files written in the container belong to root.
    docker run --rm -v "$work:/w" "$IMAGE" rm -rf /w/ostro >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# The README steps, verbatim, as one script. Runs from the repository root of the clone.
cat >"$work/steps.sh" <<'STEPS'
set -euo pipefail
PORT="${PORT:-7700}"
t0=$(date +%s)
phase() { echo "== [$(( $(date +%s) - t0 ))s] $*"; }

phase "rustup target add wasm32-wasip2"
rustup target add wasm32-wasip2

phase "cargo build --release --locked -p ostro"
cargo build --release --locked -p ostro
export PATH="$PWD/target/release:$PATH"

phase "cargo build --release --target wasm32-wasip2 (examples/order-saga)"
cd examples/order-saga
cargo build --release --target wasm32-wasip2

phase "ostro dev &"
export OSTRO_SERVER="http://127.0.0.1:$PORT"
ostro dev --listen "127.0.0.1:$PORT" >ostro-dev.log 2>&1 &
dev_pid=$!
trap 'kill "$dev_pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 300); do
  if ostro ls >/dev/null 2>&1; then break; fi
  sleep 0.2
done
ostro ls >/dev/null || { cat ostro-dev.log; exit 1; }

phase "ostro deploy target/wasm32-wasip2/release/order_saga.wasm --activate"
ostro deploy target/wasm32-wasip2/release/order_saga.wasm --activate

phase "ostro run order-saga --input ..."
started="$(ostro run order-saga --input '{"order_id":"o-1","amount":4999,"sku":"BOOK-42","qty":1}')"
echo "$started"
run="$(echo "$started" | awk '/^started run /{print $3}')"
[[ -n "$run" ]] || { echo "no run id in: $started"; exit 1; }

phase "ostro signal $run approve --payload true"
ostro signal "$run" approve --payload true

phase "ostro result $run --wait"
result="$(ostro result "$run" --wait)"
echo "$result"
if [[ "$result" != "run $run completed: "* ]] \
  || ! grep -q '"status":"completed"' <<<"$result" \
  || ! grep -q '"shipment_id":"ship-o-1"' <<<"$result"; then
  echo "unexpected result: $result"
  exit 1
fi
phase "quickstart done"
STEPS

start=$(date +%s)
echo "quickstart: mode=$mode source=$src work=$work"
git -c advice.detachedHead=false clone --quiet --no-hardlinks "file://$src" "$work/ostro"
echo "quickstart: cloned $(git -C "$work/ostro" rev-parse --short HEAD)"

set +e
if [[ "$mode" == docker ]]; then
  docker run --rm \
    -e PORT="$PORT" \
    -v "$work/ostro:/src" \
    -v "$work/steps.sh:/steps.sh:ro" \
    -w /src \
    "$IMAGE" bash /steps.sh
  rc=$?
else
  (
    cd "$work/ostro"
    # A cold build: no shared target dir, no compiler wrapper, no inherited server.
    unset CARGO_TARGET_DIR CARGO_BUILD_TARGET RUSTC_WRAPPER RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
    unset OSTRO_SERVER
    PORT="$PORT" bash "$work/steps.sh"
  )
  rc=$?
fi
set -e
wall=$(( $(date +%s) - start ))

if [[ $rc -ne 0 ]]; then
  echo "quickstart: a step failed (exit $rc) after ${wall}s"
  echo "QUICKSTART_RESULT: failed wall=${wall}s"
  exit 1
fi

if [[ "$mode" == docker ]]; then
  echo "quickstart: clean-machine wall time (docker $IMAGE): ${wall}s (budget ${BUDGET_S}s)"
  if (( wall > BUDGET_S )); then
    echo "QUICKSTART_RESULT: over_budget wall=${wall}s"
    exit 1
  fi
  echo "QUICKSTART_RESULT: within_budget wall=${wall}s"
  exit 0
fi

echo "quickstart: local cold-clone equivalent (NOT the clean-machine gate): wall=${wall}s (budget ${BUDGET_S}s)"
echo "HUMAN_NEEDED: run with docker on a clean machine (examples/order-saga/quickstart.sh with a working docker; gate: QUICKSTART_RESULT: within_budget)"
exit 0

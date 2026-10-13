#!/usr/bin/env bash
# Local equivalent of .github/workflows/ci.yml (D-02). Run before every push.
#
# Usage: scripts/ci-local.sh [--integration] [MODULE ...]
#   MODULE: root (alias "."), invoker, json, nats, jetstream, redis,
#           telemetry, prometheus, examples. No MODULE = all 9 + workspace check.
#   --integration: also run the JetStream integration tests against nats:2.15.1
#           (uses $NATS_URL as-is when already set, otherwise starts docker;
#           override host ports with NATS_HOST_PORT / NATS_MON_PORT, default 4222 / 8222).
#
# Never touches ./go.work or ./go.work.sum, never pushes, never edits tracked files.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
ROOT="$PWD"

ALL_MODULES=(. invoker json nats jetstream redis telemetry prometheus examples)
INTEGRATION=0
SELECTED=()

for arg in "$@"; do
  case "$arg" in
    --integration) INTEGRATION=1 ;;
    root | .) SELECTED+=(.) ;;
    invoker | json | nats | jetstream | redis | telemetry | prometheus | examples) SELECTED+=("$arg") ;;
    *)
      echo "unknown argument: $arg" >&2
      echo "usage: scripts/ci-local.sh [--integration] [MODULE ...]" >&2
      exit 2
      ;;
  esac
done

RUN_WORKSPACE=0
if [ ${#SELECTED[@]} -eq 0 ]; then
  SELECTED=("${ALL_MODULES[@]}")
  RUN_WORKSPACE=1
fi

if ! golangci-lint version 2>/dev/null | grep -q 'version 2\.'; then
  echo "golangci-lint v2 is required on PATH (CI uses v2.14)." >&2
  echo "Download: https://github.com/golangci/golangci-lint/releases/download/v2.14.0/golangci-lint-2.14.0-linux-amd64.tar.gz" >&2
  echo "Verify the SHA256 against golangci-lint-2.14.0-checksums.txt from the same release." >&2
  exit 2
fi

export GOWORK=off

label() {
  if [ "$1" = "." ]; then echo root; else echo "$1"; fi
}

fail() {
  echo "FAIL $1: $2" >&2
  exit 1
}

run_module() {
  local mod="$1" name
  name="$(label "$mod")"
  (
    cd "$ROOT/$mod"
    grep -qx 'go 1.26.0' go.mod || fail "$name" "directive (go.mod must declare exactly 'go 1.26.0')"
    if grep -q '^toolchain ' go.mod; then fail "$name" "directive (toolchain line present)"; fi
    go mod tidy -diff || fail "$name" "tidy"
    go mod verify || fail "$name" "verify"
    go build ./... || fail "$name" "build"
    go vet ./... || fail "$name" "vet"
    if [ "$mod" = "jetstream" ]; then
      go vet -tags integration ./... || fail "$name" "vet-integration"
    fi
    if [ "$mod" != "examples" ]; then
      go test -race -count=1 ./... || fail "$name" "test"
    fi
    golangci-lint run ./... || fail "$name" "lint"
  )
  echo "PASS $name"
}

for mod in "${SELECTED[@]}"; do
  run_module "$mod"
done

if [ "$RUN_WORKSPACE" -eq 1 ]; then
  TMPDIR_WS="$(mktemp -d)"
  trap 'rm -rf "$TMPDIR_WS"' EXIT
  (
    export GOWORK="$TMPDIR_WS/ci.work"
    go work init "${ALL_MODULES[@]}" || fail workspace "init"
    # shellcheck disable=SC2046
    go build $(go list -m -f '{{.Path}}/...') || fail workspace "build"
    # shellcheck disable=SC2046
    go test -race -count=1 $(go list -m -f '{{.Path}}/...') || fail workspace "test"
  )
  echo "PASS workspace"
fi

if [ "$INTEGRATION" -eq 1 ]; then
  if [ -z "${NATS_URL:-}" ]; then
    # Host ports default to the CI values; override when 4222/8222 are taken.
    NATS_HOST_PORT="${NATS_HOST_PORT:-4222}"
    NATS_MON_PORT="${NATS_MON_PORT:-8222}"
    docker run -d --rm --name nats-ci-local -p "${NATS_HOST_PORT}:4222" -p "${NATS_MON_PORT}:8222" nats:2.15.1 -js -m 8222 >/dev/null
    trap 'docker stop nats-ci-local >/dev/null 2>&1 || true; rm -rf "${TMPDIR_WS:-}"' EXIT
    ready=0
    for _ in $(seq 1 30); do
      if curl -sf "http://localhost:${NATS_MON_PORT}/healthz?js-enabled-only=true" >/dev/null; then
        ready=1
        break
      fi
      sleep 1
    done
    if [ "$ready" -ne 1 ]; then
      docker logs nats-ci-local || true
      fail integration "nats-server not healthy"
    fi
    export NATS_URL="nats://localhost:${NATS_HOST_PORT}"
  fi
  (
    cd "$ROOT/jetstream"
    go test -tags integration -race -count=1 ./... || fail integration "test"
  )
  echo "PASS integration"
fi

if [ -n "$(git ls-files CLAUDE.md .planning)" ]; then
  echo "FAIL: never commit CLAUDE.md or .planning" >&2
  exit 1
fi

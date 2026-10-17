#!/usr/bin/env bash
# Local equivalent of .github/workflows/ci.yml (D-02). Run before every push.
#
# Usage: scripts/ci-local.sh [--integration] [--workspace] [MODULE ...]
#   MODULE: root (alias "."), invoker, json, nats, jetstream, redis,
#           telemetry, prometheus, examples. No MODULE = all 9 + workspace check.
#   --workspace: verify the selected modules (default all 9) against the local
#           root through a temporary go.work over all 9 modules (D-24). Needs no
#           golangci-lint (lint runs only when golangci-lint v2 is on PATH). This
#           is the pre-release signal: the per-module GOWORK=off legs of modules that
#           use root API that is not published yet are red until the release (for
#           v0.4.0: invoker, json, nats, jetstream, telemetry, prometheus, examples).
#           The directive, toolchain, tidy and verify guards still run with GOWORK=off.
#   --integration: also run the JetStream integration tests against nats:2.15.1
#           (uses $NATS_URL as-is when already set, otherwise starts docker;
#           override host ports with NATS_HOST_PORT / NATS_MON_PORT, default 4222 / 8222).
#           They always run over the temporary go.work of all 9 modules, like the CI
#           integration job, so they test the tree and not the published modules.
#
# Never touches ./go.work or ./go.work.sum, never pushes, never edits tracked files.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
ROOT="$PWD"

ALL_MODULES=(. invoker json nats jetstream redis telemetry prometheus examples)
INTEGRATION=0
WORKSPACE_MODE=0
SELECTED=()

for arg in "$@"; do
  case "$arg" in
    --integration) INTEGRATION=1 ;;
    --workspace) WORKSPACE_MODE=1 ;;
    root | .) SELECTED+=(.) ;;
    invoker | json | nats | jetstream | redis | telemetry | prometheus | examples) SELECTED+=("$arg") ;;
    *)
      echo "unknown argument: $arg" >&2
      echo "usage: scripts/ci-local.sh [--integration] [--workspace] [MODULE ...]" >&2
      exit 2
      ;;
  esac
done

RUN_WORKSPACE=0
if [ ${#SELECTED[@]} -eq 0 ]; then
  SELECTED=("${ALL_MODULES[@]}")
  RUN_WORKSPACE=1
fi

HAVE_LINT=0
if golangci-lint version 2>/dev/null | grep -q 'version 2\.'; then
  HAVE_LINT=1
fi
if [ "$HAVE_LINT" -ne 1 ] && [ "$WORKSPACE_MODE" -ne 1 ]; then
  echo "golangci-lint v2 is required on PATH (CI uses v2.14)." >&2
  echo "Download: https://github.com/golangci/golangci-lint/releases/download/v2.14.0/golangci-lint-2.14.0-linux-amd64.tar.gz" >&2
  echo "Verify the SHA256 against golangci-lint-2.14.0-checksums.txt from the same release." >&2
  exit 2
fi

export GOWORK=off

# One cleanup for every exit path: removes the temporary workspace directory and
# stops the NATS container only when this run started it.
TMPDIR_WS=""
WS_FILE=""
STARTED_NATS=0
cleanup() {
  if [ "$STARTED_NATS" -eq 1 ]; then
    docker stop nats-ci-local >/dev/null 2>&1 || true
  fi
  if [ -n "$TMPDIR_WS" ]; then
    rm -rf "$TMPDIR_WS"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

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
    go test -race -count=1 ./... || fail "$name" "test"
    golangci-lint run ./... || fail "$name" "lint"
  )
  echo "PASS $name"
}

# Temporary workspace over all 9 modules, always inside a mktemp directory.
# Never the repository go.work.
make_workspace() {
  [ -z "$TMPDIR_WS" ] || return 0
  TMPDIR_WS="$(mktemp -d)"
  WS_FILE="$TMPDIR_WS/ci.work"
  (
    export GOWORK="$WS_FILE"
    go work init "${ALL_MODULES[@]}" || fail workspace "init"
  )
}

# Workspace mode (D-24): guards run with GOWORK=off, build/vet/test run against
# the local root through the temporary go.work.
run_module_workspace() {
  local mod="$1" name
  name="$(label "$mod")"
  (
    cd "$ROOT/$mod"
    grep -qx 'go 1.26.0' go.mod || fail "$name" "directive (go.mod must declare exactly 'go 1.26.0')"
    if grep -q '^toolchain ' go.mod; then fail "$name" "directive (toolchain line present)"; fi
    go mod tidy -diff || fail "$name" "tidy"
    go mod verify || fail "$name" "verify"
    export GOWORK="$WS_FILE"
    go build ./... || fail "$name" "build"
    go vet ./... || fail "$name" "vet"
    if [ "$mod" = "jetstream" ]; then
      go vet -tags integration ./... || fail "$name" "vet-integration"
    fi
    go test -race -count=1 ./... || fail "$name" "test"
    if [ "$HAVE_LINT" -eq 1 ]; then
      golangci-lint run ./... || fail "$name" "lint"
    else
      echo "SKIP lint $name (golangci-lint v2 not on PATH)"
    fi
  )
  echo "PASS $name (workspace)"
}

if [ "$WORKSPACE_MODE" -eq 1 ]; then
  make_workspace
  declare -A SEEN=()
  for mod in "${SELECTED[@]}"; do
    SEEN["$mod"]=1
    run_module_workspace "$mod"
  done
  if [ "${#SEEN[@]}" -eq "${#ALL_MODULES[@]}" ]; then
    echo "PASS workspace"
  fi
else
  for mod in "${SELECTED[@]}"; do
    run_module "$mod"
  done
fi

if [ "$RUN_WORKSPACE" -eq 1 ] && [ "$WORKSPACE_MODE" -ne 1 ]; then
  make_workspace
  (
    export GOWORK="$WS_FILE"
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
    STARTED_NATS=1
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
  make_workspace
  (
    cd "$ROOT/jetstream"
    export GOWORK="$WS_FILE"
    go test -tags integration -race -count=1 ./... || fail integration "test"
  )
  echo "PASS integration"
fi

if [ -n "$(git ls-files CLAUDE.md .planning)" ]; then
  echo "FAIL: never commit CLAUDE.md or .planning" >&2
  exit 1
fi

#!/usr/bin/env bash
# Consumer-view release verification (D-20, D-27).
#
# Usage: scripts/verify-release.sh [--wait MINUTES] [--ref GIT_REF] [--no-probe] VERSION [MODULE ...]
#
#   VERSION   vMAJOR.MINOR.PATCH, e.g. v0.3.0
#   MODULE    root, invoker, json, nats, jetstream, redis, telemetry, prometheus, examples
#             (default: all 9)
#
#   --wait MINUTES  Poll the module proxy (backoff 5s, doubling, capped at 60s) until each
#                   module@VERSION resolves or MINUTES elapse. Use ONLY after the tag has been
#                   pushed: an early request can be cached as a miss for up to 30 minutes.
#                   Normal mode only (rejected together with --ref).
#   --ref GIT_REF   Pre-publication check of the exact tagged tree. The module is extracted from
#                   `git archive GIT_REF` and wired in with a directory replace, so VERSION is never
#                   requested from the proxy; its in-repo dependencies still resolve from the proxy,
#                   which proves that the tagged go.mod requires published versions.
#   --no-probe      Skip the behavioral probe for root (NewLocalBus with a nil Invoker, two-value
#                   EmitSync).
#
# Each module is checked in its own fresh temp module under GOWORK=off with the default GOPROXY and
# GOSUMDB (what `go get` users receive). Direct VCS fetching is never used: the moved root v0.1.0
# tag makes direct fetches fail with SECURITY ERROR. The script never modifies the repository and never
# pushes anything.
#
# Exit 0 only if every requested module passed.
set -euo pipefail

ROOT_PATH="github.com/isaquesb/go-event-bus"
ALL_MODULES=(root invoker json nats jetstream redis telemetry prometheus examples)

usage() {
  sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//' >&2
  exit 2
}

die() {
  echo "error: $*" >&2
  exit 2
}

WAIT_MIN=0
REF=""
PROBE=1
while [ $# -gt 0 ]; do
  case "$1" in
    --wait)
      [ $# -ge 2 ] || die "--wait needs MINUTES"
      WAIT_MIN="$2"
      shift 2
      ;;
    --ref)
      [ $# -ge 2 ] || die "--ref needs GIT_REF"
      REF="$2"
      shift 2
      ;;
    --no-probe)
      PROBE=0
      shift
      ;;
    -h | --help) usage ;;
    --*) die "unknown option: $1" ;;
    *) break ;;
  esac
done

[ $# -ge 1 ] || usage
VERSION="$1"
shift
[[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "VERSION must look like vMAJOR.MINOR.PATCH, got: $VERSION"
[[ "$WAIT_MIN" =~ ^[0-9]+$ ]] || die "--wait MINUTES must be a non-negative integer"
if [ -n "$REF" ] && [ "$WAIT_MIN" -gt 0 ]; then
  die "--wait is for post-publication checks and cannot be combined with --ref"
fi

MODULES=()
if [ $# -eq 0 ]; then
  MODULES=("${ALL_MODULES[@]}")
else
  for m in "$@"; do
    case "$m" in
      root | invoker | json | nats | jetstream | redis | telemetry | prometheus | examples) MODULES+=("$m") ;;
      *) die "unknown module: $m (expected one of: ${ALL_MODULES[*]})" ;;
    esac
  done
fi

module_path() {
  if [ "$1" = root ]; then echo "$ROOT_PATH"; else echo "$ROOT_PATH/$1"; fi
}

module_dir() {
  if [ "$1" = root ]; then echo ""; else echo "$1"; fi
}

export GOWORK=off
export GOFLAGS=-mod=mod

# --- environment: the check must observe the public proxy and sumdb ----------------------------
# An explicit GOPROXY=off together with --ref is the one allowed exception (offline check of a
# dependency-free module); remember it before anything else looks at the variable.
OFFLINE_REF_OK=0
if [ -n "$REF" ] && [ "${GOPROXY:-}" = "off" ]; then
  OFFLINE_REF_OK=1
fi

echo "== environment"
for v in GOPROXY GOSUMDB GOPRIVATE GONOPROXY GONOSUMDB; do
  printf '%s=%s\n' "$v" "$(go env "$v")"
done

env_ok=1
GOSUMDB_VAL="$(go env GOSUMDB)"
if [ "$GOSUMDB_VAL" = "off" ]; then
  echo "FAIL environment: GOSUMDB=off hides what users get" >&2
  env_ok=0
fi

GOPROXY_VAL="$(go env GOPROXY)"
GOPROXY_FIRST="${GOPROXY_VAL%%[,|]*}"
if [ "$OFFLINE_REF_OK" -eq 1 ]; then
  echo "note: GOPROXY=off with --ref: offline pre-publication check, the replaced module is never fetched"
elif [ "$GOPROXY_FIRST" = "direct" ] || [ "$GOPROXY_FIRST" = "off" ]; then
  echo "FAIL environment: GOPROXY starts with '$GOPROXY_FIRST'; the default proxy is the real user path (D-27)" >&2
  env_ok=0
fi

# Fail when any comma-separated pattern in GOPRIVATE/GONOPROXY/GONOSUMDB matches the module path or
# one of its leading path prefixes (then the check would bypass the proxy or sumdb for it).
bypass_match() {
  local path="$1" var val pat prefix
  local -a pats
  for var in GOPRIVATE GONOPROXY GONOSUMDB; do
    val="$(go env "$var")"
    [ -n "$val" ] || continue
    IFS=',' read -r -a pats <<<"$val"
    for pat in "${pats[@]}"; do
      [ -n "$pat" ] || continue
      prefix="$path"
      while :; do
        # shellcheck disable=SC2254
        case "$prefix" in
          $pat)
            echo "$var pattern '$pat' matches $prefix"
            return 0
            ;;
        esac
        [[ "$prefix" == */* ]] || break
        prefix="${prefix%/*}"
      done
    done
  done
  return 1
}

if [ "$OFFLINE_REF_OK" -ne 1 ]; then
  for m in "${MODULES[@]}"; do
    if msg="$(bypass_match "$(module_path "$m")")"; then
      echo "FAIL environment: $msg; the proxy/sumdb would be bypassed" >&2
      env_ok=0
    fi
  done
fi

[ "$env_ok" -eq 1 ] || exit 1

# --- temp area ---------------------------------------------------------------------------------
WORKROOT="$(mktemp -d)"
cleanup() {
  # Module cache files are read-only; make the temp area writable before removal.
  chmod -R u+w "$WORKROOT" 2>/dev/null || true
  rm -rf "$WORKROOT"
}
trap cleanup EXIT

EXTRACT=""
if [ -n "$REF" ]; then
  REPO_TOP="$(git rev-parse --show-toplevel)" || die "--ref needs to run inside the repository"
  REF_SHA="$(git -C "$REPO_TOP" rev-parse --verify "${REF}^{commit}")" || die "unknown git ref: $REF"
  EXTRACT="$WORKROOT/tree"
  mkdir -p "$EXTRACT"
  git -C "$REPO_TOP" archive "$REF_SHA" | tar -x -C "$EXTRACT"
  echo "note: --ref $REF resolved to $REF_SHA and extracted to a temp tree"
fi

# --- helpers -----------------------------------------------------------------------------------
wait_for_module() {
  local path="$1" start deadline delay=5 now tmp
  start="$(date +%s)"
  deadline=$((start + WAIT_MIN * 60))
  tmp="$(mktemp -d "$WORKROOT/poll.XXXXXX")"
  (cd "$tmp" && go mod init verify.local >/dev/null 2>&1)
  while :; do
    if (cd "$tmp" && go list -m -json "${path}@${VERSION}" >/dev/null 2>&1); then
      now="$(date +%s)"
      echo "  ${path}@${VERSION} resolved on the proxy after $((now - start))s"
      return 0
    fi
    now="$(date +%s)"
    if [ "$now" -ge "$deadline" ]; then
      echo "  ${path}@${VERSION} not resolvable after $((now - start))s" >&2
      return 1
    fi
    sleep "$delay"
    delay=$((delay * 2))
    [ "$delay" -le 60 ] || delay=60
  done
}

write_blank_import() {
  printf 'package main\n\nimport _ "%s"\n\nfunc main() {}\n' "$1" >main.go
}

write_probe() {
  cat >main.go <<'EOF'
package main

import (
	"context"
	"fmt"
	"os"

	event "github.com/isaquesb/go-event-bus"
)

type probeEvt struct{}

func (probeEvt) Name() string { return "probe.evt" }

func main() {
	// nil Invoker on purpose: releases without the passthrough default panic here.
	bus := event.NewLocalBus(event.LocalBusOptions{})
	ctx := context.Background()
	called := false
	err := bus.Subscribe(ctx, "probe.evt", func(context.Context, event.Event) error {
		called = true
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "subscribe:", err)
		os.Exit(1)
	}
	// Two-value EmitSync: releases before the SyncBus alignment do not compile.
	got, err := bus.EmitSync(ctx, probeEvt{})
	if err != nil || got == nil {
		fmt.Fprintln(os.Stderr, "EmitSync returned", got, err)
		os.Exit(1)
	}
	if !called {
		fmt.Fprintln(os.Stderr, "handler was not invoked")
		os.Exit(1)
	}
	fmt.Println("probe ok")
}
EOF
}

# check_module NAME: prints PASS/FAIL, exit status 0 only on PASS. Runs in a subshell.
check_module() (
  local name="$1" path dir tmp log reasons="" stale listing
  path="$(module_path "$name")"
  dir="$(module_dir "$name")"
  tmp="$(mktemp -d "$WORKROOT/mod.XXXXXX")"
  log="$tmp.log"
  : >"$log"
  cd "$tmp"

  finish_fail() {
    echo "FAIL ${path}@${VERSION}:${reasons}"
    if [ -s "$log" ]; then tail -n 20 "$log" | sed 's/^/    | /'; fi
    exit 1
  }

  if ! go mod init verify.local >>"$log" 2>&1; then
    reasons="$reasons go mod init failed;"
    finish_fail
  fi
  write_blank_import "$path"

  if [ -n "$REF" ]; then
    if ! go mod edit -require="${path}@${VERSION}" -replace="${path}=${EXTRACT}${dir:+/$dir}" >>"$log" 2>&1; then
      reasons="$reasons go mod edit (require/replace) failed;"
      finish_fail
    fi
  else
    if ! go get "${path}@${VERSION}" >>"$log" 2>&1; then
      reasons="$reasons go get ${path}@${VERSION} failed;"
      finish_fail
    fi
  fi

  if ! go mod tidy >>"$log" 2>&1; then
    reasons="$reasons go mod tidy failed;"
  fi
  if ! go build ./... >>"$log" 2>&1; then
    reasons="$reasons go build failed;"
  fi

  # Every in-repo module in the build list must be at exactly VERSION.
  if listing="$(go list -m all 2>>"$log")"; then
    stale="$(printf '%s\n' "$listing" | awk -v m="$ROOT_PATH" -v v="$VERSION" 'index($1, m) == 1 && $2 != v { print "    stale: " $1 " " $2 }')"
    if [ -n "$stale" ]; then
      reasons="$reasons stale in-repo versions in the build list (want $VERSION);"
      printf '%s\n' "$stale" >>"$log.stale"
    fi
  else
    reasons="$reasons go list -m all failed;"
  fi

  if [ "$name" = root ] && [ "$PROBE" -eq 1 ]; then
    write_probe
    if ! timeout 180 go run . >>"$log" 2>&1; then
      reasons="$reasons behavioral probe failed (NewLocalBus nil Invoker / two-value EmitSync);"
    fi
  fi

  if [ -n "$reasons" ]; then
    echo "FAIL ${path}@${VERSION}:${reasons}"
    if [ -s "$log.stale" ]; then cat "$log.stale"; fi
    if [ -s "$log" ]; then tail -n 20 "$log" | sed 's/^/    | /'; fi
    exit 1
  fi
  echo "PASS ${path}@${VERSION}"
)

# --- main loop ---------------------------------------------------------------------------------
echo "== verifying ${VERSION}${REF:+ (tree $REF)}: ${MODULES[*]}"
rc=0
for m in "${MODULES[@]}"; do
  path="$(module_path "$m")"
  if [ "$WAIT_MIN" -gt 0 ]; then
    if ! wait_for_module "$path"; then
      echo "FAIL ${path}@${VERSION}: not visible on the proxy within ${WAIT_MIN} minutes"
      rc=1
      continue
    fi
  fi
  if ! check_module "$m"; then
    rc=1
  fi
done

if [ "$rc" -eq 0 ]; then
  echo "== all ${#MODULES[@]} requested module(s) passed"
else
  echo "== verification FAILED" >&2
fi
exit "$rc"

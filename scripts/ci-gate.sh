#!/usr/bin/env bash
# CI gate for the merge and every release layer (D-21, D-28). Read-only: it only reads the ci.yml
# run of a commit through gh and never re-runs, cancels or edits anything.
#
# Usage: scripts/ci-gate.sh --sha SHA --require "MODULES" --allow-red "MODULES"
#                           [--event push|pull_request] [--wait] [--report FILE]
#        scripts/ci-gate.sh --self-test
#   --sha SHA        commit whose ci.yml run is judged (the PR head for --event pull_request)
#   --event EVENT    push (default) or pull_request
#   --require        modules whose test and lint legs must be success
#   --allow-red      modules whose legs may be red, but only with compile-error evidence (below)
#                    Every module (root invoker json nats jetstream redis telemetry prometheus
#                    examples) must be in exactly one of the two lists, otherwise exit 2.
#   --wait           poll until the run completes (deadline 45 minutes, backoff 15s to 60s)
#   --report FILE    also write the verdict lines to FILE
#   CI_GATE_GH       gh binary to use (default gh); CI_GATE_SLEEP replaces sleep (tests)
#
# Rule (D-21/D-28): the jobs `workspace` and `integration (nats-server)`, the test/lint legs of
# the required modules and every job not named above must be success. A leg of an allow-red
# module may be success, or failure when the log of its failed step shows a Go compile or
# type-check error (undefined symbol, unknown field, wrong argument count, ...) and no
# "--- FAIL:" or "panic:" line. Anything else fails.
#
# One verdict line per job: "OK <job>", "ALLOWED-RED <job> <evidence>" or "FAIL <job> <conclusion>
# <reason>"; then "PASS ci-gate" (exit 0) or "ci-gate: N failure(s)" (exit 1). Needs gh and jq.
set -euo pipefail

GH="${CI_GATE_GH:-gh}"
SLEEP="${CI_GATE_SLEEP:-sleep}"
ALL_MODULES=(root invoker json nats jetstream redis telemetry prometheus examples)
COMPILE_RE='undefined: [A-Za-z_]+\.|unknown field|not enough arguments in call|too many arguments in call|has no field or method|cannot use .* as .* value'
DEADLINE_SEC=2700

SHA=""
EVENT="push"
REQUIRE=""
ALLOW_RED=""
WAIT=0
REPORT=""
SELF_TEST=0
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

usage_error() {
  echo "ci-gate: $1" >&2
  echo "usage: scripts/ci-gate.sh --sha SHA --require \"MODULES\" --allow-red \"MODULES\" [--event push|pull_request] [--wait] [--report FILE] | --self-test" >&2
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --sha) SHA="${2:-}"; shift 2 || usage_error "--sha needs a value" ;;
    --event) EVENT="${2:-}"; shift 2 || usage_error "--event needs a value" ;;
    --require) REQUIRE="${2:-}"; shift 2 || usage_error "--require needs a value" ;;
    --allow-red) ALLOW_RED="${2:-}"; shift 2 || usage_error "--allow-red needs a value" ;;
    --report) REPORT="${2:-}"; shift 2 || usage_error "--report needs a value" ;;
    --wait) WAIT=1; shift ;;
    --self-test) SELF_TEST=1; shift ;;
    -h | --help) sed -n '2,/^set -euo/p' "$SELF" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) usage_error "unknown argument: $1" ;;
  esac
done

# ---------------------------------------------------------------------------
# Self-test: a fake gh serves canned runs, jobs and failed-step logs.
# ---------------------------------------------------------------------------
JOB_NAMES=("workspace" "integration (nats-server)")
for m in "${ALL_MODULES[@]}"; do
  s="$m"
  [ "$m" != root ] || s="."
  JOB_NAMES+=("test ($s)")
done
for m in "${ALL_MODULES[@]}"; do
  s="$m"
  [ "$m" != root ] || s="."
  JOB_NAMES+=("lint ($s)")
done

st_write_fake_gh() { # file
  cat >"$1" <<'FAKE'
#!/usr/bin/env bash
# Fake gh for ci-gate.sh --self-test: answers from $FAKE_DIR.
set -euo pipefail
[ "$1" = run ] || { echo "fake gh: unexpected $*" >&2; exit 9; }
shift
sub="$1"
shift
case "$sub" in
  list)
    if [ -f "$FAKE_DIR/runs.pending.json" ] && [ ! -f "$FAKE_DIR/polled" ]; then
      : >"$FAKE_DIR/polled"
      cat "$FAKE_DIR/runs.pending.json"
    else
      cat "$FAKE_DIR/runs.json"
    fi
    ;;
  view)
    job="" logfailed=0
    while [ $# -gt 0 ]; do
      case "$1" in
        --job) job="$2"; shift 2 ;;
        --log-failed) logfailed=1; shift ;;
        *) shift ;;
      esac
    done
    if [ "$logfailed" -eq 1 ]; then
      cat "$FAKE_DIR/log.$job"
    else
      cat "$FAKE_DIR/jobs.json"
    fi
    ;;
  *) echo "fake gh: unexpected run $sub" >&2; exit 9 ;;
esac
FAKE
  chmod +x "$1"
}

st_mk_jobs() { # dir name=conclusion... (every other job is success)
  local dir="$1" i=100 n c o
  shift
  : >"$dir/jobs.tsv"
  for n in "${JOB_NAMES[@]}"; do
    c=success
    for o in "$@"; do
      if [ "${o%%=*}" = "$n" ]; then c="${o#*=}"; fi
    done
    printf '%s\t%s\t%s\n' "$i" "$n" "$c" >>"$dir/jobs.tsv"
    i=$((i + 1))
  done
  jq -Rn '[inputs | split("\t") | {databaseId: (.[0] | tonumber), name: .[1], status: "completed", conclusion: .[2]}] | {jobs: .}' \
    <"$dir/jobs.tsv" >"$dir/jobs.json"
}

st_job_id() { # dir name
  awk -F'\t' -v n="$2" '$2 == n { print $1 }' "$1/jobs.tsv"
}

st_run() { # dir args... ; sets ST_OUT ST_RC
  local dir="$1"
  shift
  ST_RC=0
  ST_OUT="$(FAKE_DIR="$dir" CI_GATE_GH="$dir/gh" CI_GATE_SLEEP=true bash "$SELF" "$@" 2>&1)" || ST_RC=$?
}

st_assert() { # label expected-rc grep-pattern...
  local label="$1" rc="$2" pat
  shift 2
  if [ "$ST_RC" -ne "$rc" ]; then
    echo "self-test FAILED: $label expected exit $rc, got $ST_RC" >&2
    printf '%s\n' "$ST_OUT" >&2
    exit 1
  fi
  for pat in "$@"; do
    if ! printf '%s\n' "$ST_OUT" | grep -qE "$pat"; then
      echo "self-test FAILED: $label output has no line matching: $pat" >&2
      printf '%s\n' "$ST_OUT" >&2
      exit 1
    fi
  done
}

self_test() {
  local tmp
  tmp="$(mktemp -d)"
  trap "rm -rf '$tmp'" EXIT
  st_write_fake_gh "$tmp/gh"
  local sha=0123456789abcdef0123456789abcdef01234567
  printf '[{"databaseId":777,"headSha":"%s","status":"completed","conclusion":"success"}]\n' "$sha" >"$tmp/runs.json"
  local all="root invoker json nats jetstream redis telemetry prometheus examples"
  local id

  # 1. All required jobs green passes with one OK line per job.
  st_mk_jobs "$tmp"
  st_run "$tmp" --sha "$sha" --require "$all" --allow-red ""
  st_assert 'all green' 0 '^OK workspace$' '^OK integration \(nats-server\)$' '^OK lint \(\.\)$' '^PASS ci-gate$'
  if [ "$(printf '%s\n' "$ST_OUT" | grep -c '^OK ')" -ne 20 ]; then
    echo "self-test FAILED: all green should print 20 OK lines" >&2
    printf '%s\n' "$ST_OUT" >&2
    exit 1
  fi

  # 2. An allow-red leg that fails with a compile error passes as ALLOWED-RED.
  st_mk_jobs "$tmp" "test (jetstream)=failure" "lint (jetstream)=success"
  id="$(st_job_id "$tmp" 'test (jetstream)')"
  printf 'test (jetstream)\tBuild\t2026-10-17T10:00:00Z ./bus.go:41:12: undefined: event.Observer\n' >"$tmp/log.$id"
  st_run "$tmp" --sha "$sha" --require "root invoker json nats redis telemetry prometheus examples" --allow-red "jetstream"
  st_assert 'compile-error red leg' 0 '^ALLOWED-RED test \(jetstream\) .*undefined: event\.Observer' '^PASS ci-gate$'

  # 3. An allow-red leg whose log has a test failure fails, even next to a compile-looking line.
  printf 'test (jetstream)\tTest\t... ./bus.go:41:12: undefined: event.Observer\ntest (jetstream)\tTest\t--- FAIL: TestX (0.01s)\n' >"$tmp/log.$id"
  st_run "$tmp" --sha "$sha" --require "root invoker json nats redis telemetry prometheus examples" --allow-red "jetstream"
  st_assert 'test-failure red leg' 1 '^FAIL test \(jetstream\) failure' 'ci-gate: 1 failure'
  printf 'test (jetstream)\tTest\tpanic: boom\ntest (jetstream)\tTest\t./x.go:1:1: unknown field Foo in struct literal\n' >"$tmp/log.$id"
  st_run "$tmp" --sha "$sha" --require "root invoker json nats redis telemetry prometheus examples" --allow-red "jetstream"
  st_assert 'panic red leg' 1 '^FAIL test \(jetstream\) failure'
  printf 'test (jetstream)\tTest\tsome unrelated error\n' >"$tmp/log.$id"
  st_run "$tmp" --sha "$sha" --require "root invoker json nats redis telemetry prometheus examples" --allow-red "jetstream"
  st_assert 'red leg without evidence' 1 '^FAIL test \(jetstream\) failure'

  # 4. A red required leg fails, even with a compile error in its log.
  st_mk_jobs "$tmp" "test (redis)=failure"
  id="$(st_job_id "$tmp" 'test (redis)')"
  printf 'test (redis)\tBuild\t./x.go:1:1: undefined: event.Observer\n' >"$tmp/log.$id"
  st_run "$tmp" --sha "$sha" --require "root invoker json nats redis telemetry prometheus examples" --allow-red "jetstream"
  st_assert 'red required leg' 1 '^FAIL test \(redis\) failure'

  # 5. A red workspace job fails, also when every module is allowed to be red.
  st_mk_jobs "$tmp" "workspace=failure"
  st_run "$tmp" --sha "$sha" --require "root" --allow-red "invoker json nats jetstream redis telemetry prometheus examples"
  st_assert 'red workspace' 1 '^FAIL workspace failure' 'ci-gate: 1 failure'

  # 6. No run for the commit, and a usage error.
  st_mk_jobs "$tmp"
  st_run "$tmp" --sha ffffffffffffffffffffffffffffffffffffffff --require "$all" --allow-red ""
  st_assert 'no run' 1 '^FAIL no CI run for SHA'
  st_run "$tmp" --sha "$sha" --require "root" --allow-red ""
  st_assert 'module in neither list' 2 'neither'

  # 7. --wait polls until the run completes.
  printf '[{"databaseId":777,"headSha":"%s","status":"in_progress","conclusion":""}]\n' "$sha" >"$tmp/runs.pending.json"
  rm -f "$tmp/polled"
  st_run "$tmp" --sha "$sha" --require "$all" --allow-red "" --wait
  st_assert 'wait' 0 '^PASS ci-gate$'
  rm -f "$tmp/runs.pending.json"

  echo 'PASS self-test'
}

if [ "$SELF_TEST" -eq 1 ]; then
  self_test
  exit 0
fi

# ---------------------------------------------------------------------------
# Gate
# ---------------------------------------------------------------------------
[ -n "$SHA" ] || usage_error "--sha is required"
case "$EVENT" in push | pull_request) ;; *) usage_error "--event must be push or pull_request" ;; esac
command -v jq >/dev/null 2>&1 || usage_error "jq is required"

declare -A REQ=() RED=()
valid_module() {
  local m
  for m in "${ALL_MODULES[@]}"; do [ "$m" = "$1" ] && return 0; done
  return 1
}
for m in $REQUIRE; do
  valid_module "$m" || usage_error "unknown module in --require: $m"
  REQ["$m"]=1
done
for m in $ALLOW_RED; do
  valid_module "$m" || usage_error "unknown module in --allow-red: $m"
  [ -z "${REQ[$m]:-}" ] || usage_error "module $m is in both --require and --allow-red"
  RED["$m"]=1
done
for m in "${ALL_MODULES[@]}"; do
  [ -n "${REQ[$m]:-}${RED[$m]:-}" ] || usage_error "module $m is in neither --require nor --allow-red"
done

if [ -n "$REPORT" ]; then : >"$REPORT"; fi
FAILURES=0
verdict() {
  printf '%s\n' "$*"
  if [ -n "$REPORT" ]; then printf '%s\n' "$*" >>"$REPORT"; fi
}
bad() { # job conclusion reason
  FAILURES=$((FAILURES + 1))
  verdict "FAIL $1 $2 $3"
}

# Find the newest ci.yml run of SHA for EVENT; with --wait, poll until it completed.
RUN_ID=""
RUN_STATUS=""
now() { date +%s; }
deadline=$(($(now) + DEADLINE_SEC))
delay=15
GH_ERR=""
while true; do
  GH_ERR=""
  runs_rc=0
  runs_json="$("$GH" run list --workflow ci.yml --event "$EVENT" --json databaseId,headSha,status,conclusion --limit 50 2>&1)" || runs_rc=$?
  if [ "$runs_rc" -eq 0 ]; then
    line="$(printf '%s' "$runs_json" | jq -r --arg sha "$SHA" '[.[] | select(.headSha == $sha)][0] // empty | [.databaseId, .status] | @tsv')"
    if [ -n "$line" ]; then
      RUN_ID="${line%%$'\t'*}"
      RUN_STATUS="${line#*$'\t'}"
      [ "$RUN_STATUS" != completed ] || break
    fi
  else
    GH_ERR="$runs_json"
  fi
  [ "$WAIT" -eq 1 ] || break
  [ "$(now)" -lt "$deadline" ] || break
  "$SLEEP" "$delay"
  delay=$((delay * 2))
  [ "$delay" -le 60 ] || delay=60
done

if [ -z "$RUN_ID" ]; then
  if [ -n "$GH_ERR" ]; then
    bad "gh" "error" "cannot list CI runs: $(printf '%s' "$GH_ERR" | head -n 1)"
  else
    FAILURES=$((FAILURES + 1))
    verdict "FAIL no CI run for SHA $SHA (event $EVENT)"
  fi
  echo "ci-gate: $FAILURES failure(s)"
  exit 1
fi

jobs_rc=0
jobs_json="$("$GH" run view "$RUN_ID" --json jobs 2>&1)" || jobs_rc=$?
if [ "$jobs_rc" -ne 0 ]; then
  bad "gh" "error" "cannot read jobs of run $RUN_ID: $(printf '%s' "$jobs_json" | head -n 1)"
  echo "ci-gate: $FAILURES failure(s)"
  exit 1
fi

# job name -> "id<TAB>status<TAB>conclusion"
declare -A JOB=()
declare -a JOB_ORDER=()
while IFS=$'\t' read -r jid jname jstatus jconc; do
  [ -n "$jname" ] || continue
  JOB["$jname"]="$jid"$'\t'"$jstatus"$'\t'"$jconc"
  JOB_ORDER+=("$jname")
done < <(printf '%s' "$jobs_json" | jq -r '.jobs[] | [.databaseId, .name, .status, (.conclusion // "")] | @tsv' | awk -F'\t' '{ print $1 "\t" $2 "\t" $3 "\t" $4 }')

# Expected jobs in a fixed order, each with the module it belongs to ("" = always required).
declare -a EXPECT=() EXPECT_MOD=()
EXPECT+=("workspace"); EXPECT_MOD+=("")
EXPECT+=("integration (nats-server)"); EXPECT_MOD+=("")
for kind in test lint; do
  for m in "${ALL_MODULES[@]}"; do
    s="$m"
    [ "$m" != root ] || s="."
    EXPECT+=("$kind ($s)")
    EXPECT_MOD+=("$m")
  done
done

evaluate() { # job-name module ("" = required, ALL = unexpected job)
  local name="$1" mod="$2" rec jid jstatus jconc log ev
  rec="${JOB[$name]:-}"
  if [ -z "$rec" ]; then
    bad "$name" "missing" "job not present in run $RUN_ID"
    return
  fi
  IFS=$'\t' read -r jid jstatus jconc <<<"$rec"
  if [ "$jstatus" != completed ]; then
    bad "$name" "$jstatus" "job not completed"
    return
  fi
  if [ "$jconc" = success ]; then
    verdict "OK $name"
    return
  fi
  if [ -n "$mod" ] && [ "$mod" != ALL ] && [ -n "${RED[$mod]:-}" ] && [ "$jconc" = failure ]; then
    log="$("$GH" run view "$RUN_ID" --job "$jid" --log-failed 2>&1 || true)"
    if printf '%s\n' "$log" | grep -qE -- '--- FAIL:|panic:'; then
      bad "$name" "$jconc" "allow-red leg has a test failure or panic in its failed-step log"
      return
    fi
    ev="$(printf '%s\n' "$log" | grep -E -m1 -- "$COMPILE_RE" || true)"
    if [ -n "$ev" ]; then
      ev="$(printf '%s' "$ev" | sed -E 's/^[[:space:]]+//' | cut -c1-200)"
      verdict "ALLOWED-RED $name $ev"
      return
    fi
    bad "$name" "$jconc" "allow-red leg has no Go compile or type-check error in its failed-step log"
    return
  fi
  if [ -n "$mod" ] && [ "$mod" != ALL ]; then
    bad "$name" "$jconc" "required module $mod must be success"
  else
    bad "$name" "$jconc" "job must be success"
  fi
}

declare -A DONE=()
for i in "${!EXPECT[@]}"; do
  evaluate "${EXPECT[$i]}" "${EXPECT_MOD[$i]}"
  DONE["${EXPECT[$i]}"]=1
done
for name in "${JOB_ORDER[@]}"; do
  [ -z "${DONE[$name]:-}" ] || continue
  evaluate "$name" ALL
done

if [ "$FAILURES" -eq 0 ]; then
  verdict "PASS ci-gate"
  exit 0
fi
verdict "ci-gate: $FAILURES failure(s)"
exit 1

#!/usr/bin/env bash
# Docs gate for the v0.4.0 release (D-01, D-04, D-05). Read-only: it never edits a file.
#
# Usage: scripts/check-docs.sh [PATH ...]
#        scripts/check-docs.sh --self-test
#   PATH: markdown or Go files, or directories scanned for *.md and *.go.
#         Default: docs README.md ARCHITECTURE.md examples (missing defaults are skipped).
#   CHANGELOG.md is never scanned: it is the migration guide (D-01) and names removed API on purpose.
#
# Checks (one "FAIL <check> <file>:<line> <detail>" line per problem):
#   stale  API removed or renamed in v0.4.0 must not appear (docs are latest-only, D-04).
#   chain  the canonical invoker order is in the five pages that show it (D-05), and no scoped
#          file draws a chain with Metrics in it or with DLQ after CircuitBreaker.
#   link   {{ site.baseurl }}, github.io, relative *.md and github.com/.../main links resolve.
#   nav    just-the-docs navigation: parent pages exist and have has_children, sibling
#          nav_order and permalinks are unique.
#   api    only when docs/api-reference/index.md is in scope: every exported func and type of
#          the 8 library packages is named in backticks on that page (FAIL api <package> <name>).
#
# Ends with "PASS check-docs" (exit 0) or "check-docs: N problem(s)" (exit 1); exit 2 is a usage error.
# Needs bash 4+, awk, grep, sed, find and (for the api check) go. Never touches ./go.work.
#
# Environment (used by --self-test, not needed otherwise):
#   CHECK_DOCS_ROOT         tree to check instead of the repository top
#   CHECK_DOCS_API_MODULES  space separated module dirs for the api check
set -euo pipefail

SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
ROOT="${CHECK_DOCS_ROOT:-$(git rev-parse --show-toplevel)}"
cd "$ROOT"

# Canonical chain order (D-05) and the pages that must show it verbatim.
CANON='Tracer → RateLimit → Idempotency → DLQ → Retry → CircuitBreaker → Handler'
CANON_FILES=(
  docs/invoker-chain/composition.md
  docs/invoker-chain/index.md
  docs/examples/full-invoker-chain.md
  docs/architecture/execution-pipeline.md
  README.md
)

# API removed or renamed in v0.4.0 (names match the v0.3.0 tree):
#   invoker.NewMetrics / invoker.Metrics   the Metrics invoker constructor and type
#   MetricProvider, NoopProvider           the metrics interface and its no-op provider
#   NewDQL                                 the old JetStream DLQ constructor (transposed letters)
#   OnErr:                                 the bus OnErr option literal (the event's OnErr method has no colon)
#   prometheus.NewProvider, WithRegisterer the old Prometheus Provider constructor and registerer option
#   invoker.Labels                         the metric label map type
#   event.handle                           the v0.3.0 span name
#   dql.                                   the v0.3.0 DLQ subject prefix
#   metrics-invoker                        the deleted page slug (D-02)
STALE_RE='\bNewMetrics\b|invoker\.Metrics\b|\bMetricProvider\b|\bNoopProvider\b|\bNewDQL\b|\bOnErr:|prometheus\.NewProvider|\bWithRegisterer\b|invoker\.Labels|\bevent\.handle\b|\bdql\.|metrics-invoker'

# Invoker names, used to tell an invoker arrow chain from prose that merely contains an arrow.
INVOKER_RE='Tracer|RateLimit|Idempotency|Retry|CircuitBreaker|DLQ|Handler'

# Module dirs of the 8 library packages the api check covers; examples joins the temporary go.work.
API_MODULES="${CHECK_DOCS_API_MODULES:-. invoker json nats jetstream redis telemetry prometheus}"
WORK_MODULES="${CHECK_DOCS_WORK_MODULES:-. invoker json nats jetstream redis telemetry prometheus examples}"

PROBLEMS=0
fail() { # check location detail
  printf 'FAIL %s %s %s\n' "$1" "$2" "$3"
  PROBLEMS=$((PROBLEMS + 1))
}

usage() { sed -n '2,/^set -euo/p' "$SELF" | sed '$d' | sed 's/^# \{0,1\}//'; }

# ---------------------------------------------------------------------------
# Self-test: fixtures in a temporary directory, never the repository.
# ---------------------------------------------------------------------------
mkpage() { # path title parent nav_order permalink has_children body...
  local path="$1" title="$2" parent="$3" order="$4" perma="$5" hc="$6"
  shift 6
  mkdir -p "$(dirname "$path")"
  {
    echo '---'
    echo 'layout: default'
    echo "title: $title"
    [ -z "$parent" ] || echo "parent: $parent"
    [ -z "$order" ] || echo "nav_order: $order"
    [ "$hc" != true ] || echo 'has_children: true'
    [ -z "$perma" ] || echo "permalink: $perma"
    echo '---'
    echo
    printf '# %s\n\n' "$title"
    printf '%s\n' "$@"
  } >"$path"
}

make_clean_fixture() { # dir
  local d="$1"
  mkdir -p "$d"
  (
    cd "$d"
    mkpage docs/index.md Home '' 1 / false 'See [chain]({{ site.baseurl }}/invoker-chain/).'
    mkpage docs/invoker-chain/index.md 'Invoker Chain' '' 4 /invoker-chain/ true "$CANON" 'See [composition](composition.md).'
    mkpage docs/invoker-chain/composition.md Composition 'Invoker Chain' 1 '' false "$CANON"
    mkpage docs/examples/index.md Examples '' 6 /examples/ true 'Examples.'
    mkpage docs/examples/full-invoker-chain.md 'Full chain' Examples 1 '' false "$CANON"
    mkpage docs/architecture/index.md Architecture '' 5 /architecture/ true 'Architecture.'
    mkpage docs/architecture/execution-pipeline.md 'Execution pipeline' Architecture 1 '' false "$CANON"
    {
      printf '# README\n\n%s\n\n' "$CANON"
      echo "The event's own \`OnErr\` method: \`func (e E) OnErr(ctx context.Context, err error, name string)\`."
      echo '[site](https://isaquesb.github.io/go-event-bus/invoker-chain/) [src](https://github.com/isaquesb/go-event-bus/blob/main/docs/index.md) [rel](docs/index.md#top)'
      echo 'Docs: [https://isaquesb.github.io/go-event-bus/](https://isaquesb.github.io/go-event-bus/).'
      echo '```'
      echo '[inside a fence](missing-in-fence.md)'
      echo '```'
    } >README.md
    # The migration guide names removed API on purpose and is never scanned.
    printf '# Changelog\n\nRemoved invoker.NewMetrics, MetricProvider and NoopProvider.\n' >CHANGELOG.md
  )
}

st_run() { # dir args... ; sets ST_OUT and ST_RC
  local d="$1"
  shift
  ST_RC=0
  ST_OUT="$(CHECK_DOCS_ROOT="$d" bash "$SELF" "$@" 2>&1)" || ST_RC=$?
}

st_expect_pass() { # label dir args...
  local label="$1"
  shift
  st_run "$@"
  if [ "$ST_RC" -ne 0 ] || ! printf '%s\n' "$ST_OUT" | grep -qx 'PASS check-docs'; then
    echo "self-test FAILED: $label should pass (rc=$ST_RC)" >&2
    printf '%s\n' "$ST_OUT" >&2
    exit 1
  fi
}

st_expect_fail() { # label check dir args...
  local label="$1" check="$2"
  shift 2
  st_run "$@"
  if [ "$ST_RC" -ne 1 ] || ! printf '%s\n' "$ST_OUT" | grep -q "^FAIL $check "; then
    echo "self-test FAILED: $label should print 'FAIL $check' and exit 1 (rc=$ST_RC)" >&2
    printf '%s\n' "$ST_OUT" >&2
    exit 1
  fi
}

self_test() {
  local tmp
  tmp="$(mktemp -d)"
  trap "rm -rf '$tmp'" EXIT
  local clean="$tmp/clean" bad

  make_clean_fixture "$clean"
  st_expect_pass 'clean fixture (default scope)' "$clean"
  st_expect_pass 'clean fixture with CHANGELOG.md passed explicitly' "$clean" docs README.md CHANGELOG.md

  mkbad() { rm -rf "$tmp/bad" && cp -r "$clean" "$tmp/bad" && bad="$tmp/bad"; }

  mkbad && echo 'Use invoker.NewMetrics(p) here.' >>"$bad/docs/invoker-chain/composition.md"
  st_expect_fail 'stale symbol' stale "$bad"
  mkbad && echo 'Span name event.handle.' >>"$bad/docs/invoker-chain/composition.md"
  st_expect_fail 'stale span name' stale "$bad"
  mkbad && echo 'bus := NewLocalBus(OnErr: f)' >>"$bad/docs/invoker-chain/composition.md"
  st_expect_fail 'stale OnErr literal' stale "$bad"

  mkbad && sed -i 's/Tracer → RateLimit → Idempotency → DLQ/Tracer → Idempotency → RateLimit → DLQ/' "$bad/README.md"
  st_expect_fail 'missing canonical chain' chain "$bad"
  mkbad && echo 'Tracer → Metrics → RateLimit → Retry → Handler' >>"$bad/docs/examples/full-invoker-chain.md"
  st_expect_fail 'Metrics in the chain' chain "$bad"
  mkbad && echo 'Retry → CircuitBreaker → DLQ → Handler' >>"$bad/docs/architecture/execution-pipeline.md"
  st_expect_fail 'DLQ after CircuitBreaker' chain "$bad"

  mkbad && echo '[x]({{ site.baseurl }}/nope/)' >>"$bad/docs/invoker-chain/composition.md"
  st_expect_fail 'broken baseurl link' link "$bad"
  mkbad && echo '[x](missing.md)' >>"$bad/docs/invoker-chain/composition.md"
  st_expect_fail 'broken relative link' link "$bad"
  mkbad && echo '[x](https://github.com/isaquesb/go-event-bus/blob/main/docs/missing.md)' >>"$bad/README.md"
  st_expect_fail 'broken github link' link "$bad"

  mkbad && sed -i 's/^parent: Invoker Chain/parent: Missing Parent/' "$bad/docs/invoker-chain/composition.md"
  st_expect_fail 'orphan parent' nav "$bad"
  mkbad && mkpage "$bad/docs/invoker-chain/retry.md" Retry 'Invoker Chain' 1 '' false "$CANON"
  st_expect_fail 'duplicate nav_order' nav "$bad"

  # api check: a one-package module whose exported names the page must list.
  mkbad
  printf 'module example.com/fx\n\ngo 1.21\n' >"$bad/go.mod"
  printf 'package fx\n\n// Hello is exported.\nfunc Hello() {}\n\n// Thing is exported.\ntype Thing struct{}\n' >"$bad/fx.go"
  mkpage "$bad/docs/api-reference/index.md" 'API Reference' '' 9 /api-reference/ false 'Names: `Hello`.'
  ST_RC=0
  ST_OUT="$(CHECK_DOCS_ROOT="$bad" CHECK_DOCS_API_MODULES=. CHECK_DOCS_WORK_MODULES=. bash "$SELF" docs/api-reference/index.md 2>&1)" || ST_RC=$?
  if [ "$ST_RC" -ne 1 ] || ! printf '%s\n' "$ST_OUT" | grep -q '^FAIL api .* Thing$' || printf '%s\n' "$ST_OUT" | grep -q '^FAIL api .* Hello$'; then
    echo "self-test FAILED: api check should flag only Thing (rc=$ST_RC)" >&2
    printf '%s\n' "$ST_OUT" >&2
    exit 1
  fi
  mkpage "$bad/docs/api-reference/index.md" 'API Reference' '' 9 /api-reference/ false 'Names: `Hello` and `fx.Thing`.'
  ST_RC=0
  ST_OUT="$(CHECK_DOCS_ROOT="$bad" CHECK_DOCS_API_MODULES=. CHECK_DOCS_WORK_MODULES=. bash "$SELF" docs/api-reference/index.md 2>&1)" || ST_RC=$?
  if [ "$ST_RC" -ne 0 ]; then
    echo "self-test FAILED: api check should pass when every name is listed (rc=$ST_RC)" >&2
    printf '%s\n' "$ST_OUT" >&2
    exit 1
  fi

  echo 'PASS self-test'
}

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
PATHS=()
for arg in "$@"; do
  case "$arg" in
    --self-test)
      self_test
      exit 0
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    --*)
      echo "unknown option: $arg" >&2
      echo "usage: scripts/check-docs.sh [PATH ...] | --self-test" >&2
      exit 2
      ;;
    *) PATHS+=("$arg") ;;
  esac
done

if [ ${#PATHS[@]} -eq 0 ]; then
  for p in docs README.md ARCHITECTURE.md examples; do
    if [ -e "$p" ]; then PATHS+=("$p"); fi
  done
fi

# Scope: every *.md and *.go file under the given paths, never CHANGELOG.md.
FILES=()
for p in "${PATHS[@]}"; do
  p="${p#./}"
  p="${p%/}"
  if [ -d "$p" ]; then
    while IFS= read -r f; do FILES+=("$f"); done < <(
      find "$p" -type f \( -name '*.md' -o -name '*.go' \) ! -name CHANGELOG.md \
        ! -path '*/_site/*' ! -path '*/node_modules/*' ! -path '*/vendor/*' | LC_ALL=C sort
    )
  elif [ -f "$p" ]; then
    if [ "$(basename "$p")" != CHANGELOG.md ]; then FILES+=("$p"); fi
  else
    echo "check-docs: no such path: $p" >&2
    exit 2
  fi
done

in_scope() { # path
  local f
  for f in "${FILES[@]}"; do
    [ "$f" = "$1" ] && return 0
  done
  return 1
}

# ---------------------------------------------------------------------------
# Check: stale (removed in v0.4.0)
# ---------------------------------------------------------------------------
check_stale() {
  local f hit ln tok
  for f in "${FILES[@]}"; do
    while IFS= read -r hit; do
      [ -n "$hit" ] || continue
      ln="${hit%%:*}"
      tok="${hit#*:}"
      fail stale "$f:$ln" "$tok (removed or renamed in v0.4.0; the CHANGELOG is the migration guide)"
    done < <(grep -noE "$STALE_RE" "$f" || true)
  done
}

# ---------------------------------------------------------------------------
# Check: chain order (D-05)
# ---------------------------------------------------------------------------
check_chain() {
  local f c hit ln
  for f in "${FILES[@]}"; do
    for c in "${CANON_FILES[@]}"; do
      if [ "$f" = "$c" ] && ! grep -qF "$CANON" "$f"; then
        fail chain "$f:1" "missing the canonical order: $CANON"
      fi
    done
    # An arrow chain with Metrics in it, or with DLQ after CircuitBreaker.
    while IFS= read -r hit; do
      [ -n "$hit" ] || continue
      ln="${hit%%:*}"
      fail chain "$f:$ln" "chain places Metrics in the chain (the Metrics invoker was removed): ${hit#*:}"
    done < <(grep -nE 'Metrics →|→ Metrics' "$f" | grep -E "$INVOKER_RE" || true)
    while IFS= read -r hit; do
      [ -n "$hit" ] || continue
      ln="${hit%%:*}"
      fail chain "$f:$ln" "chain places DLQ after CircuitBreaker (DLQ is outside Retry): ${hit#*:}"
    done < <(grep -n 'CircuitBreaker → DLQ' "$f" || true)
  done
}

# ---------------------------------------------------------------------------
# Docs index (front matter of every docs page) for the link and nav checks
# ---------------------------------------------------------------------------
declare -A TITLE=() PARENT=() GPARENT=() ORDER=() HASCH=() PERMA=() EXCL=()
declare -A PERMALINKS=()
DOCS_PAGES=()

index_docs() {
  [ -d docs ] || return 0
  local f k v p
  while IFS= read -r f; do
    DOCS_PAGES+=("$f")
    while IFS=$'\t' read -r k v; do
      case "$k" in
        title) TITLE["$f"]="$v" ;;
        parent) PARENT["$f"]="$v" ;;
        grand_parent) GPARENT["$f"]="$v" ;;
        nav_order) ORDER["$f"]="$v" ;;
        has_children) HASCH["$f"]="$v" ;;
        permalink) PERMA["$f"]="$v" ;;
        nav_exclude) EXCL["$f"]="$v" ;;
      esac
    done < <(awk '
      NR == 1 { if ($0 != "---") exit; next }
      $0 == "---" { exit }
      { i = index($0, ":"); if (i > 0) {
          k = substr($0, 1, i - 1); v = substr($0, i + 1)
          sub(/^[ \t]+/, "", v); sub(/[ \t\r]+$/, "", v)
          sub(/^["\047]/, "", v); sub(/["\047]$/, "", v)
          print k "\t" v } }' "$f")
    if [ -n "${PERMA[$f]:-}" ]; then
      p="${PERMA[$f]#/}"
      p="${p%/}"
      if [ -n "$p" ]; then PERMALINKS["$p"]=1; fi
    fi
  done < <(find docs -type f -name '*.md' ! -path '*/_site/*' | LC_ALL=C sort)
}

# ---------------------------------------------------------------------------
# Check: internal links
# ---------------------------------------------------------------------------
site_target_ok() { # path after the site root, e.g. /observability/metrics/
  local x="${1%%\?*}"
  x="${x#/}"
  x="${x%/}"
  [ -z "$x" ] && return 0
  [ -f "docs/$x.md" ] || [ -f "docs/$x/index.md" ] || [ -f "docs/$x" ] || [ -n "${PERMALINKS[$x]:-}" ]
}

check_links() {
  local f ln kind target dir
  for f in "${FILES[@]}"; do
    case "$f" in *.md) ;; *) continue ;; esac
    dir="$(dirname "$f")"
    while IFS=$'\t' read -r ln kind target; do
      [ -n "$ln" ] || continue
      case "$kind" in
        site)
          site_target_ok "$target" || fail link "$f:$ln" "site link $target resolves to no docs page"
          ;;
        gh)
          if [ -n "$target" ] && [ ! -e "$target" ]; then
            fail link "$f:$ln" "repository link $target does not exist in the working tree"
          fi
          ;;
        rel)
          if [ ! -e "$dir/$target" ]; then
            fail link "$f:$ln" "relative link $target does not exist"
          fi
          ;;
      esac
    done < <(awk '
      /^[ \t]*(```|~~~)/ { fence = !fence; next }
      fence { next }
      {
        s = $0
        while (match(s, /[{][{] *site[.]baseurl *[}][}]\/[^]) "\047#}<>]*/)) {
          t = substr(s, RSTART, RLENGTH); sub(/^[{][{] *site[.]baseurl *[}][}]/, "", t)
          print NR "\tsite\t" t; s = substr(s, RSTART + RLENGTH)
        }
        s = $0
        while (match(s, /https:\/\/isaquesb[.]github[.]io\/go-event-bus[^]) "\047#<>]*/)) {
          t = substr(s, RSTART, RLENGTH); sub(/^https:\/\/isaquesb[.]github[.]io\/go-event-bus/, "", t)
          print NR "\tsite\t" t; s = substr(s, RSTART + RLENGTH)
        }
        s = $0
        while (match(s, /https:\/\/github[.]com\/isaquesb\/go-event-bus\/(blob|tree)\/main\/?[^]) "\047#<>]*/)) {
          t = substr(s, RSTART, RLENGTH); sub(/^https:\/\/github[.]com\/isaquesb\/go-event-bus\/(blob|tree)\/main\/?/, "", t)
          sub(/\/$/, "", t)
          print NR "\tgh\t" t; s = substr(s, RSTART + RLENGTH)
        }
        s = $0
        while (match(s, /\]\([^) ]+\)/)) {
          t = substr(s, RSTART + 2, RLENGTH - 3); s = substr(s, RSTART + RLENGTH)
          sub(/#.*$/, "", t)
          if (t ~ /[.]md$/ && t !~ /^(https?:|\/|[{]|mailto:)/) print NR "\trel\t" t
        }
      }' "$f")
  done
}

# ---------------------------------------------------------------------------
# Check: just-the-docs navigation
# ---------------------------------------------------------------------------
check_nav() {
  local f g p key found order seen
  for f in "${FILES[@]}"; do
    case "$f" in docs/*.md) ;; *) continue ;; esac
    p="${PARENT[$f]:-}"
    if [ -n "$p" ]; then
      found=0
      for g in "${DOCS_PAGES[@]}"; do
        if [ "${TITLE[$g]:-}" = "$p" ] && [ "${HASCH[$g]:-}" = true ]; then
          found=1
          break
        fi
      done
      [ "$found" -eq 1 ] || fail nav "$f:1" "parent \"$p\" has no docs page with title: $p and has_children: true"
    fi
    order="${ORDER[$f]:-}"
    if [ -n "$order" ] && [ "${EXCL[$f]:-}" != true ]; then
      key="${GPARENT[$f]:-}|$p"
      for g in "${DOCS_PAGES[@]}"; do
        [ "$g" != "$f" ] || continue
        [ "${EXCL[$g]:-}" != true ] || continue
        if [ "${GPARENT[$g]:-}|${PARENT[$g]:-}" = "$key" ] && [ "${ORDER[$g]:-}" = "$order" ]; then
          fail nav "$f:1" "nav_order $order is also used by $g (same parent: ${p:-top level})"
        fi
      done
    fi
    if [ -n "${PERMA[$f]:-}" ]; then
      seen="${PERMA[$f]#/}"
      seen="${seen%/}"
      for g in "${DOCS_PAGES[@]}"; do
        [ "$g" != "$f" ] || continue
        p="${PERMA[$g]:-}"
        p="${p#/}"
        p="${p%/}"
        if [ -n "${PERMA[$g]:-}" ] && [ "$p" = "$seen" ]; then
          fail nav "$f:1" "permalink ${PERMA[$f]} is also used by $g"
        fi
      done
    fi
  done
}

# ---------------------------------------------------------------------------
# Check: api reference names every exported func and type (CLAUDE.md release checklist)
# ---------------------------------------------------------------------------
check_api() {
  local page=docs/api-reference/index.md
  in_scope "$page" || return 0
  local tmp ws words m label name
  tmp="$(mktemp -d)"
  ws="$tmp/api.work"
  # Words that appear in backticks (inline spans or fenced code) on the page.
  words="$tmp/words"
  awk '
    /^[ \t]*(```|~~~)/ { fence = !fence; next }
    fence { print; next }
    { s = $0; while (match(s, /`[^`]+`/)) { print substr(s, RSTART + 1, RLENGTH - 2); s = substr(s, RSTART + RLENGTH) } }
  ' "$page" | grep -oE '[A-Za-z_][A-Za-z0-9_]*' | LC_ALL=C sort -u >"$words" || true

  local args=() d
  for d in $WORK_MODULES; do args+=("$ROOT/$d"); done
  if ! (GOWORK="$ws" go work init "${args[@]}" >/dev/null 2>&1); then
    rm -rf "$tmp"
    fail api - "go work init failed over: $WORK_MODULES"
    return 0
  fi
  for m in $API_MODULES; do
    label="$m"
    [ "$m" != . ] || label=root
    while IFS= read -r name; do
      [ -n "$name" ] || continue
      grep -qxF "$name" "$words" || fail api "$label" "$name"
    done < <(
      cd "$ROOT/$m" && GOWORK="$ws" go doc -short . 2>/dev/null |
        sed -nE 's/^ *(func|type) +([A-Z][A-Za-z0-9_]*).*/\2/p' | LC_ALL=C sort -u
    )
  done
  rm -rf "$tmp"
}

index_docs
check_stale
check_chain
check_links
check_nav
check_api

if [ "$PROBLEMS" -eq 0 ]; then
  echo 'PASS check-docs'
  exit 0
fi
echo "check-docs: $PROBLEMS problem(s)"
exit 1

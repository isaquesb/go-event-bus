#!/usr/bin/env bash
# Per-layer release preparation (D-18, D-22): bump in-repo requires, verify with GOWORK=off,
# commit, create annotated LOCAL tags, and print the push commands. It never pushes, never deletes
# or moves a tag, and never uses a bulk tag push. Pushing stays a separate, human-gated step
# (see RELEASING.md).
#
# Usage: scripts/release.sh [--dry-run] VERSION LAYER
#        scripts/release.sh --check-layers
#   VERSION  vMAJOR.MINOR.PATCH
#   LAYER    1 root
#            2 invoker json nats telemetry prometheus  (require root)
#            3 jetstream redis                         (require root invoker)
#            4 examples                                (require root invoker jetstream json redis telemetry)
#   --dry-run       run the preflight checks (printing OK/FAIL per check) and print the planned go get,
#                   commit and tag actions; changes no file, commit or tag. Exits non-zero if any
#                   preflight check failed.
#   --check-layers  compare the in-repo requires of every module's go.mod with the layer table above
#                   and the layer order (a module may only require modules of lower layers); print one
#                   OK/FAIL line per module and exit 0 or 1. It reads go.mod files only and needs no
#                   VERSION or LAYER, no git state and no network. Every real and dry run repeats it
#                   in the preflight.
set -euo pipefail

ROOT_PATH="github.com/isaquesb/go-event-bus"

die() {
  echo "error: $*" >&2
  exit 2
}

# --- the D-26 layer table, encoded once --------------------------------------------------------
layer_modules() {
  case "$1" in
    1) echo "root" ;;
    2) echo "invoker json nats telemetry prometheus" ;;
    3) echo "jetstream redis" ;;
    4) echo "examples" ;;
  esac
}

# In-repo dependencies a layer's modules must require at VERSION. Root is named explicitly for
# layer 3 because redis carries it only as an indirect line.
layer_deps() {
  case "$1" in
    1) echo "" ;;
    2) echo "root" ;;
    3) echo "root invoker" ;;
    4) echo "root invoker jetstream json redis telemetry" ;;
  esac
}

mod_path() {
  if [ "$1" = root ]; then echo "$ROOT_PATH"; else echo "$ROOT_PATH/$1"; fi
}
mod_dir() {
  if [ "$1" = root ]; then echo "."; else echo "$1"; fi
}
mod_tag() {
  if [ "$1" = root ]; then echo "$VERSION"; else echo "$1/$VERSION"; fi
}

# The layer number of module $1, empty when it is in no layer.
layer_of() {
  local want="$1" l x
  for l in 1 2 3 4; do
    for x in $(layer_modules "$l"); do
      if [ "$x" = "$want" ]; then
        echo "$l"
        return 0
      fi
    done
  done
  return 0
}

# check_layers: the layer table must agree with the real in-repo requires of every go.mod (R1: a
# layer-3 tidy drops a require the module does not import, which aborts a layer after the earlier
# tags are public). Per module: (a) the set of in-repo requires, direct and indirect, equals
# layer_deps of its layer; (b) every required module is in a lower layer. Reads go.mod only, from
# the current directory (the repository top level); it never runs go get or go mod tidy.
check_layers() {
  local l m dir listing p name got want d g k bad rc=0 shown
  for l in 1 2 3 4; do
    for m in $(layer_modules "$l"); do
      dir="$(mod_dir "$m")"
      if [ ! -f "$dir/go.mod" ]; then
        echo "FAIL layers: $m (layer $l): $dir/go.mod not found"
        rc=1
        continue
      fi
      listing="$(cd "$dir" && GOWORK=off go mod edit -print | awk -v m="$ROOT_PATH" '
        { if ($1 == "require") { p = $2; v = $3 } else { p = $1; v = $2 }
          if (index(p, m) == 1 && v ~ /^v[0-9]/) print p }')"
      got=""
      while read -r p; do
        [ -n "$p" ] || continue
        if [ "$p" = "$ROOT_PATH" ]; then name=root; else name="${p#"$ROOT_PATH"/}"; fi
        got="$got $name"
      done <<<"$listing"
      got="$(printf '%s\n' $got | sort -u | tr '\n' ' ')"
      want="$(layer_deps "$l")"
      bad=0
      for d in $want; do
        case " $got " in
          *" $d "*) ;;
          *)
            echo "FAIL layers: $m (layer $l): missing $d"
            bad=1
            ;;
        esac
      done
      for g in $got; do
        case " $want " in
          *" $g "*) ;;
          *)
            echo "FAIL layers: $m (layer $l): unexpected $g"
            bad=1
            ;;
        esac
        k="$(layer_of "$g")"
        if [ -z "$k" ]; then
          echo "FAIL layers: $m (layer $l): $g is in no layer"
          bad=1
        elif [ "$k" -ge "$l" ]; then
          echo "FAIL layers: $m (layer $l): $g is in layer $k, not below $l"
          bad=1
        fi
      done
      if [ "$bad" -eq 0 ]; then
        shown="${got% }"
        echo "OK   layers: $m (layer $l) requires: ${shown:-none}"
      else
        rc=1
      fi
    done
  done
  return "$rc"
}

CHECK_ONLY=0
DRY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run)
      DRY=1
      shift
      ;;
    --check-layers)
      CHECK_ONLY=1
      shift
      ;;
    -h | --help)
      sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    --*) die "unknown option: $1" ;;
    *) break ;;
  esac
done
if [ "$CHECK_ONLY" -eq 1 ]; then
  [ $# -eq 0 ] || die "--check-layers takes no VERSION or LAYER"
  TOP="$(git rev-parse --show-toplevel 2>/dev/null)" || die "--check-layers must run inside the repository"
  cd "$TOP"
  if check_layers; then exit 0; else exit 1; fi
fi
[ $# -eq 2 ] || die "usage: scripts/release.sh [--dry-run] VERSION LAYER | --check-layers"
VERSION="$1"
LAYER="$2"
[[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "VERSION must look like vMAJOR.MINOR.PATCH, got: $VERSION"
[[ "$LAYER" =~ ^[1-4]$ ]] || die "LAYER must be 1, 2, 3 or 4, got: $LAYER"

read -r -a MODS <<<"$(layer_modules "$LAYER")"
read -r -a DEPS <<<"$(layer_deps "$LAYER")"

LOWER_MODS=()
for ((l = 1; l < LAYER; l++)); do
  read -r -a lm <<<"$(layer_modules "$l")"
  LOWER_MODS+=("${lm[@]}")
done

# --- preflight -----------------------------------------------------------------------------------
FAILED=0
pf() {
  case "$1" in
    ok) echo "OK   $2" ;;
    warn) echo "WARN $2" ;;
    fail)
      echo "FAIL $2"
      FAILED=1
      if [ "$DRY" -eq 0 ]; then
        echo "preflight failed, stopping" >&2
        exit 1
      fi
      ;;
  esac
}

MODE=""
[ "$DRY" -eq 0 ] || MODE=" (dry run)"
echo "== release.sh$MODE $VERSION layer $LAYER: ${MODS[*]}"
echo "== preflight"

TOP="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -n "$TOP" ] && [ "$PWD" = "$TOP" ]; then
  pf ok "running from the repository top level"
else
  pf fail "run from the repository top level (${TOP:-not a git repository})"
fi
[ -n "$TOP" ] || exit 1
cd "$TOP"

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$BRANCH" = main ]; then pf ok "on branch main"; else pf fail "on branch '$BRANCH', must be main"; fi

if [ -z "$(git status --porcelain)" ]; then pf ok "working tree clean"; else pf fail "working tree is dirty"; fi

if [ -z "$(git ls-files CLAUDE.md .planning)" ]; then
  pf ok "CLAUDE.md and .planning are untracked"
else
  pf fail "CLAUDE.md or .planning is tracked"
fi

if check_layers; then
  pf ok "layer table matches every go.mod"
else
  pf fail "layer table disagrees with go.mod (run scripts/release.sh --check-layers)"
fi

# Branch only: no tag fetch, no prune, so unpushed local tags survive.
if git fetch --no-tags origin main >/dev/null 2>&1; then
  pf ok "fetched origin/main"
  if git merge-base --is-ancestor origin/main HEAD; then
    pf ok "origin/main is an ancestor of HEAD"
  else
    pf fail "origin/main is not an ancestor of HEAD (diverged or behind)"
  fi
else
  pf fail "git fetch origin main failed"
fi

HEAD_SHA="$(git rev-parse HEAD)"

for m in "${MODS[@]}"; do
  tag="$(mod_tag "$m")"
  remote="$(git ls-remote --tags origin "refs/tags/$tag" 2>/dev/null || echo "__LSREMOTE_FAILED__")"
  if [ "$remote" = "__LSREMOTE_FAILED__" ]; then
    pf fail "could not query origin for tag $tag"
  elif [ -n "$remote" ]; then
    pf fail "tag $tag already exists on origin"
  else
    pf ok "tag $tag absent on origin"
  fi
  if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    if [ "$(git rev-parse "refs/tags/$tag^{commit}")" = "$HEAD_SHA" ]; then
      pf ok "local tag $tag already points at HEAD"
    else
      pf fail "local tag $tag exists but does not point at HEAD; delete that unpushed local tag yourself (git tag -d $tag) and rerun"
    fi
  fi
done

if [ "$LAYER" -gt 1 ]; then
  for m in "${LOWER_MODS[@]}"; do
    tag="$(mod_tag "$m")"
    remote="$(git ls-remote --tags origin "refs/tags/$tag" 2>/dev/null || true)"
    if [ -n "$remote" ]; then
      pf ok "lower-layer tag $tag is on origin"
    else
      pf fail "lower-layer tag $tag is not on origin yet (release the lower layers first)"
    fi
  done
else
  if [ "$HEAD_SHA" = "$(git rev-parse origin/main 2>/dev/null || true)" ]; then
    pf ok "HEAD equals origin/main (end-of-phase commit is published)"
  else
    pf warn "HEAD is not yet equal to origin/main: push main and wait for green CI before pushing the tag"
  fi
fi

# --- plan ---------------------------------------------------------------------------------------
if [ "$DRY" -eq 1 ]; then
  echo "== planned actions (dry run, nothing is changed)"
  if [ "$LAYER" -gt 1 ]; then
    for m in "${MODS[@]}"; do
      args=""
      for d in "${DEPS[@]}"; do args="$args $(mod_path "$d")@$VERSION"; done
      echo "  (cd $(mod_dir "$m") && GOWORK=off go get$args && GOWORK=off go mod tidy)"
    done
  fi
  echo "  scripts/ci-local.sh ${MODS[*]}"
  if [ "$LAYER" -gt 1 ]; then
    echo "  git add <layer go.mod/go.sum by explicit path> && git commit -m 'chore(release): require $VERSION of in-repo dependencies for layer $LAYER'"
  fi
  for m in "${MODS[@]}"; do
    echo "  git tag -a $(mod_tag "$m") -m 'Release $(mod_tag "$m")'"
  done
  if [ "$FAILED" -ne 0 ]; then
    echo "== preflight FAILED (dry run)" >&2
    exit 1
  fi
  echo "== preflight OK (dry run)"
  exit 0
fi

# --- real run -----------------------------------------------------------------------------------
export GOWORK=off

directive_guard() {
  local dir="$1"
  grep -qx 'go 1.26.0' "$dir/go.mod" || {
    echo "FAIL $dir/go.mod: directive must be exactly 'go 1.26.0'" >&2
    exit 1
  }
  if grep -q '^toolchain ' "$dir/go.mod"; then
    echo "FAIL $dir/go.mod: toolchain line present" >&2
    exit 1
  fi
}

# Every in-repo require line in DIR/go.mod must be at VERSION (direct or indirect), and each of
# the layer's dependencies must be present.
assert_requires() {
  local dir="$1" listing d p
  listing="$(cd "$dir" && go mod edit -print | awk -v m="$ROOT_PATH" '
    { if ($1 == "require") { p = $2; v = $3 } else { p = $1; v = $2 }
      if (index(p, m) == 1 && v ~ /^v[0-9]/) print p " " v }')"
  while read -r p v; do
    [ -n "$p" ] || continue
    if [ "$v" != "$VERSION" ]; then
      echo "FAIL $dir/go.mod: $p is $v, want $VERSION" >&2
      exit 1
    fi
  done <<<"$listing"
  for d in "${DEPS[@]}"; do
    printf '%s\n' "$listing" | grep -qxF "$(mod_path "$d") $VERSION" || {
      echo "FAIL $dir/go.mod: missing require $(mod_path "$d") $VERSION" >&2
      exit 1
    }
  done
}

if [ "$LAYER" -gt 1 ]; then
  echo "== bumping in-repo requires to $VERSION"
  for m in "${MODS[@]}"; do
    dir="$(mod_dir "$m")"
    args=()
    for d in "${DEPS[@]}"; do args+=("$(mod_path "$d")@$VERSION"); done
    (cd "$dir" && go get "${args[@]}" && go mod tidy)
    directive_guard "$dir"
    assert_requires "$dir"
    echo "  $m: requires bumped"
  done
fi

echo "== verification (GOWORK=off): scripts/ci-local.sh ${MODS[*]}"
scripts/ci-local.sh "${MODS[@]}"

if [ "$LAYER" -gt 1 ]; then
  allowed=()
  for m in "${MODS[@]}"; do
    allowed+=("$(mod_dir "$m")/go.mod" "$(mod_dir "$m")/go.sum")
  done
  to_add=()
  for f in "${allowed[@]}"; do
    if [ -n "$(git status --porcelain -- "$f")" ]; then to_add+=("$f"); fi
  done
  if [ ${#to_add[@]} -gt 0 ]; then
    git add -- "${to_add[@]}"
    staged="$(git diff --cached --name-only)"
    for f in $staged; do
      ok=0
      for a in "${to_add[@]}"; do [ "$f" = "$a" ] && ok=1; done
      if [ "$ok" -ne 1 ]; then
        echo "FAIL: unexpected staged path: $f (aborting before commit)" >&2
        exit 1
      fi
    done
    git commit -m "chore(release): require $VERSION of in-repo dependencies for layer $LAYER"
    HEAD_SHA="$(git rev-parse HEAD)"
    echo "== committed bump: $(git rev-parse --short HEAD)"
  else
    echo "== go.mod/go.sum already at $VERSION, no bump commit needed"
  fi
fi

echo "== creating annotated local tags on $(git rev-parse --short HEAD)"
TAGS=()
for m in "${MODS[@]}"; do
  tag="$(mod_tag "$m")"
  if git rev-parse -q --verify "refs/tags/$tag" >/dev/null &&
    [ "$(git rev-parse "refs/tags/$tag^{commit}")" = "$HEAD_SHA" ]; then
    echo "  $tag already points at HEAD, skipping"
  else
    git tag -a "$tag" -m "Release $tag" "$HEAD_SHA"
  fi
  [ "$(git rev-parse "refs/tags/$tag^{commit}")" = "$HEAD_SHA" ] || {
    echo "FAIL: $tag does not point at HEAD" >&2
    exit 1
  }
  TAGS+=("refs/tags/$tag")
done

PUBLISHED=("${LOWER_MODS[@]}" "${MODS[@]}")

echo
echo "== next steps (this script did not push anything)"
if ! git merge-base --is-ancestor HEAD origin/main; then
  echo "  git push origin main"
  echo "  # wait for the CI run of $(git rev-parse --short HEAD) to be green"
fi
echo "  # human review: git log --oneline origin/main..HEAD ; git rev-parse ${TAGS[*]/refs\/tags\//}"
echo "  git push origin ${TAGS[*]}"
echo "  scripts/verify-release.sh --wait 30 $VERSION ${PUBLISHED[*]}"

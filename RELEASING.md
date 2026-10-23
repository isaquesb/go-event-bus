# Releasing

How to publish a version of go-event-bus. Read it fully before the first tag: pushed tags are
permanent (see [Rules](#rules)).

## Overview

The repository is 9 Go modules released together at one version:

| Module | Module path | Tag |
|--------|-------------|-----|
| root | `github.com/isaquesb/go-event-bus` | `vX.Y.Z` |
| invoker, json, nats, jetstream, redis, telemetry, prometheus, examples | `github.com/isaquesb/go-event-bus/DIR` | `DIR/vX.Y.Z` |

Tags are annotated, with the message `Release TAG` (for example `Release invoker/v0.3.0`).
`scripts/release.sh` creates them that way.

## Dependency graph and release layers

The in-repo modules depend on each other, so a tag can only be created after the modules it requires
are published. The real import graph gives four layers:

| Layer | Modules | In-repo requires bumped to the new version |
|-------|---------|--------------------------------------------|
| 1 | root | none (root has no dependencies) |
| 2 | invoker, json, nats, telemetry, prometheus | root |
| 3 | jetstream, redis | root, invoker |
| 4 | examples | root, invoker, jetstream, json, redis, telemetry |

Why: invoker, json, nats, telemetry and prometheus import root only (prometheus stopped importing
invoker in v0.4.0); jetstream imports root and invoker; redis imports invoker and carries root only
as an indirect line, which is why the script names root explicitly in layer 3; examples imports root,
invoker, jetstream, json, redis and telemetry.

`scripts/release.sh` encodes exactly this table (and so must any change to this document).
`scripts/release.sh --check-layers` asserts the table against the in-repo requires of every go.mod
and the layer order (a module may only require modules of lower layers), and every run of
`scripts/release.sh`, real or `--dry-run`, repeats it in the preflight. It exists because a layer-3
`go mod tidy` drops a require the module does not import (for example prometheus on invoker) and
aborts the layer after the earlier tags are already public.

## Pre-release gate

Before the first layer:

1. Update `docs/` (transports, core concepts, examples, `docs/api-reference/index.md`), add a
   `## [X.Y.Z]` section to `CHANGELOG.md` (Added, Changed, Fixed, Removed, Dependencies) and make
   sure `examples/*.go` still build (they are compiled in CI). `scripts/check-docs.sh` must pass.
   If the release changes root API (for example a new return value), also check that `examples`
   passes `scripts/ci-local.sh examples` against the new root: before the layer-4 bump the per-module
   leg still builds against the old published root, so a break (v0.3.0: `errcheck` on the new
   `EmitSync` result) only shows up in the layer-4 bump. Fix it in a normal commit pushed before
   `scripts/release.sh VERSION 4`; the bump commit then descends from that fix and still touches only
   `examples/go.mod` and `go.sum`.
2. Run the full local CI equivalent: `scripts/ci-local.sh --workspace --integration`, with
   golangci-lint v2 on PATH. The per-module `GOWORK=off` legs of modules that use unreleased root API
   are red before the release (see [Merging the release work](#merging-the-release-work)).
3. Merge the release work as described next.

### Merging the release work

The milestone work reaches `main` through a pull request that is merged before layer 1, and layer 1
is tagged from the merged `main` commit. The pull request must not carry planning or local files:
`git diff --name-only origin/main...HEAD` must list nothing under `.planning/`, no `CLAUDE.md` and no
`go.work`.

Merge gate:

- Every CI job is green except the `test (MODULE)` and `lint (MODULE)` legs of the modules that use
  in-repo API that is not published yet. Those legs build against the published versions of the
  in-repo dependencies (`GOWORK=off`), so they are red until that API is released.
- Each such red leg may fail only with Go compile or type-check errors against the published in-repo
  modules (for example `undefined: event.Observer`, `unknown field`, `not enough arguments in
  call`), confirmed by reading the job log. `scripts/ci-gate.sh --sha SHA --event pull_request|push
  --require MODULES --allow-red MODULES` checks this mechanically. A `--- FAIL:` test line, a
  `panic:` or any other error blocks the merge.
- The `workspace` and `integration (nats-server)` jobs must be green: they test the whole tree.
- The v0.4.0 red set is invoker, json, nats, jetstream, telemetry, prometheus, examples. redis builds
  against the published v0.3.0 and must be green.

`main` has no branch protection (the `ProtectMain` ruleset is disabled), so required checks do not
block the merge; the human checkpoint before the merge is the gate. This section answers the Phase 6
review finding WR-05 (the red `GOWORK=off` legs before a release are expected, the `workspace` job is
the signal).

## Per-layer procedure

Repeat for layers 1 to 4, in order. VERSION is for example `v0.3.0`; N is the layer number.

1. `scripts/release.sh VERSION N` (add `--dry-run` first to see the preflight). For layers 2 to 4
   it bumps the in-repo requires and `go.sum` with `GOWORK=off`, runs `scripts/ci-local.sh` for the
   layer's modules, commits the bump and creates annotated local tags. It never pushes.
2. `scripts/ci-local.sh --workspace --integration` for the full picture.
3. `git push origin main` (layers 2 to 4 have a bump commit; layer 1 usually has nothing new) and
   wait for green CI on that exact commit. Green CI for layer N means: on that exact commit the
   `workspace` job, the `integration (nats-server)` job and the `test`/`lint` legs of every module
   of layers 1..N are green. Check it with
   `scripts/ci-gate.sh --wait --sha HEAD_SHA --event push --require "<modules of layers 1..N>" --allow-red "<modules of higher layers>"`.
   Legs of higher layers may still be red, only under the merge-gate rule above: after the v0.4.0
   layer-2 bump jetstream and examples are red, after layer 3 examples is red, and after layer 4
   all 20 jobs are green.
4. Pre-publication proof (layers 2 to 4): `scripts/verify-release.sh --ref HEAD VERSION MODULE...`
   for the layer's modules. It resolves those modules from the local tagged tree and their
   dependencies from the proxy, so it needs no tag on origin. At layer 2 this also runs the
   prometheus probe. (Layer 1 used `GOPROXY=off scripts/verify-release.sh --ref HEAD VERSION root`,
   which runs the v0.4.0 root probe offline.)
5. Human checkpoint. Review the tag-to-commit mapping (`git rev-parse TAG^{commit}` for every
   tag of the layer) and run `git merge-base --is-ancestor TAG origin/main` for each tag. Only
   continue when every tag points at the intended, published, green commit.
6. Push the layer's tags by explicit refspec only, for example
   `git push origin refs/tags/v0.3.0` or
   `git push origin refs/tags/invoker/v0.3.0 refs/tags/json/v0.3.0 refs/tags/nats/v0.3.0 refs/tags/telemetry/v0.3.0 refs/tags/prometheus/v0.3.0`.
7. `scripts/verify-release.sh --wait 30 VERSION MODULE...` for every module published so far
   (the script prints the exact command). Do not start the next layer until it exits 0.

The layers by module name for `verify-release.sh` are: layer 1 `root`; layer 2 adds
`invoker json nats telemetry prometheus`; layer 3 adds `jetstream redis`; layer 4 adds `examples`.

## Rules

- Never push all tags at once (`git push --tags` and `git push origin --tags` are forbidden). Always
  push explicit `refs/tags/...` refspecs, one layer at a time.
- Never move, delete or re-push a pushed tag. proxy.golang.org and sum.golang.org keep the first
  hash they saw. The root `v0.1.0` tag was moved after it was cached, and now a direct fetch of that
  version fails with `SECURITY ERROR: checksum mismatch`. A mistake needs a new version.
- Never request an unpublished version from the proxy (`go get`, `go list -m` of a version whose tag is
  not pushed yet): a miss can be cached for up to 30 minutes. Before the tag is pushed use
  `scripts/verify-release.sh --ref GIT_REF VERSION MODULE`, which resolves the module from a local
  directory instead. After the push, a new tag normally appears on the proxy within a minute;
  `--wait 30` polls with backoff.
- `GOPROXY=direct` is only a way to confirm that a tag exists on GitHub, never a verification: the
  check must observe what the default proxy and sumdb serve to users.
- `scripts/release.sh` creates commits and local tags only. Pushing is always a separate, human-gated
  step.
- Never commit `CLAUDE.md` or anything under `.planning/`.

## Go directive policy

Every module declares exactly `go 1.26.0` and has no `toolchain` line. Set it with
`go mod edit -go=1.26.0`, never with the `-go` flag of `go mod tidy` (it fails once a dependency
declares `go 1.26.0`). Run every build, test and release verification with `GOWORK=off`: a local
`go.work` may list parked modules (kafka, rabbitmq, sqs) and hides stale `require` lines. CI fails any
module whose directive differs.

## CI caveat: GOWORK=off legs and unreleased root API

The per-module CI legs run with `GOWORK=off`, so each submodule is built against the published
versions of its in-repo dependencies. When a submodule starts to use root API that is not released
yet, that submodule's leg turns red until the root is released. That is by design, not a CI bug. The
`workspace` job (which builds all 9 modules together) is the pre-release signal in that period.

The `integration (nats-server)` job and `scripts/ci-local.sh --integration` run the JetStream
integration tests over a temporary `go.work` of all 9 modules, like the `workspace` job, so they test
the tree and not the published dependencies. The examples module runs `go test` in CI as well.

## Local integration tests

`scripts/ci-local.sh --integration` starts a `nats:2.15.1` container and runs the JetStream
integration tests. If host ports 4222 or 8222 are taken, set `NATS_HOST_PORT` and `NATS_MON_PORT`.
Manual equivalent:

```bash
docker run -d --rm --name nats-it -p 4222:4222 -p 8222:8222 nats:2.15.1 -js -m 8222
until curl -sf 'http://localhost:8222/healthz?js-enabled-only=true' >/dev/null; do sleep 0.5; done
cd jetstream && GOWORK=off NATS_URL=nats://localhost:4222 go test -tags integration -race -count=1 ./...
docker stop nats-it
```

## Final verification

After layer 4 is pushed:

```bash
scripts/verify-release.sh --wait 30 VERSION
```

It checks all 9 modules in fresh temporary modules with the default proxy, requires every in-repo
module in the build list to be at exactly VERSION, and runs two behavioral probes that only pass on
v0.4.0 code. The root probe builds a `LocalBus` with a nil Invoker, an `Observer` and a `MemoryDLQ`,
checks the two-value `EmitSync`, and requires exactly one `Failure` with outcome `dead_lettered` and
one DLQ record for a terminal error. The prometheus probe calls `NewObserver` on a fresh registry,
reports one `HandlerDone` and requires `eventbus_handler_total` with one metric of value 1.
`scripts/verify-release.sh --print-probe root|prometheus` prints a probe program. The command must
exit 0.

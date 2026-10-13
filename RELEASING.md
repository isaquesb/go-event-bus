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
| 2 | invoker, json, nats, telemetry | root |
| 3 | jetstream, redis, prometheus | root, invoker |
| 4 | examples | root, invoker, jetstream, json, redis, telemetry |

Why: invoker, json, nats and telemetry import root; jetstream, redis and prometheus import invoker
(jetstream also imports root); examples imports root, invoker, jetstream, json, redis and telemetry.
redis and prometheus carry root only as an indirect line, which is why the script names root
explicitly in layer 3.

`scripts/release.sh` encodes exactly this table (and so must any change to this document).

## Pre-release gate

Before the first layer:

1. Update `docs/` (transports, core concepts, examples, `docs/api-reference/index.md`), add a
   `## [X.Y.Z]` section to `CHANGELOG.md` (Added, Changed, Fixed, Dependencies) and make sure
   `examples/*.go` still build (they are compiled in CI). If the release changes root API (for
   example a new return value), also check that `examples` passes `scripts/ci-local.sh examples`
   against the new root: before the layer-4 bump the per-module leg still builds against the old
   published root, so a break (v0.3.0: `errcheck` on the new `EmitSync` result) only shows up in
   the layer-4 bump. Fix it in a normal commit pushed before `scripts/release.sh VERSION 4`; the
   bump commit then descends from that fix and still touches only `examples/go.mod` and `go.sum`.
2. Run the full local CI equivalent: `scripts/ci-local.sh --integration`.
3. Push `main` and wait for its CI run to be green. The root tag points at this end-of-phase commit,
   so nothing may be tagged from an unpublished or red commit.

## Per-layer procedure

Repeat for layers 1 to 4, in order. VERSION is for example `v0.3.0`; N is the layer number.

1. `scripts/release.sh VERSION N` (add `--dry-run` first to see the preflight). For layers 2 to 4
   it bumps the in-repo requires and `go.sum` with `GOWORK=off`, runs `scripts/ci-local.sh` for the
   layer's modules, commits the bump and creates annotated local tags. It never pushes.
2. `scripts/ci-local.sh --integration` for the full picture.
3. `git push origin main` (layers 2 to 4 have a bump commit; layer 1 usually has nothing new) and
   wait for green CI on that exact commit.
4. Pre-publication proof (layers 2 to 4): `scripts/verify-release.sh --ref HEAD VERSION MODULE...`
   for the layer's modules. It resolves those modules from the local tagged tree and their
   dependencies from the proxy, so it needs no tag on origin. (Layer 1 used
   `GOPROXY=off scripts/verify-release.sh --ref VERSION VERSION root`.)
5. Human checkpoint. Review the tag-to-commit mapping (`git rev-parse TAG^{commit}` for every
   tag of the layer) and run `git merge-base --is-ancestor TAG origin/main` for each tag. Only
   continue when every tag points at the intended, published, green commit.
6. Push the layer's tags by explicit refspec only, for example
   `git push origin refs/tags/v0.3.0` or
   `git push origin refs/tags/invoker/v0.3.0 refs/tags/json/v0.3.0 refs/tags/nats/v0.3.0 refs/tags/telemetry/v0.3.0`.
7. `scripts/verify-release.sh --wait 30 VERSION MODULE...` for every module published so far
   (the script prints the exact command). Do not start the next layer until it exits 0.

The layers by module name for `verify-release.sh` are: layer 1 `root`; layer 2 adds
`invoker json nats telemetry`; layer 3 adds `jetstream redis prometheus`; layer 4 adds `examples`.

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
module in the build list to be at exactly VERSION, and runs a probe that proves the fixed root code
is what users get. It must exit 0.

# elle-cli (vendored)

Standalone CLI for Jepsen's Elle transactional-safety checker.

- Upstream: https://github.com/ligurio/elle-cli
- Version: 0.1.9 (release asset `elle-cli-bin-0.1.9.zip`)
- sha256(elle-cli-0.1.9-standalone.jar): `c9ba9b9fd32640e73d632cb5f15069c162ba6528a67f27a878767187c59f539a`
- License: Eclipse Public License 2.0 (see `LICENSE`, copied from upstream)
- Requires: Java 11+ (repo toolchain: Temurin 21)
- Used by: `scripts/elle_check.sh` via `make consistency/elle`
  (see `docs/tasks/task41_elle_consistency_harness.md`)

Invocation shape the pipeline relies on:

```
java -jar elle-cli-0.1.9-standalone.jar --model list-append \
    --consistency-models <snapshot-isolation|serializable> <history.edn>
```

stdout: `<file>\t<true|false|unknown>`.

`fixtures/known_bad.edn` is a hand-written list-append write-skew history
(two transactions that each read a key the other appends to — a pure
rw-antidependency cycle). It is invalid under `serializable` but valid under
`snapshot-isolation`, so it exercises exactly the model distinction the
pipeline depends on; the check script requires elle-cli to reject it under
`serializable` before trusting any real verdict.

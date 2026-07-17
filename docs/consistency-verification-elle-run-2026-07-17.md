# Elle consistency run — results, 2026-07-17

Recorded output of `make consistency/elle` against the current tree. All three
read passes (point-read, table-scan, predicate/index) pass under both isolation
levels: **SI shows exactly `G2-item` (write skew) and nothing worse; SSI shows
no anomalies.** For what the harness is and why these are the expected verdicts,
see the canonical writeup `docs/consistency-verification-elle-2026-07-07.md` and
the task docs `docs/tasks/task45_elle_consistency_harness.md` /
`docs/tasks/task47_elle_anomaly_and_mutation.md`.

## Provenance

- 2026-07-17, `make consistency/elle` (default sizing), local sandbox.
- ultima_db git `27d7581`, rustc 1.96.0, OpenJDK 21.0.11.
- Checker: vendored elle-cli 0.1.9 (`tools/elle-cli/elle-cli-0.1.9-standalone.jar`),
  model `list-append`.
- Each pass generates one SI and one SSI history of 24,000 Elle events
  (12,000 transaction attempts × invoke+complete), then runs `scripts/elle_check.sh`.
- Histories were written to a scratch dir (16–26 MB each, 144k events total) and
  are **not committed** — they are regenerated each run. This doc records the
  verdicts and generator stats; rerun the command to reproduce the `.edn`.

## Checker verdicts

Identical across all three passes (point / scan / predicate):

| Assertion | Result |
|---|---|
| Fixture smoke: known-bad (write skew) **rejected** under `serializable` | ✅ |
| Fixture smoke: known-bad **accepted** under `snapshot-isolation` | ✅ |
| SI history satisfies `snapshot-isolation` (UltimaDB SI claim) | ✅ |
| SI history vs `serializable`: anomalies ⊆ {`G2-item`}, observed `G2-item` | ✅ write skew only |
| SSI history satisfies `serializable` | ✅ |
| SSI history has no `anomaly-types` | ✅ |

`elle consistency check passed` for point-read, table-scan, and predicate/index.

## Generator stats (per history, 24,000 events)

| Pass | Isolation | committed (`:ok`) | write-conflict | serialization-failure | read txns |
|---|---|--:|--:|--:|--:|
| point | SI  | 5,483 | 6,517 | 0     | — |
| point | SSI | 4,861 | 5,908 | 1,231 | — |
| scan  | SI  | 5,651 | 6,349 | 0     | 6,050 scan |
| scan  | SSI | 4,455 | 5,320 | 2,225 | 6,050 scan |
| pred  | SI  | 5,837 | 6,163 | 0     | 6,081 predicate |
| pred  | SSI | 4,533 | 5,189 | 2,278 | 6,081 predicate |

Notes:
- Under SI, `serialization_failure = 0` in every pass — SI never aborts for
  read-set conflicts; the only aborts are key-level write conflicts. Under SSI,
  the extra aborts (1.2k–2.3k) are exactly the SSI read-set (write-skew)
  guard doing its job, and the committed count drops accordingly.
- Contention is deliberately high (write-conflict aborts outnumber commits) so
  the SI history reliably exhibits write skew — a run with no `G2-item` would
  `WARN`, not silently pass. All three SI passes exhibited it.

## Reproduce

```bash
make consistency/elle                       # default sizing (this run), needs java + jq
make consistency/elle ELLE_DIR=./elle-out   # keep the .edn histories somewhere non-ephemeral
make consistency/elle-mutation              # teeth-test: inject commit-path bugs, confirm Elle catches them
```

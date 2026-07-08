# bench-infra — standalone AWS NVMe bench rig for ultima_db

Provisions **one** AWS local-NVMe host, installs the toolchain, OS-tunes it,
rsyncs the local `ultima_db` working tree, runs a selected benchmark workload on
NVMe, and pulls results to `bench-out/dist/<ts>/`. AWS-only, single-node,
decoupled from `ultima_cluster`. Design:
`docs/superpowers/specs/2026-07-08-bench-infra-carveout-design.md`.

## Control-machine setup

Tools needed on the machine that *runs* the rig (not the provisioned host):

| Tool | Min version | Used by |
|------|-------------|---------|
| `terraform` | >= 1.6 | `make init/up/destroy/status` |
| `ansible` (ansible-core) | >= 2.16 | `make up/bench/*` |
| collections `ansible.posix`, `community.general` | latest | `sysctl`, `synchronize` (rsync) |
| `jq` | any | inventory generator + `make status/ssh` |
| `rsync` | any | source push + result pull |
| an SSH keypair | — | host access (path in `terraform.tfvars`) |

```bash
ansible-galaxy collection install ansible.posix community.general
```

## Credentials

Put AWS creds in a gitignored `.env` (auto-loaded by the Makefile), or use
`AWS_PROFILE` / the standard provider chain:

    cp .env.example .env   # then fill in, or leave empty and use AWS_PROFILE
    # .env: bare KEY=value, e.g.  AWS_PROFILE=ultimadb-bench

Use a scoped IAM principal, never root account keys.

## Quickstart

    cp example.tfvars terraform.tfvars   # edit ssh key + allow_ssh_cidr (your IP/32)
    make init
    make up                 # provision + configure (~cold build: several min)
    make bench/competitor   # or bench/wal-ab, or bench/autobench
    make destroy            # ALWAYS tear down — nothing auto-reaps

One-shot: `make bench-oneshot TARGET=competitor` (up → bench → destroy).
Persistent: `make up` once, `make bench/*` repeatedly, `make ssh` to poke,
`make destroy` when done. `make status` lists the host + uptime (cost guard).

## Workloads (`bench_target`)

| Target | Runs | Notes |
|--------|------|-------|
| `competitor` | `make bench/ycsb/compare` | UltimaDB vs RocksDB/Fjall/ReDB, both durability tiers. Reproduces `docs/benchmarks/competitor-nvme-2026-06-26.md`. |
| `wal-ab` | `make bench/wal-ab` | ultima-only: nondurable vs consistent vs inline vs standalone_fast. |
| `autobench` | `make perf/baseline` | Gate-A microbenches (smr-apply + mw-commit). **Gate B is NOT run** — it needs `ultima_cluster` and is a local-only step. |

## Notes

- **Same-host relative only.** Absolute numbers are one-host; compare engine
  ordering + ratios, not across machines.
- **NVMe is ephemeral** (instance store) — destroyed with the instance. Fine for
  benches, never for data you want to keep.
- **rocksdb-sys builds from source** on first `bench/competitor` (cold, minutes).
- **Cost guard:** nothing auto-reaps; `ttl_hours` is only a tag. Run `make destroy`.

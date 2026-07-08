# bench-infra carve-out — standalone AWS NVMe bench rig for ultima_db

**Date:** 2026-07-08
**Status:** Design approved; implementation pending.
**Scope:** A new top-level `bench-infra/` in `ultima_db`, AWS-only, single-node,
carved from `../ultima_cluster/bench-infra` with all cluster / Aeron / networking /
multi-cloud machinery stripped. Makes `ultima_db`'s cloud benchmarking reproducible
without depending on the sibling repo.

## Problem

`ultima_db` has no in-repo cloud provisioning. Its cloud A/B captures (e.g.
`docs/benchmarks/competitor-nvme-2026-06-26.md`) were produced by borrowing
`../ultima_cluster/bench-infra` ad hoc: spin up a node, rsync the source, run
`make bench/...` by hand, `make destroy`. That rig is purpose-built for
cluster/Aeron parity benchmarking (3-host fleets; `build_uc` / `build_aeron` /
`build_aerongo` / `gomatch` / `netping_serve` roles; cross-host UDP/QUIC ping
harnesses) and is slated to relocate with `ultima_cluster`. Coupling `ultima_db`'s
bench reproducibility to it is fragile.

The *local* perf infra is already standalone (`autobench/` + `make perf/check`).
The only borrowed piece is **cloud NVMe provisioning** — spinning up a real
local-NVMe box for competitor / WAL-durability benches.

## Goals

- A self-contained `ultima_db/bench-infra/` that provisions one AWS NVMe node,
  installs the toolchain, OS-tunes it, syncs the local `ultima_db` tree, runs a
  selected benchmark workload on local NVMe, and pulls results back — then tears
  down.
- Zero dependency on `ultima_cluster` (no Gate B, no Aeron, no UC).
- Reproduce the proven `competitor-nvme-2026-06-26` capture exactly (same
  instance class, same `make bench/ycsb/compare` matrix).

## Non-goals (YAGNI)

- Multi-cloud (AWS only). No Hetzner/GCP dispatch.
- Multi-node / cluster placement group / inter-node networking.
- Aeron / UC / gomatch / netping.
- autobench Gate B (`make perf/check`'s cross-repo build of `ultima_cluster`).
  The cloud rig runs Gate-A microbenches only; Gate B stays a local-only step.
- Auto-reaping. TTL stays an advisory tag + a `make status` cost guard, matching
  the parent rig.

## Decisions (resolved during brainstorming)

- **Approach:** lean carve-out (not a full copy, not continued borrowing).
- **Cloud:** AWS only.
- **Workloads:** competitor YCSB matrix + WAL/durability A/B + autobench (Gate A only).
- **autobench:** Gate A only, decoupled — sync only `ultima_db`, run the in-repo
  microbenches, skip cross-repo Gate B.
- **Instance default:** `c6id.2xlarge` (8 vCPU + local NVMe instance store) — the
  exact class used by `competitor-nvme-2026-06-26`, not the parent's `c6id.4xlarge`.
- **Source sync:** rsync the local working tree (respects `.gitignore`, excludes
  `target/`), so a branch / uncommitted state can be benched — as the competitor
  capture did (`main @ 04c27b3` + a bench toggle).

## Layout

```
bench-infra/
  Makefile              # init / up / bench/* / status / ssh / destroy  (.env autoload)
  README.md
  .env.example          # AWS creds template; real .env is gitignored
  example.tfvars        # ssh key + allow_ssh_cidr + instance_type template
  .gitignore
  terraform/            # AWS-only root — single provider, no module dispatch
    main.tf
    variables.tf
    outputs.tf
    versions.tf
  inventory/
    terraform_to_inventory.sh
    .gitkeep
  ansible/
    ansible.cfg
    group_vars/all.yml  # remote_home, bench_dir, bench matrix params
    provision.yml       # toolchains + os_tune
    bench.yml           # run + collect (parametrized by bench_target)
    roles/
      toolchains/
      os_tune/
      run/
      collect/
```

## Component design

### 1. Terraform — single AWS NVMe node

Inline the parent's `terraform/modules/aws` as the root config; drop the
hetzner/gcp providers, the `cloud` dispatch var, and the `count`/module
indirection.

- **Keep:** the AMI lookup (Ubuntu 24.04 noble), VPC/subnet/IGW/route-table,
  security group (SSH from `allow_ssh_cidr` + intra-VPC), key pair import, and —
  critically — the `aws_ec2_instance_type_offerings` AZ lookup so the NVMe type
  lands in a supported AZ.
- **Drop:** `aws_placement_group` (cluster strategy is meaningless for one node).
- `node_count` default **1**. `instance_type` default **`c6id.2xlarge`**.
- **Variables:** `instance_type`, `region` (default `us-east-1`), `ssh_public_key`,
  `ssh_private_key_file`, `allow_ssh_cidr`, `ttl_hours` (advisory tag), `owner`
  (default `ultimadb-bench`).
- **Outputs:** `public_ip`, `private_ip`, `ssh_user` (`ubuntu`) — consumed by the
  inventory generator. A single node, exposed as a one-element `nodes` list so the
  inventory script and `make status` reuse the parent's `jq` shape.

### 2. Ansible

**`provision.yml`** runs `os_tune` **then** `toolchains`. Order matters:
`os_tune`'s `/etc/hosts` + `UseDNS` fix must precede any `apt`/`sudo` step (it
kills the ~62s sudo/getaddrinfo stall that would otherwise hit `toolchains`'
package installs), and the NVMe mount is established before the run role writes
benchmark data to `bench_dir`.

**`toolchains` role** — base build deps + rustup stable:
- apt: `git unzip curl build-essential pkg-config clang protobuf-compiler`
  (`clang` + `protobuf-compiler` are required by `rocksdb-sys` in the competitor
  tier; `build-essential`/`pkg-config` by the general build).
- **No JDK** (the parent installs it for Aeron/elle; neither runs here).
- rustup + stable toolchain (idempotent, `creates:` guard), cargo on PATH via
  `/etc/profile.d/cargo.sh`.
- Record `rustc --version` for provenance.

**`os_tune` role** — reused essentially verbatim from the parent:
- The `/etc/hosts` self-name mapping + `UseDNS no` sshd fix (kills the
  intermittent ~62s sudo/getaddrinfo stall). These run **first**.
- Install tuning tools (`tuned`, `util-linux`, `e2fsprogs`).
- Detect the local instance-store NVMe (device model string
  `Instance Storage`), `mkfs.ext4`, and mount it at `{{ bench_dir }}`
  (default `/mnt/nvme/bench`). Idempotent; a no-op on non-NVMe hosts.
- `bench_dir` is exported to benches as `ULTIMA_BENCH_DIR`.

**`run` role** (new — replaces the parent's Aeron sweep) — parametrized by a
`bench_target` var:
1. **Sync source:** `ansible.posix.synchronize` (rsync push) the local
   `ultima_db` working tree → `{{ remote_home }}/ultima_db`, `--delete`, excluding
   `target/`, `.git/`, `bench-out/`, `.claude/`. Honors the working-tree state so a
   branch can be benched.
2. **Dispatch on `bench_target`:**
   - `competitor` →
     `make bench/ycsb/compare ULTIMA_BENCH_DIR={{ bench_dir }}`
     (both durability tiers × ultima/fjall/rocksdb/redb; needs `critcmp` — install
     via `cargo install critcmp` in `toolchains` or a preflight task).
   - `wal-ab` → a **new** root-repo `make bench/wal-ab` target (see §4):
     the ultima-only `ycsb_bench` swept across durability × `ULTIMA_BENCH_INLINE` ×
     `ULTIMA_BENCH_PREALLOC`, on `ULTIMA_BENCH_DIR={{ bench_dir }}`.
   - `autobench` → `make perf/baseline` (the Gate-A microbenches:
     `smr-apply-microbench` + `mw-commit-microbench`, writing baselines). **No Gate B.**
3. **Provenance manifest** written to `results/manifest.txt`: timestamp, instance
   type + vCPU/mem, kernel, `rustc` version, git rev of the synced tree,
   `bench_target`, durability tiers.

**`collect` role** — pull `{{ remote_home }}/.../results/` (and the criterion
`target/criterion` baselines for the YCSB tiers) → `bench-out/dist/<ts>/` on the
control machine (reused, trimmed of the parent's cluster-specific manifest fields).

### 3. Inventory

`inventory/terraform_to_inventory.sh` — reused, simplified to emit a single
`node0` host from the terraform `nodes` output, writing the private key path from
`terraform.tfvars` into `inventory/hosts.yml` (gitignored).

### 4. Makefile (bench-infra) + one root-repo target

**`bench-infra/Makefile`** — `.env` autoload (the parent's `-include .env` +
strip-surrounding-quotes safety net for `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
/ `AWS_SESSION_TOKEN` / `AWS_PROFILE`). Targets:

| Target | Action |
|--------|--------|
| `init` | `terraform init` |
| `up` | `terraform apply` → `inventory` → `provision.yml` (toolchains + os_tune) |
| `bench/competitor` | `bench.yml -e bench_target=competitor` |
| `bench/wal-ab` | `bench.yml -e bench_target=wal-ab` |
| `bench/autobench` | `bench.yml -e bench_target=autobench` |
| `bench-oneshot` | `up` → `bench.yml -e bench_target=$(TARGET)` → `destroy` |
| `status` | list instance + uptime vs `ttl_hours` (cost guard) |
| `ssh` | ssh to node0 |
| `destroy` | `terraform destroy` + remove `inventory/hosts.yml` |

**Root `ultima_db/Makefile`** gains **one** new target, `bench/wal-ab`, composing
the WAL A/B sweep that `competitor-nvme-2026-06-26` ran by hand (ultima
`ycsb_bench` across `nondurable`/`strict` × `{inline,prealloc}` env toggles). This
keeps the WAL A/B reproducible locally, not only via the cloud rig. Added to the
root `.PHONY` line.

### 5. Hygiene & docs

- **`bench-infra/.gitignore`:** `.env`, `terraform/.terraform/`, `*.tfstate*`,
  `terraform.tfvars`, `inventory/hosts.yml`, `bench-out/`.
- **Committed:** `.env.example`, `example.tfvars`, all terraform + ansible source.
- **`bench-infra/README.md`:** control-machine setup (terraform ≥ 1.6, ansible-core
  ≥ 2.16, `ansible.posix`/`community.general`, `jq`, `rsync`, an SSH keypair),
  credentials via `.env`, quickstart (`init` → `up` → `bench/competitor` →
  `destroy`), and the same-host-relative caveat + cost guard.
- **Root `CLAUDE.md`:** one-line pointer under the bench-crates note — bench-infra
  now lives in-repo (AWS NVMe single-node), decoupled from `ultima_cluster`.
- **No `docs/tasks/taskXX`.** This is infra tooling, not a library feature; this
  spec + the README are canonical. (The CLAUDE.md per-feature-doc convention
  targets library features.)

## Data flow

```
control machine (laptop)                    AWS c6id.2xlarge (Ubuntu 24.04)
─────────────────────────                   ───────────────────────────────
make up ──► terraform apply ──────────────► VPC/subnet/SG/keypair + NVMe instance
         └► inventory (hosts.yml)
         └► provision.yml ────────────────► toolchains (rust+clang+protobuf)
                                            os_tune (sudo/DNS fix; mount NVMe @ /mnt/nvme/bench)
make bench/<target>
   └► run role: rsync ultima_db tree ─────► ~/ultima_db  (excl target/,.git/)
   └► run role: make bench/<target> ──────► cargo bench on NVMe (ULTIMA_BENCH_DIR)
                                            results/ + manifest.txt
   └► collect role: rsync pull ◄─────────── results/  →  bench-out/dist/<ts>/
make destroy ──► terraform destroy ───────► (all resources torn down)
```

## Testing / validation

Infra, so validation is operational rather than unit-tested:

1. `terraform validate` + `terraform plan` clean on the AWS-only root.
2. `ansible-playbook --syntax-check` on `provision.yml` and `bench.yml`;
   `ansible-lint` if available. `shellcheck` on `terraform_to_inventory.sh` and any
   sweep shell.
3. End-to-end smoke: `make bench-oneshot TARGET=autobench` (cheapest workload) —
   confirm a node comes up, NVMe mounts at `/mnt/nvme/bench`, the tree syncs,
   `make perf/baseline` runs, results land in `bench-out/dist/<ts>/`, `destroy`
   cleans up.
4. Reproduce fidelity: a `make bench/competitor` run should produce the same engine
   ordering as `docs/benchmarks/competitor-nvme-2026-06-26.md` (same-host-relative
   only; absolute numbers not portable across hosts).
5. Root `make bench/wal-ab` runs locally against a disk-backed `ULTIMA_BENCH_DIR`
   (refuses tmpfs, like `bench/ycsb/compare`).

## Risks / open notes

- **`critcmp` dependency:** `make bench/ycsb/compare` needs `critcmp` on PATH.
  Install it in the `toolchains` role (`cargo install critcmp`) so the competitor
  target doesn't fail its preflight `check_cmd`.
- **Cost guard:** nothing auto-reaps. `ttl_hours` is a tag; `make status` warns.
  README must make `make destroy` prominent.
- **rocksdb-sys build time:** the competitor tier compiles RocksDB from source
  (cold build ~minutes). Acceptable for a one-shot cloud run; noted in the README.
- **Single-AZ / instance-store:** results are same-host-relative only; the NVMe is
  ephemeral (destroyed with the instance) — fine for benches, never for durable data.

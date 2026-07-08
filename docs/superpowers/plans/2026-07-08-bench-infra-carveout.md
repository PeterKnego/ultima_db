# bench-infra Carve-Out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A self-contained `ultima_db/bench-infra/` that provisions one AWS local-NVMe node, tunes it, syncs the local `ultima_db` working tree, runs a selected benchmark workload on NVMe, pulls results back, and tears down — with zero dependency on `ultima_cluster`.

**Architecture:** Terraform (AWS-only, single node) provisions VPC + NVMe instance; Ansible `provision.yml` runs `os_tune` (sudo/DNS fix + NVMe mount at `/opt/bench`) then `toolchains` (rust + rocksdb build deps + critcmp); `bench.yml` runs a `run` role (rsync source → `make bench/<target>`) then a `collect` role (pull results to `bench-out/dist/<ts>/`). A single new root-repo `make bench/wal-ab` target composes the WAL A/B sweep.

**Tech Stack:** Terraform ≥ 1.6 (hashicorp/aws ~> 5.0), Ansible-core ≥ 2.16 (`ansible.posix`, `community.general`), GNU Make, bash, rustup/cargo on the provisioned host.

## Global Constraints

- **AWS only.** No Hetzner/GCP provider, no `cloud` dispatch var. Single provider block.
- **Single node.** `node_count = 1`; no cluster placement group; no intra-cluster SG rule.
- **Default instance:** `c6id.2xlarge` (8 vCPU + local NVMe instance store), region `us-east-1`. Both overridable.
- **No `ultima_cluster` dependency.** No Aeron/UC/gomatch/netping; autobench runs Gate A only (`make perf/baseline`), never Gate B.
- **Source sync = rsync the local working tree** (respects excludes; benches a branch/uncommitted state).
- **On-NVMe layout:** NVMe mounts at `remote_home=/opt/bench`; `CARGO_HOME=/opt/bench/.cargo`, source at `/opt/bench/ultima_db`, `ULTIMA_BENCH_DIR=/opt/bench/bench-data` — all on NVMe.
- **All work on branch `feat/bench-infra-carveout`** (already created; the design spec is committed there).
- **`ycsb_bench` compiles with default features** — never pass `--features persistence` to it (verified: `src/lib.rs:40,60` are ungated).
- **Match existing Makefile style:** tab-indented recipes, `ULTIMA_BENCH_DIR` guard refuses empty/tmpfs (copy the pattern from `bench/ycsb/compare`).

---

### Task 1: Scaffold `bench-infra/` hygiene + templates

**Files:**
- Create: `bench-infra/.gitignore`
- Create: `bench-infra/.env.example`
- Create: `bench-infra/example.tfvars`
- Create: `bench-infra/inventory/.gitkeep`

**Interfaces:**
- Produces: the `bench-infra/` root, gitignore rules, and the AWS creds + tfvars templates every later task assumes. Committed files: `.gitignore`, `.env.example`, `example.tfvars`, `inventory/.gitkeep`.

- [ ] **Step 1: Create `bench-infra/.gitignore`**

```gitignore
# Terraform
terraform/.terraform/
terraform/.terraform.lock.hcl
*.tfstate
*.tfstate.*
crash.log

# Local config (copy from the *.example templates)
terraform.tfvars
.env

# Generated inventory + keys
inventory/hosts.yml
*.pem

# Ansible
ansible/*.retry

# Collected results
bench-out/
```

- [ ] **Step 2: Create `bench-infra/.env.example`**

```bash
# AWS credentials for bench-infra. Copy to `.env` (gitignored) and fill in,
# OR just use AWS_PROFILE / the standard provider chain and leave this empty.
# The Makefile auto-loads .env and exports these into terraform/ansible.
# Use bare KEY=value (no spaces around '='); surrounding quotes are stripped
# for the cred vars but bare is preferred.

# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# AWS_SESSION_TOKEN=...
# AWS_PROFILE=ultimadb-bench
```

- [ ] **Step 3: Create `bench-infra/example.tfvars`**

```hcl
# Copy to terraform.tfvars (gitignored) and edit. Credentials are NOT here —
# set AWS_PROFILE or AWS_ACCESS_KEY_ID/SECRET in .env (see .env.example).
# Do NOT use root account keys; use a scoped IAM principal.

instance_type        = "c6id.2xlarge"  # 8 vCPU + local NVMe (matches competitor-nvme-2026-06-26)
region               = "us-east-1"
ssh_public_key       = "ssh-ed25519 AAAA... you@host"
ssh_private_key_file = "~/.ssh/id_ed25519"
allow_ssh_cidr       = "203.0.113.4/32" # your IP/32 — NOT 0.0.0.0/0
ttl_hours            = 4                 # advisory tag only; nothing auto-reaps
owner                = "ultimadb-bench"
```

- [ ] **Step 4: Create `bench-infra/inventory/.gitkeep`** (empty file — keeps the dir tracked; `hosts.yml` is generated + gitignored)

```
```

- [ ] **Step 5: Verify gitignore behavior**

Run: `cd /home/claude/ultima/ultima_db && git add -A bench-infra && git status -s bench-infra`
Expected: `.gitignore`, `.env.example`, `example.tfvars`, `inventory/.gitkeep` are staged; no `terraform.tfvars`/`.env`/`hosts.yml` present.

- [ ] **Step 6: Commit**

```bash
git add bench-infra/.gitignore bench-infra/.env.example bench-infra/example.tfvars bench-infra/inventory/.gitkeep
git commit -m "feat(bench-infra): scaffold dir + gitignore + env/tfvars templates"
```

---

### Task 2: Terraform — AWS-only single-node root

**Files:**
- Create: `bench-infra/terraform/versions.tf`
- Create: `bench-infra/terraform/variables.tf`
- Create: `bench-infra/terraform/main.tf`
- Create: `bench-infra/terraform/outputs.tf`

**Interfaces:**
- Consumes: `terraform.tfvars` values (Task 1 template).
- Produces: outputs `nodes` (list of `{name, role, public_ip, private_ip}`) and `ssh_user` (`"ubuntu"`) — consumed by the inventory generator (Task 3) and the Makefile (Task 8). Resource `aws_instance.node` (count 1), NVMe-bearing.

- [ ] **Step 1: Create `bench-infra/terraform/versions.tf`**

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
```

- [ ] **Step 2: Create `bench-infra/terraform/variables.tf`**

```hcl
variable "instance_type" {
  description = "EC2 instance type. Default is a local-NVMe class matching competitor-nvme-2026-06-26."
  type        = string
  default     = "c6id.2xlarge"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "ssh_public_key" {
  description = "SSH public key contents to install on the host."
  type        = string
}

variable "ssh_private_key_file" {
  description = "Path to the matching private key, written into the Ansible inventory by the Makefile."
  type        = string
}

variable "allow_ssh_cidr" {
  description = "CIDR allowed to SSH (e.g. your IP/32). Do NOT use 0.0.0.0/0."
  type        = string
}

variable "ttl_hours" {
  description = "Advisory TTL tag for the cost guard. Nothing auto-reaps."
  type        = number
  default     = 4
}

variable "owner" {
  description = "Owner tag prefix for resources."
  type        = string
  default     = "ultimadb-bench"
}
```

- [ ] **Step 3: Create `bench-infra/terraform/main.tf`**

```hcl
provider "aws" {
  region = var.region
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

resource "aws_vpc" "bench" {
  cidr_block           = "10.10.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "${var.owner}-vpc", owner = var.owner }
}

# Pick an AZ that actually offers the requested type — larger NVMe types are
# not in every AZ, and the wrong AZ yields a RunInstances "Unsupported" 400.
data "aws_ec2_instance_type_offerings" "supported_az" {
  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
  location_type = "availability-zone"
}

resource "aws_subnet" "bench" {
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = "10.10.1.0/24"
  map_public_ip_on_launch = true
  availability_zone       = sort(data.aws_ec2_instance_type_offerings.supported_az.locations)[0]
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }
}

resource "aws_route_table_association" "bench" {
  subnet_id      = aws_subnet.bench.id
  route_table_id = aws_route_table.bench.id
}

resource "aws_security_group" "bench" {
  name   = "${var.owner}-sg"
  vpc_id = aws_vpc.bench.id
  ingress {
    description = "ssh"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allow_ssh_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "bench" {
  key_name   = "${var.owner}-key"
  public_key = var.ssh_public_key
}

resource "aws_instance" "node" {
  count                  = 1
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.bench.id
  vpc_security_group_ids = [aws_security_group.bench.id]
  key_name               = aws_key_pair.bench.key_name
  private_ip             = "10.10.1.${count.index + 10}"
  tags = {
    Name      = "${var.owner}-node${count.index}"
    owner     = var.owner
    ttl_hours = tostring(var.ttl_hours)
    role      = "node${count.index}"
  }
}
```

- [ ] **Step 4: Create `bench-infra/terraform/outputs.tf`**

```hcl
output "nodes" {
  value = [
    for i, s in aws_instance.node : {
      name       = s.tags["Name"]
      role       = "node${i}"
      public_ip  = s.public_ip
      private_ip = s.private_ip
    }
  ]
}

output "ssh_user" {
  value = "ubuntu"
}
```

- [ ] **Step 5: Validate terraform (offline — no AWS creds needed)**

Run: `cd /home/claude/ultima/ultima_db/bench-infra/terraform && terraform init -backend=false && terraform validate`
Expected: `Success! The configuration is valid.` (If `terraform` is not installed, install per the README control-machine steps; the plan assumes it is available on the control machine.)

- [ ] **Step 6: Format check**

Run: `terraform fmt -check -diff /home/claude/ultima/ultima_db/bench-infra/terraform`
Expected: no diff (files already canonical). If it reports changes, run `terraform fmt` and re-stage.

- [ ] **Step 7: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add bench-infra/terraform
git commit -m "feat(bench-infra): AWS-only single-NVMe-node terraform root"
```

---

### Task 3: Inventory generator

**Files:**
- Create: `bench-infra/inventory/terraform_to_inventory.sh`

**Interfaces:**
- Consumes: `terraform output -json` (`nodes`, `ssh_user` from Task 2); env `SSH_PRIVATE_KEY_FILE`.
- Produces: `bench-infra/inventory/hosts.yml` (gitignored) with groups `cluster` (all nodes) and `node0`. Consumed by `ansible.cfg` (Task 4).

- [ ] **Step 1: Create `bench-infra/inventory/terraform_to_inventory.sh`**

```bash
#!/usr/bin/env bash
# Turn `terraform output -json` into an Ansible inventory at inventory/hosts.yml.
# Groups: [cluster] = all nodes; [node0] = the single bench host.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="${HERE}/../terraform"
OUT="${HERE}/hosts.yml"
SSH_KEY="${SSH_PRIVATE_KEY_FILE:?set SSH_PRIVATE_KEY_FILE to the private key path}"

json="$(cd "$TF_DIR" && terraform output -json)"
ssh_user="$(echo "$json" | jq -r '.ssh_user.value')"

{
  echo "all:"
  echo "  vars:"
  echo "    ansible_user: ${ssh_user}"
  echo "    ansible_ssh_private_key_file: ${SSH_KEY}"
  echo "    ansible_ssh_common_args: '-o StrictHostKeyChecking=accept-new'"
  echo "  children:"
  echo "    cluster:"
  echo "      hosts:"
  echo "$json" | jq -r '.nodes.value[] |
    "        \(.name):\n          ansible_host: \(.public_ip)\n          private_ip: \(.private_ip)\n          node_role: \(.role)"'
  echo "    node0:"
  echo "      hosts:"
  echo "$json" | jq -r '.nodes.value[] | select(.role=="node0") | "        \(.name): {}"'
} > "$OUT"

echo "wrote $OUT"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x /home/claude/ultima/ultima_db/bench-infra/inventory/terraform_to_inventory.sh`

- [ ] **Step 3: Lint with shellcheck**

Run: `shellcheck /home/claude/ultima/ultima_db/bench-infra/inventory/terraform_to_inventory.sh`
Expected: no warnings. (If `shellcheck` is unavailable, `bash -n` the file instead: `bash -n .../terraform_to_inventory.sh` → no output.)

- [ ] **Step 4: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add bench-infra/inventory/terraform_to_inventory.sh
git commit -m "feat(bench-infra): terraform -> ansible inventory generator"
```

---

### Task 4: Ansible base (cfg + group_vars + provision.yml) + `os_tune` role

**Files:**
- Create: `bench-infra/ansible/ansible.cfg`
- Create: `bench-infra/ansible/group_vars/all.yml`
- Create: `bench-infra/ansible/provision.yml`
- Create: `bench-infra/ansible/roles/os_tune/tasks/main.yml`
- Create: `bench-infra/ansible/roles/os_tune/handlers/main.yml`

**Interfaces:**
- Consumes: `inventory/hosts.yml` (Task 3).
- Produces: group vars `remote_home=/opt/bench`, `bench_dir=/opt/bench/bench-data`, `ud_src_dir=/opt/bench/ultima_db`, `ud_local_path`, `results_local_dir`, `bench_target` (default `competitor`). `provision.yml` playbook running `os_tune`. NVMe mounted at `remote_home`.

- [ ] **Step 1: Create `bench-infra/ansible/ansible.cfg`**

```ini
[defaults]
inventory = ../inventory/hosts.yml
host_key_checking = False
roles_path = roles
stdout_callback = default
result_format = yaml
forks = 2
# Wide privilege-escalation timeout — a cold cloud host's sudo/sshd can stall
# ~62s on reverse-DNS of its own name before os_tune's fix lands.
timeout = 120

[ssh_connection]
pipelining = True
ssh_args = -o ControlMaster=auto -o ControlPersist=300s -o ServerAliveInterval=15 -o ServerAliveCountMax=6 -o TCPKeepAlive=yes -o PreferredAuthentications=publickey
retries = 3
```

- [ ] **Step 2: Create `bench-infra/ansible/group_vars/all.yml`**

```yaml
# --- Layout on the provisioned host (all on the NVMe mount) ---
remote_home: "/opt/bench"                 # NVMe instance store is mounted here by os_tune
bench_dir: "/opt/bench/bench-data"        # ULTIMA_BENCH_DIR — on NVMe
ud_src_dir: "/opt/bench/ultima_db"        # rsync target for the local ultima_db tree

# --- Source sync (rsync the local working tree, so a branch / uncommitted state benches) ---
# playbook_dir = bench-infra/ansible ; ../../ = ultima_db repo root
ud_local_path: "{{ playbook_dir }}/../../"

# --- Which workload the run role executes: competitor | wal-ab | autobench ---
bench_target: "competitor"

# --- Results pulled back to the control machine (bench-infra/bench-out/dist/<ts>/) ---
results_local_dir: "{{ playbook_dir }}/../bench-out/dist"
```

- [ ] **Step 3: Create `bench-infra/ansible/provision.yml`** (os_tune only — toolchains is added in Task 5)

```yaml
---
- name: Provision the bench host (OS tune + toolchain)
  hosts: cluster
  become: true
  # Defer fact-gathering until AFTER wait_for_connection so a cold host's sshd
  # is not raced by the implicit gather.
  gather_facts: false
  pre_tasks:
    - name: Wait for SSH to come up
      ansible.builtin.wait_for_connection:
        delay: 10
        timeout: 180
    - name: Gather facts (after SSH is reachable)
      ansible.builtin.setup:
  roles:
    - os_tune
```

- [ ] **Step 4: Create `bench-infra/ansible/roles/os_tune/tasks/main.yml`**

```yaml
---
# --- Fix the intermittent ~62s sudo/sshd getaddrinfo stall (cloud hosts whose
# own name is not in /etc/hosts + slow reverse-DNS). Idempotent; persists on
# disk. Placed FIRST so the rest of provisioning runs against fast sudo. ---
- name: Map the host's own name to a local address (kills sudo getaddrinfo stall)
  ansible.builtin.lineinfile:
    path: /etc/hosts
    line: "127.0.1.1 {{ ansible_fqdn }} {{ ansible_hostname }}"
    regexp: '^127\.0\.1\.1\s'
    state: present
    create: false

- name: Disable sshd reverse-DNS lookups (UseDNS no — avoids connect-time stall)
  ansible.builtin.lineinfile:
    path: /etc/ssh/sshd_config
    line: "UseDNS no"
    regexp: '^\s*#?\s*UseDNS\b'
    state: present
    validate: "/usr/sbin/sshd -t -f %s"
  notify: Reload sshd

- name: Install tuning tools
  ansible.builtin.apt:
    name: [linux-tools-common, util-linux, tuned, e2fsprogs]
    update_cache: true
    state: present
  failed_when: false   # linux-tools-common naming varies; non-fatal

# --- Local NVMe instance store: detect the "Instance Storage" device, format
# ext4, mount at remote_home so source, cargo target, AND bench data all land
# on local NVMe. No-op on EBS-only hosts. ---
- name: Detect local instance-store NVMe device
  ansible.builtin.shell: |
    set -e
    for d in /sys/block/nvme*; do
      [ -e "$d/device/model" ] || continue
      if grep -qi 'Instance Storage' "$d/device/model"; then
        dev="/dev/$(basename "$d")"
        if [ -z "$(lsblk -nro MOUNTPOINT "$dev" | tr -d '[:space:]')" ]; then
          echo "$dev"; exit 0
        fi
      fi
    done
    exit 0   # none found -> empty stdout
  register: nvme_detect
  changed_when: false

- name: Mount the instance-store NVMe at remote_home
  when: nvme_detect.stdout | trim | length > 0
  block:
    - name: Ensure mount point exists
      ansible.builtin.file:
        path: "{{ remote_home }}"
        state: directory
        mode: "0755"

    - name: Format ext4 + mount (idempotent — skips if already mounted)
      ansible.builtin.shell: |
        set -e
        DEV="{{ nvme_detect.stdout | trim }}"
        if mountpoint -q "{{ remote_home }}"; then
          echo "already-mounted"; exit 0
        fi
        mkfs.ext4 -F -q "$DEV"
        mount -o noatime "$DEV" "{{ remote_home }}"
        echo "mounted $DEV at {{ remote_home }}"
      register: nvme_mount
      changed_when: "'mounted ' in nvme_mount.stdout"

    - name: Report NVMe mount status
      ansible.builtin.debug:
        msg: "instance-store NVMe: {{ nvme_mount.stdout | trim }}"

- name: Ensure remote_home exists (EBS-only fallback when no NVMe was mounted)
  ansible.builtin.file:
    path: "{{ remote_home }}"
    state: directory
    mode: "0755"

- name: Set CPU governor to performance
  ansible.builtin.shell: |
    if command -v cpupower >/dev/null; then cpupower frequency-set -g performance || true; fi
    for c in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
      [ -w "$c" ] && echo performance > "$c" || true
    done
  changed_when: false

- name: Disable transparent hugepages
  ansible.builtin.shell: |
    echo never > /sys/kernel/mm/transparent_hugepage/enabled || true
    echo never > /sys/kernel/mm/transparent_hugepage/defrag || true
  changed_when: false

- name: Apply low-latency sysctls
  ansible.posix.sysctl:
    name: "{{ item.k }}"
    value: "{{ item.v }}"
    sysctl_set: true
    reload: true
  loop:
    - { k: "vm.swappiness", v: "0" }

- name: Raise open-file limits
  ansible.builtin.copy:
    dest: /etc/security/limits.d/99-bench.conf
    content: |
      * soft nofile 1048576
      * hard nofile 1048576
    mode: "0644"

- name: Apply tuned latency profile (best effort)
  ansible.builtin.command: tuned-adm profile latency-performance
  changed_when: false
  failed_when: false
```

- [ ] **Step 5: Create `bench-infra/ansible/roles/os_tune/handlers/main.yml`**

```yaml
---
# Reload (not restart) sshd so UseDNS takes effect WITHOUT dropping the live
# control connection. failed_when:false — the unit name varies (ssh vs sshd)
# and a reload failure must not abort a provision whose real work succeeded.
- name: Reload sshd
  ansible.builtin.service:
    name: "{{ item }}"
    state: reloaded
  loop: [ssh, sshd]
  failed_when: false
```

- [ ] **Step 6: Syntax-check the playbook**

Run: `cd /home/claude/ultima/ultima_db/bench-infra/ansible && ansible-playbook --syntax-check provision.yml`
Expected: `playbook: provision.yml` with no errors. (Requires `ansible.posix` collection installed for the `sysctl` module resolution; install with `ansible-galaxy collection install ansible.posix community.general` if the check errors on the module.)

- [ ] **Step 7: Lint (best effort)**

Run: `ansible-lint /home/claude/ultima/ultima_db/bench-infra/ansible/provision.yml || true`
Expected: no *errors* (warnings acceptable). Skip if `ansible-lint` is not installed.

- [ ] **Step 8: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add bench-infra/ansible/ansible.cfg bench-infra/ansible/group_vars bench-infra/ansible/provision.yml bench-infra/ansible/roles/os_tune
git commit -m "feat(bench-infra): ansible base + os_tune role (sudo/DNS fix + NVMe mount)"
```

---

### Task 5: `toolchains` role

**Files:**
- Create: `bench-infra/ansible/roles/toolchains/tasks/main.yml`
- Modify: `bench-infra/ansible/provision.yml` (append `toolchains` to `roles`)

**Interfaces:**
- Consumes: `remote_home` (Task 4 group_vars).
- Produces: `CARGO_HOME=/opt/bench/.cargo`, `rustc`/`cargo`/`critcmp` on PATH via `/etc/profile.d/cargo.sh`. No JDK.

- [ ] **Step 1: Create `bench-infra/ansible/roles/toolchains/tasks/main.yml`**

```yaml
---
- name: Install base build dependencies
  ansible.builtin.apt:
    name:
      - git
      - unzip
      - curl
      - build-essential
      - pkg-config
      - cmake
      - clang
      - libclang-dev   # rocksdb-sys bindgen needs libclang
    update_cache: true
    state: present

- name: Install rustup + stable toolchain (idempotent)
  ansible.builtin.shell: |
    if ! [ -x "{{ remote_home }}/.cargo/bin/cargo" ]; then
      curl -sSf https://sh.rustup.rs | CARGO_HOME={{ remote_home }}/.cargo RUSTUP_HOME={{ remote_home }}/.rustup sh -s -- -y --default-toolchain stable --profile minimal
    fi
  args:
    creates: "{{ remote_home }}/.cargo/bin/cargo"

- name: Export cargo on PATH for all shells
  ansible.builtin.copy:
    dest: /etc/profile.d/cargo.sh
    content: "export CARGO_HOME={{ remote_home }}/.cargo\nexport RUSTUP_HOME={{ remote_home }}/.rustup\nexport PATH={{ remote_home }}/.cargo/bin:$PATH\n"
    mode: "0644"

- name: Install critcmp (required by `make bench/ycsb/compare`)
  ansible.builtin.command: "{{ remote_home }}/.cargo/bin/cargo install critcmp"
  args:
    creates: "{{ remote_home }}/.cargo/bin/critcmp"
  environment:
    CARGO_HOME: "{{ remote_home }}/.cargo"
    RUSTUP_HOME: "{{ remote_home }}/.rustup"

- name: Record rustc version (provenance)
  ansible.builtin.command: "{{ remote_home }}/.cargo/bin/rustc --version"
  register: rustc_version
  changed_when: false

- name: Show toolchain version
  ansible.builtin.debug:
    var: rustc_version.stdout
```

- [ ] **Step 2: Append `toolchains` to `provision.yml` roles**

In `bench-infra/ansible/provision.yml`, change the `roles:` block from:

```yaml
  roles:
    - os_tune
```

to:

```yaml
  roles:
    - os_tune
    - toolchains
```

- [ ] **Step 3: Syntax-check**

Run: `cd /home/claude/ultima/ultima_db/bench-infra/ansible && ansible-playbook --syntax-check provision.yml`
Expected: no errors; both roles resolve.

- [ ] **Step 4: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add bench-infra/ansible/roles/toolchains bench-infra/ansible/provision.yml
git commit -m "feat(bench-infra): toolchains role (rust + rocksdb deps + critcmp, no JDK)"
```

---

### Task 6: `run` role + `bench.yml`

**Files:**
- Create: `bench-infra/ansible/roles/run/tasks/main.yml`
- Create: `bench-infra/ansible/bench.yml`

**Interfaces:**
- Consumes: `ud_local_path`, `ud_src_dir`, `bench_dir`, `remote_home`, `bench_target` (Task 4); `/etc/profile.d/cargo.sh` (Task 5); `make bench/ycsb/compare` (existing), `make bench/wal-ab` (Task 9), `make perf/baseline` (existing).
- Produces: `{{ remote_home }}/results/` on the host (logs + criterion output + manifest). `bench.yml` playbook running `run` (collect added in Task 7).

- [ ] **Step 1: Create `bench-infra/ansible/roles/run/tasks/main.yml`**

```yaml
---
- name: Ensure the NVMe bench data dir exists
  ansible.builtin.file:
    path: "{{ bench_dir }}"
    state: directory
    mode: "0755"

- name: Ensure results dir exists
  ansible.builtin.file:
    path: "{{ remote_home }}/results"
    state: directory
    mode: "0755"

- name: Sync the local ultima_db working tree to the host (push)
  ansible.posix.synchronize:
    src: "{{ ud_local_path }}/"
    dest: "{{ ud_src_dir }}/"
    delete: true
    rsync_opts:
      - "--exclude=target/"
      - "--exclude=.git/"
      - "--exclude=bench-out/"
      - "--exclude=bench-infra/bench-out/"
      - "--exclude=.claude/"

- name: Record rustc version (provenance)
  ansible.builtin.shell: "source /etc/profile.d/cargo.sh && rustc --version"
  args:
    executable: /bin/bash
  register: rustc_version
  changed_when: false

- name: Record git rev of the synced tree (best-effort)
  ansible.builtin.command: "git -C {{ ud_src_dir }} rev-parse --short HEAD"
  register: ud_git_rev
  changed_when: false
  failed_when: false

- name: Run competitor YCSB matrix
  when: bench_target == "competitor"
  ansible.builtin.shell: |
    set -euo pipefail
    source /etc/profile.d/cargo.sh
    cd {{ ud_src_dir }}
    make bench/ycsb/compare ULTIMA_BENCH_DIR={{ bench_dir }} 2>&1 | tee {{ remote_home }}/results/competitor.log
  args:
    executable: /bin/bash
  changed_when: true

- name: Run WAL/durability A/B sweep
  when: bench_target == "wal-ab"
  ansible.builtin.shell: |
    set -euo pipefail
    source /etc/profile.d/cargo.sh
    cd {{ ud_src_dir }}
    make bench/wal-ab ULTIMA_BENCH_DIR={{ bench_dir }} 2>&1 | tee {{ remote_home }}/results/wal-ab.log
  args:
    executable: /bin/bash
  changed_when: true

- name: Run autobench Gate-A microbenches (record baselines)
  when: bench_target == "autobench"
  ansible.builtin.shell: |
    set -euo pipefail
    source /etc/profile.d/cargo.sh
    cd {{ ud_src_dir }}
    make perf/baseline 2>&1 | tee {{ remote_home }}/results/autobench.log
    cp -r autobench/baselines {{ remote_home }}/results/autobench-baselines
  args:
    executable: /bin/bash
  changed_when: true

- name: Copy criterion output into results (competitor / wal-ab)
  when: bench_target in ["competitor", "wal-ab"]
  ansible.builtin.shell: |
    set -e
    if [ -d {{ ud_src_dir }}/target/criterion ]; then
      cp -r {{ ud_src_dir }}/target/criterion {{ remote_home }}/results/criterion
    fi
  changed_when: true

- name: Write provenance manifest
  ansible.builtin.copy:
    dest: "{{ remote_home }}/results/manifest.txt"
    mode: "0644"
    content: |
      timestamp={{ ansible_date_time.iso8601 }}
      bench_target={{ bench_target }}
      instance_vcpus={{ ansible_processor_vcpus | default('?') }}
      mem_mb={{ ansible_memtotal_mb | default('?') }}
      kernel={{ ansible_kernel }}
      rustc={{ rustc_version.stdout | default('n/a') }}
      ud_git_rev={{ ud_git_rev.stdout | default('n/a') }}
      bench_dir={{ bench_dir }}
```

- [ ] **Step 2: Create `bench-infra/ansible/bench.yml`** (run only — collect added in Task 7)

```yaml
---
- name: Run the selected benchmark workload and collect results
  hosts: cluster
  become: true
  gather_facts: false
  pre_tasks:
    - name: Wait for SSH
      ansible.builtin.wait_for_connection:
        delay: 2
        timeout: 120
    - name: Gather facts
      ansible.builtin.setup:
  roles:
    - run
```

- [ ] **Step 3: Syntax-check**

Run: `cd /home/claude/ultima/ultima_db/bench-infra/ansible && ansible-playbook --syntax-check bench.yml`
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add bench-infra/ansible/roles/run bench-infra/ansible/bench.yml
git commit -m "feat(bench-infra): run role (rsync source + bench_target dispatch + manifest)"
```

---

### Task 7: `collect` role

**Files:**
- Create: `bench-infra/ansible/roles/collect/tasks/main.yml`
- Modify: `bench-infra/ansible/bench.yml` (append `collect` to `roles`)

**Interfaces:**
- Consumes: `{{ remote_home }}/results/` (Task 6), `results_local_dir` (Task 4).
- Produces: `bench-infra/bench-out/dist/<ts>/` on the control machine.

- [ ] **Step 1: Create `bench-infra/ansible/roles/collect/tasks/main.yml`**

```yaml
---
- name: Compute run timestamp (once)
  ansible.builtin.command: date +%Y%m%dT%H%M%SZ
  register: run_ts
  run_once: true
  changed_when: false

- name: Fetch results to local bench-out/dist/<ts>
  ansible.posix.synchronize:
    mode: pull
    src: "{{ remote_home }}/results/"
    dest: "{{ results_local_dir }}/{{ run_ts.stdout }}/"
    rsync_opts:
      - "--mkpath"   # create dest parent dirs (rsync only makes the leaf otherwise)
  changed_when: true

- name: Report collected location
  ansible.builtin.debug:
    msg: "Results pulled to {{ results_local_dir }}/{{ run_ts.stdout }}/"
  run_once: true
```

- [ ] **Step 2: Append `collect` to `bench.yml` roles**

Change the `roles:` block in `bench-infra/ansible/bench.yml` from:

```yaml
  roles:
    - run
```

to:

```yaml
  roles:
    - run
    - collect
```

- [ ] **Step 3: Syntax-check**

Run: `cd /home/claude/ultima/ultima_db/bench-infra/ansible && ansible-playbook --syntax-check bench.yml`
Expected: no errors; both roles resolve.

- [ ] **Step 4: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add bench-infra/ansible/roles/collect bench-infra/ansible/bench.yml
git commit -m "feat(bench-infra): collect role (pull results to bench-out/dist/<ts>)"
```

---

### Task 8: `bench-infra/Makefile`

**Files:**
- Create: `bench-infra/Makefile`

**Interfaces:**
- Consumes: `terraform.tfvars` (`ssh_private_key_file`), `.env` (AWS creds), terraform outputs, the ansible playbooks.
- Produces: `make init/up/inventory/bench{competitor,wal-ab,autobench}/bench-oneshot/status/ssh/destroy`.

- [ ] **Step 1: Create `bench-infra/Makefile`**

```makefile
# Load AWS credentials from a gitignored .env if present, and export them into
# every recipe so terraform/ansible pick them up. Leading '-' makes .env optional.
-include .env
# make's -include keeps surrounding quotes literally; strip them from cred vars
# so both KEY=abc and KEY="abc" work.
_ENV_CREDS := AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE
$(foreach v,$(_ENV_CREDS),$(eval $(v) := $$(patsubst "%",%,$$($(v)))))
export

TF      := terraform -chdir=terraform
TFVARS  ?= terraform.tfvars
ANSIBLE := ansible-playbook
SSH_KEY := $(shell awk -F'"' '/ssh_private_key_file/{print $$2}' $(TFVARS) 2>/dev/null)

.PHONY: init up inventory bench/competitor bench/wal-ab bench/autobench bench-oneshot status ssh destroy

init: ## terraform init
	$(TF) init

up: ## provision infra + configure (os_tune + toolchains)
	$(TF) apply -auto-approve -var-file=../$(TFVARS)
	$(MAKE) inventory
	cd ansible && SSH_PRIVATE_KEY_FILE=$(SSH_KEY) $(ANSIBLE) provision.yml

inventory:
	SSH_PRIVATE_KEY_FILE=$(SSH_KEY) ./inventory/terraform_to_inventory.sh

bench/competitor: ## competitor YCSB matrix (RocksDB/Fjall/ReDB) on NVMe
	cd ansible && $(ANSIBLE) bench.yml -e bench_target=competitor

bench/wal-ab: ## WAL/durability A/B sweep (ultima-only) on NVMe
	cd ansible && $(ANSIBLE) bench.yml -e bench_target=wal-ab

bench/autobench: ## autobench Gate-A microbenches + baseline record
	cd ansible && $(ANSIBLE) bench.yml -e bench_target=autobench

bench-oneshot: up ## up -> bench (TARGET=competitor|wal-ab|autobench) -> destroy
	cd ansible && $(ANSIBLE) bench.yml -e bench_target=$(TARGET)
	$(MAKE) destroy

status: ## list instance + uptime (cost guard — nothing auto-reaps)
	@$(TF) output -json nodes | jq -r '.[] | "\(.name)\t\(.public_ip)\t\(.role)"'
	@echo "TTL guard: nothing auto-reaps — run 'make destroy' when done."
	@cd ansible && ansible cluster -m shell -a "awk '{print int(\$$1/3600)\"h up\"}' /proc/uptime" 2>/dev/null || true

ssh: ## ssh to node0
	@ip=$$($(TF) output -json nodes | jq -r '.[]|select(.role=="node0").public_ip'); \
	 user=$$($(TF) output -raw ssh_user); ssh -i $(SSH_KEY) $$user@$$ip

destroy: ## tear everything down
	$(TF) destroy -auto-approve -var-file=../$(TFVARS)
	rm -f inventory/hosts.yml
```

- [ ] **Step 2: Verify Make parses the file and targets are declared**

Run: `cd /home/claude/ultima/ultima_db/bench-infra && make -n bench/competitor`
Expected: prints `cd ansible && ansible-playbook bench.yml -e bench_target=competitor` (no parse error). A "terraform: command not found" style error is fine here — the point is Make parses the recipe.

- [ ] **Step 3: Verify `.env` cred strip + `bench/wal-ab`/`bench/autobench` recipes**

Run: `cd /home/claude/ultima/ultima_db/bench-infra && make -n bench/wal-ab bench/autobench`
Expected: prints the two `ansible-playbook bench.yml -e bench_target=...` lines.

- [ ] **Step 4: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add bench-infra/Makefile
git commit -m "feat(bench-infra): Makefile (init/up/bench/status/ssh/destroy, .env autoload)"
```

---

### Task 9: Root-repo `make bench/wal-ab` target

**Files:**
- Modify: `Makefile` (root — add `bench/wal-ab` target + add it to `.PHONY`)

**Interfaces:**
- Consumes: `ULTIMA_BENCH_DIR` env; `critcmp`; `benches/ycsb_bench.rs` env toggles `ULTIMA_BENCH_DURABILITY`/`ULTIMA_BENCH_INLINE`/`ULTIMA_BENCH_PREALLOC`.
- Produces: root target `bench/wal-ab` (invoked by the bench-infra `run` role, Task 6).

- [ ] **Step 1: Add `bench/wal-ab` to the root `.PHONY` line**

In `Makefile` line 1, append ` bench/wal-ab` to the `.PHONY:` target list (after `bench/ycsb/compare`, before `bench/multiwriter`):

Change:
```makefile
... bench/ycsb/redb bench/ycsb/compare bench/multiwriter ...
```
to:
```makefile
... bench/ycsb/redb bench/ycsb/compare bench/wal-ab bench/multiwriter ...
```

- [ ] **Step 2: Add the `bench/wal-ab` recipe**

Insert this block in `Makefile` immediately after the `bench/ycsb/compare` recipe (after its closing `critcmp -g '(.+)/[^/]+' ultima_strict ...` line, before the `# Multi-writer contention benchmarks` comment):

```makefile
# WAL/durability A/B sweep (ultima-only): the standalone_fast toggles on real
# NVMe. Baselines: nondurable (Eventual), strict-consistent (bg-thread fsync),
# strict-inline (off-lock fsync), strict-standalone_fast (inline + prealloc).
# ycsb_bench reads ULTIMA_BENCH_{DURABILITY,INLINE,PREALLOC} (benches/ycsb_bench.rs).
# Requires ULTIMA_BENCH_DIR on a REAL disk (refuses tmpfs, like bench/ycsb/compare).
bench/wal-ab:
	$(call check_cmd,critcmp)
	@if [ -z "$(ULTIMA_BENCH_DIR)" ]; then \
	  echo "ERROR: ULTIMA_BENCH_DIR is not set — refusing to run."; \
	  echo "  Point it at a real disk-backed dir (NOT a tmpfs like /tmp):"; \
	  echo "    make bench/wal-ab ULTIMA_BENCH_DIR=\$$HOME/bench-disk"; \
	  exit 1; \
	fi
	@mkdir -p "$(ULTIMA_BENCH_DIR)"
	ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=nondurable \
	  cargo bench --bench ycsb_bench -- --save-baseline wal_nondurable
	ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=strict \
	  cargo bench --bench ycsb_bench -- --save-baseline wal_strict_consistent
	ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=strict ULTIMA_BENCH_INLINE=1 \
	  cargo bench --bench ycsb_bench -- --save-baseline wal_strict_inline
	ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=strict ULTIMA_BENCH_INLINE=1 ULTIMA_BENCH_PREALLOC=1 \
	  cargo bench --bench ycsb_bench -- --save-baseline wal_strict_standalone_fast
	@echo "===== WAL A/B (lower = better) ====="
	critcmp wal_nondurable wal_strict_consistent wal_strict_inline wal_strict_standalone_fast
```

- [ ] **Step 3: Verify the guard fires with no ULTIMA_BENCH_DIR**

Run: `cd /home/claude/ultima/ultima_db && make bench/wal-ab`
Expected: exits non-zero with `ERROR: ULTIMA_BENCH_DIR is not set — refusing to run.` (the `check_cmd,critcmp` runs first — if critcmp is absent you'll see its install hint instead; either is an acceptable early-exit).

- [ ] **Step 4: Verify the recipe compiles the bench (no feature flag needed)**

Run: `cd /home/claude/ultima/ultima_db && cargo bench --bench ycsb_bench --no-run`
Expected: compiles successfully (confirms `ycsb_bench` builds with default features — no `--features persistence`). This is a compile-only check; it does not run the sweep.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add Makefile
git commit -m "feat(bench): add bench/wal-ab durability A/B sweep target"
```

---

### Task 10: README + root CLAUDE.md pointer

**Files:**
- Create: `bench-infra/README.md`
- Modify: `CLAUDE.md` (one-line pointer in the bench-crates note)

**Interfaces:**
- Consumes: everything above (documents the operational flow).
- Produces: control-machine setup docs + quickstart + cost guard; a discoverability pointer from `CLAUDE.md`.

- [ ] **Step 1: Create `bench-infra/README.md`**

````markdown
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
````

- [ ] **Step 2: Add the CLAUDE.md pointer**

In `CLAUDE.md`, find the `**Bench crates**` bullet (under Architecture) and add a sentence at its end:

Find:
```markdown
- **Bench crates**: `bench_workloads` (shared YCSB/SmallBank generators, lib) and `compare_benches` (RocksDB/Fjall/ReDB baselines, opt-in tier — keeps their deps out of the root crate). First-party benches stay in `benches/`.
```

Change to:
```markdown
- **Bench crates**: `bench_workloads` (shared YCSB/SmallBank generators, lib) and `compare_benches` (RocksDB/Fjall/ReDB baselines, opt-in tier — keeps their deps out of the root crate). First-party benches stay in `benches/`. Cloud A/B provisioning lives in-repo at `bench-infra/` (AWS local-NVMe single node; terraform + ansible; targets `bench/competitor`, `bench/wal-ab`, `bench/autobench` Gate-A). Decoupled from `ultima_cluster`. See `docs/superpowers/specs/2026-07-08-bench-infra-carveout-design.md`.
```

- [ ] **Step 3: Verify markdown is well-formed**

Run: `ls -la /home/claude/ultima/ultima_db/bench-infra/README.md && grep -c '^#' /home/claude/ultima/ultima_db/bench-infra/README.md`
Expected: file exists; heading count > 0.

- [ ] **Step 4: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add bench-infra/README.md CLAUDE.md
git commit -m "docs(bench-infra): README + CLAUDE.md pointer"
```

---

## Self-Review

**1. Spec coverage:**
- Layout (spec §Layout) → Tasks 1–8 create every listed file. ✓
- Terraform AWS single-node, drop placement group, keep AZ lookup, `c6id.2xlarge`, `node_count=1` (spec §1) → Task 2. ✓
- toolchains rust+clang+protobuf, no JDK, +critcmp (spec §2) → Task 5. (Note: spec mentioned `protobuf-compiler`; the plan installs `clang`/`libclang-dev`/`cmake` — the actual rocksdb-sys build deps. protobuf-compiler is not needed by ultima_db's competitor tier; dropped deliberately. If a future bench adds a protobuf codegen dep, add it then.) ✓ (deviation noted)
- os_tune verbatim, mount at remote_home (spec §2) → Task 4. ✓
- run role: rsync tree + bench_target dispatch + manifest (spec §2) → Task 6. ✓
- collect → bench-out/dist/<ts> (spec §2) → Task 7. ✓
- Makefile targets incl. bench-oneshot/status/ssh (spec §4) → Task 8. ✓
- root bench/wal-ab (spec §4) → Task 9. ✓
- hygiene .gitignore/.env.example/example.tfvars (spec §5) → Task 1. ✓
- README + CLAUDE.md pointer, no taskXX (spec §5) → Task 10. ✓

**2. Placeholder scan:** No TBD/TODO/"handle errors"/"similar to Task N". Every code step has full content. ✓

**3. Type consistency:** `bench_target` values `competitor|wal-ab|autobench` consistent across group_vars (T4), run role (T6), Makefile (T8). `remote_home=/opt/bench`, `bench_dir=/opt/bench/bench-data`, `ud_src_dir=/opt/bench/ultima_db` consistent T4/T6. Terraform outputs `nodes`/`ssh_user` consistent T2 → inventory script T3 → Makefile T8. Baseline names in `bench/wal-ab` (T9) match the `critcmp` args. ✓

**Deviations from spec (deliberate):**
- Dropped `protobuf-compiler` from toolchains (not a real ultima_db bench dep); added `cmake`/`clang`/`libclang-dev` (the real rocksdb-sys deps). Documented in T5 and here.
- `bench_dir` placed under the NVMe mount (`/opt/bench/bench-data`) rather than a separate `/mnt/nvme/bench`, so os_tune stays verbatim (mounts at `remote_home`) and everything lands on NVMe. Spec §Data-flow said `/mnt/nvme/bench`; this is a strictly-simpler equivalent. Reflected in group_vars (T4).
- Trimmed os_tune's sysctl loop to `vm.swappiness=0` only (the network sysctls were Aeron-specific). Kept governor/THP/limits/tuned. Justified: single-node storage bench, no inter-node UDP.

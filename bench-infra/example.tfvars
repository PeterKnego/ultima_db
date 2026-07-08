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

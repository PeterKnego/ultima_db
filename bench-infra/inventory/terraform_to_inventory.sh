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

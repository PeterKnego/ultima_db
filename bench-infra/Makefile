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

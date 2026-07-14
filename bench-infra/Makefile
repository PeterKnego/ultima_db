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

.PHONY: init up inventory bench/competitor bench/wal-ab bench/smr-ycsb bench/autobench bench/fanout bench/smr-ab bench/fanout-micro bench-oneshot status ssh destroy

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

bench/smr-ycsb: ## SMR (checkpoint-only) vs Eventual YCSB A/B (ultima-only) on NVMe
	cd ansible && $(ANSIBLE) bench.yml -e bench_target=smr-ycsb

bench/autobench: ## autobench Gate-A microbenches + baseline record
	cd ansible && $(ANSIBLE) bench.yml -e bench_target=autobench

bench/fanout: ## B-tree fanout (T) A/B sweep — get/insert/remove @1M random keys
	cd ansible && $(ANSIBLE) bench.yml -e bench_target=fanout

bench/smr-ab: ## SMR-apply/read-p99 fanout (T) A/B — the contended perf-gate workload
	cd ansible && $(ANSIBLE) bench.yml -e bench_target=smr-ab

bench/fanout-micro: ## Fanout (T) read-vs-write asymmetry — warm+cold get/insert/update/remove @T 8..256
	cd ansible && $(ANSIBLE) bench.yml -e bench_target=fanout-micro

bench-oneshot: ## guard TARGET, then up -> bench (TARGET=competitor|wal-ab|smr-ycsb|autobench|fanout|smr-ab|fanout-micro) -> destroy (destroy runs even if up/bench fail)
	@if [ -z "$(TARGET)" ]; then echo "ERROR: set TARGET=competitor|wal-ab|smr-ycsb|autobench|fanout|smr-ab|fanout-micro"; exit 1; fi
	@rc=0; \
	( $(MAKE) up && cd ansible && $(ANSIBLE) bench.yml -e bench_target=$(TARGET) ) || rc=$$?; \
	if ! $(MAKE) destroy; then \
	  echo "ERROR: destroy FAILED — billable resources may still be running; rerun 'make destroy' and check 'make status'"; \
	  exit 1; \
	fi; \
	if [ $$rc -ne 0 ]; then echo "bench-oneshot: up/bench failed (rc=$$rc); host was destroyed."; fi; \
	exit $$rc

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

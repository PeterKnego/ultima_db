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

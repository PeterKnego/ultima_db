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

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

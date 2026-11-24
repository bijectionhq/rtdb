output "flight_endpoint" {
  description = "TLS Arrow Flight endpoint for RTDB clients."
  value       = "grpc+tls://${var.tls.hostname}:443"
}

output "node_security_group_id" {
  description = "Security group to allow as a client of Kafka or Logkeeper."
  value       = aws_security_group.node.id
}

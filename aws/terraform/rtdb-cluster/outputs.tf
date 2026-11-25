output "flight_endpoint" {
  description = "TLS Arrow Flight endpoint for RTDB clients."
  value       = "grpc+tls://${var.tls.hostname}:443"
}

output "node_security_group_id" {
  description = "Security group to allow as a client of Kafka or Logkeeper."
  value       = aws_security_group.node.id
}

output "classic_candidate_relay_target" {
  description = "Private DNS entry and TCP port for the Classic recovery relay, when enabled."
  value = local.classic_candidate == null ? null : {
    entry = local.peer_discovery_host
    port  = local.classic_candidate.port
  }
}

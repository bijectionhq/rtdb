resource "aws_security_group" "node" {
  name   = "${var.name}-node"
  vpc_id = var.vpc_id
  tags   = var.tags
}

resource "aws_security_group" "flight" {
  name   = "${var.name}-flight"
  vpc_id = var.vpc_id
  tags   = var.tags
}

resource "aws_vpc_security_group_ingress_rule" "flight" {
  for_each                     = var.client_security_group_ids
  security_group_id            = aws_security_group.flight.id
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}

resource "aws_vpc_security_group_egress_rule" "flight" {
  security_group_id            = aws_security_group.flight.id
  referenced_security_group_id = aws_security_group.node.id
  ip_protocol                  = "tcp"
  from_port                    = local.flight_port
  to_port                      = local.flight_port
}

resource "aws_vpc_security_group_ingress_rule" "node" {
  security_group_id            = aws_security_group.node.id
  referenced_security_group_id = aws_security_group.flight.id
  ip_protocol                  = "tcp"
  from_port                    = local.flight_port
  to_port                      = local.flight_port
}

resource "aws_vpc_security_group_ingress_rule" "peers" {
  count                        = local.multi ? 1 : 0
  security_group_id            = aws_security_group.node.id
  referenced_security_group_id = aws_security_group.node.id
  ip_protocol                  = "tcp"
  from_port                    = local.flight_port
  to_port                      = local.flight_port
}

resource "aws_vpc_security_group_ingress_rule" "write_peers" {
  count                        = local.multi ? 1 : 0
  security_group_id            = aws_security_group.node.id
  referenced_security_group_id = aws_security_group.node.id
  ip_protocol                  = "tcp"
  from_port                    = local.peer_port
  to_port                      = local.peer_port
}

resource "aws_vpc_security_group_ingress_rule" "classic_candidate" {
  count                        = local.classic_candidate == null ? 0 : 1
  security_group_id            = aws_security_group.node.id
  referenced_security_group_id = local.classic_candidate.source_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = local.classic_candidate.port
  to_port                      = local.classic_candidate.port
}

resource "aws_vpc_security_group_ingress_rule" "classic_opening" {
  for_each                     = local.classic_opening == null ? toset([]) : local.classic_opening.source_security_group_ids
  security_group_id            = aws_security_group.node.id
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  from_port                    = local.classic_opening.port
  to_port                      = local.classic_opening.port
}

resource "aws_vpc_security_group_ingress_rule" "native_dependent_custody" {
  for_each                     = local.native_dependent_ingress == null ? toset([]) : local.native_dependent_ingress.source_security_group_ids
  security_group_id            = aws_security_group.node.id
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  from_port                    = local.native_dependent_ingress.port
  to_port                      = local.native_dependent_ingress.port
}

resource "aws_vpc_security_group_ingress_rule" "original_private_replay_approvals" {
  count                        = local.replay_approvals == null ? 0 : 1
  security_group_id            = local.replay_approvals.mount_target_security_group_id
  referenced_security_group_id = aws_security_group.node.id
  ip_protocol                  = "tcp"
  from_port                    = 2049
  to_port                      = 2049
}

resource "aws_vpc_security_group_ingress_rule" "consensus_journal" {
  count                        = local.consensus_journal == null ? 0 : 1
  security_group_id            = local.consensus_journal.mount_target_security_group_id
  referenced_security_group_id = aws_security_group.node.id
  ip_protocol                  = "tcp"
  from_port                    = 2049
  to_port                      = 2049
}

resource "aws_vpc_security_group_egress_rule" "node" {
  security_group_id = aws_security_group.node.id
  ip_protocol       = "tcp"
  from_port         = 1
  to_port           = 65535
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_lb" "flight" {
  name                             = "${var.name}-flight"
  internal                         = true
  load_balancer_type               = "network"
  security_groups                  = [aws_security_group.flight.id]
  subnets                          = var.private_subnet_ids
  enable_cross_zone_load_balancing = true
  tags                             = var.tags
}

resource "aws_lb_target_group" "flight" {
  name        = "${var.name}-flight"
  port        = local.flight_port
  protocol    = "TCP"
  target_type = "ip"
  vpc_id      = var.vpc_id
  health_check {
    protocol = "TCP"
  }
  tags = var.tags
}

resource "aws_lb_listener" "flight" {
  load_balancer_arn = aws_lb.flight.arn
  port              = 443
  protocol          = "TLS"
  alpn_policy       = "HTTP2Only"
  certificate_arn   = var.tls.certificate_arn
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.flight.arn
  }
  tags = var.tags
}

resource "aws_route53_record" "flight" {
  zone_id = var.tls.zone_id
  name    = var.tls.hostname
  type    = "A"
  alias {
    name                   = aws_lb.flight.dns_name
    zone_id                = aws_lb.flight.zone_id
    evaluate_target_health = true
  }
}

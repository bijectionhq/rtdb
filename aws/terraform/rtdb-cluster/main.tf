data "aws_region" "current" {}

locals {
  flight_port = 3000
  multi       = var.mode == "multi"
  secrets = merge({ RTDB_ROOT_PASSWORD = var.root_password_secret }, var.journal_credentials == null ? {} : {
    RTDB_JOURNAL_CA_CERTIFICATE = var.journal_credentials.ca_certificate
    RTDB_JOURNAL_CERTIFICATE    = var.journal_credentials.certificate
    RTDB_JOURNAL_PRIVATE_KEY    = var.journal_credentials.private_key
  })
  environment = merge({
    AWS_REGION              = data.aws_region.current.region
    RTDB_BUCKET             = var.bucket
    RTDB_KMS_KEY            = var.kms_key
    RTDB_MEMORY_LIMIT_BYTES = "3221225472"
    }, local.multi ? {
    RTDB_JOURNAL_URL = var.journal_url
    RTDB_PEERS       = "node.${aws_service_discovery_private_dns_namespace.cluster.name}"
    } : {
    RTDB_KAFKA_BOOTSTRAP = var.kafka_bootstrap
    RTDB_KAFKA_TOPIC     = var.kafka_topic
  })
}

resource "aws_ecs_cluster" "cluster" {
  name = var.name
  tags = var.tags
}

resource "aws_service_discovery_private_dns_namespace" "cluster" {
  name = "${var.name}.rtdb.internal"
  vpc  = var.vpc_id
  tags = var.tags
}

resource "aws_service_discovery_service" "node" {
  name = "node"
  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.cluster.id
    routing_policy = "MULTIVALUE"
    dns_records {
      type = "A"
      ttl  = 5
    }
  }
  health_check_custom_config {}
  tags = var.tags
}

resource "aws_cloudwatch_log_group" "node" {
  name              = "/ecs/${var.name}"
  retention_in_days = 30
  tags              = var.tags
}

resource "aws_ecs_task_definition" "node" {
  family                   = var.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 2048
  memory                   = 4096
  task_role_arn            = aws_iam_role.node.arn
  execution_role_arn       = aws_iam_role.execution.arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([{
    name            = "rtdb"
    image           = var.image
    essential       = true
    user            = "20000:20000"
    command         = local.multi ? ["node", "--multi"] : ["node", "--file", "/etc/rtdb/cluster-default.yaml"]
    environment     = [for name, value in local.environment : { name = name, value = value } if value != null]
    secrets         = [for name, value in local.secrets : { name = name, valueFrom = value }]
    stopTimeout     = 90
    portMappings    = [{ containerPort = local.flight_port, protocol = "tcp" }]
    linuxParameters = { capabilities = { drop = ["ALL"] } }
    logConfiguration = { logDriver = "awslogs", options = {
      awslogs-group         = aws_cloudwatch_log_group.node.name
      awslogs-region        = data.aws_region.current.region
      awslogs-stream-prefix = "node"
    } }
  }])
  lifecycle {
    precondition {
      condition = local.multi ? (
        try(startswith(var.journal_url, "logkeeper+tls://"), false) && var.journal_credentials != null && var.journal_client_security_group_id != null &&
        var.kafka_bootstrap == null && var.kafka_topic == null
        ) : (
        try(length(trimspace(var.kafka_bootstrap)) > 0, false) && try(length(trimspace(var.kafka_topic)) > 0, false) &&
        var.journal_url == null && var.journal_credentials == null && var.journal_client_security_group_id == null
      )
      error_message = "Default mode requires Kafka bootstrap and topic. Multi mode requires a Logkeeper TLS URL, journal credentials and a journal client security group."
    }
  }
  tags = var.tags
}

resource "aws_ecs_service" "node" {
  name             = "node"
  cluster          = aws_ecs_cluster.cluster.arn
  task_definition  = aws_ecs_task_definition.node.arn
  desired_count    = var.node_count
  launch_type      = "FARGATE"
  platform_version = "1.4.0"
  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = concat([aws_security_group.node.id], var.journal_client_security_group_id == null ? [] : [var.journal_client_security_group_id])
    assign_public_ip = false
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.flight.arn
    container_name   = "rtdb"
    container_port   = local.flight_port
  }
  service_registries {
    registry_arn = aws_service_discovery_service.node.arn
  }
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
  depends_on = [
    aws_lb_listener.flight,
    aws_vpc_security_group_ingress_rule.node,
    aws_vpc_security_group_egress_rule.node,
    aws_iam_role_policy.objects,
    aws_iam_role_policy.secrets,
    aws_iam_role_policy_attachment.execution,
    aws_s3_bucket_server_side_encryption_configuration.objects,
  ]
  tags = var.tags
}

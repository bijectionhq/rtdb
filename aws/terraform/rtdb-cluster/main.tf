data "aws_region" "current" {}

locals {
  flight_port                = 3000
  peer_port                  = 3001
  multi                      = var.mode == "multi"
  custom_multi_configuration = local.multi && var.multi_configuration_secret != null
  classic_candidate          = local.multi ? var.classic_candidate_ingress : null
  classic_opening            = local.multi ? var.classic_opening_ingress : null
  native_dependent_custody   = local.multi ? var.native_dependent_custody : null
  native_dependent_ingress   = try(local.native_dependent_custody.ingress, null)
  native_dependent_directory = "/run/rtdb/native-dependent-custody"
  replay_approvals           = local.multi ? var.original_private_replay_approvals : null
  replay_approvals_directory = "/run/rtdb/original-private-replay"
  consensus_journal          = local.multi ? var.consensus_journal : null
  consensus_directory        = "/run/rtdb/journal"
  consensus_journal_url      = local.consensus_journal == null ? null : "logkeeper-consensus://${local.consensus_directory}/${local.consensus_journal.file}"
  journal_url                = local.consensus_journal == null ? var.journal_url : local.consensus_journal_url
  peer_discovery_host        = "node.${aws_service_discovery_private_dns_namespace.cluster.name}"
  node_ports = concat(
    [{ containerPort = local.flight_port, protocol = "tcp" }],
    local.multi ? [{ containerPort = local.peer_port, protocol = "tcp" }] : [],
    local.classic_candidate == null ? [] : [{ containerPort = local.classic_candidate.port, protocol = "tcp" }],
    local.classic_opening == null ? [] : [{ containerPort = local.classic_opening.port, protocol = "tcp" }],
    local.native_dependent_ingress == null ? [] : [{ containerPort = local.native_dependent_ingress.port, protocol = "tcp" }]
  )
  secrets = merge(
    { RTDB_ROOT_PASSWORD = var.root_password_secret },
    local.custom_multi_configuration ? { RTDB_MULTI_CONFIG_YAML = var.multi_configuration_secret } : {},
    local.classic_candidate == null ? {} : { RTDB_CLASSIC_CANDIDATE_INGRESS_CONFIG = local.classic_candidate.configuration_secret },
    local.classic_opening == null ? {} : { RTDB_CLASSIC_OPENING_INGRESS_CONFIG = local.classic_opening.configuration_secret },
    merge({}, [
      for slot, credential in(var.journal_credentials == null ? {} : var.journal_credentials) : {
        "RTDB_JOURNAL_${slot}_CA_CERTIFICATE" = credential.ca_certificate
        "RTDB_JOURNAL_${slot}_CERTIFICATE"    = credential.certificate
        "RTDB_JOURNAL_${slot}_PRIVATE_KEY"    = credential.private_key
      }
    ]...)
  )
  multi_command = concat(
    ["node", "--multi"],
    local.custom_multi_configuration ? ["--config-env", "RTDB_MULTI_CONFIG_YAML"] : ["--file", "/etc/rtdb/cluster-multi.yaml"]
  )

  environment = merge({
    AWS_REGION              = data.aws_region.current.region
    RTDB_BUCKET             = var.bucket
    RTDB_KMS_KEY            = var.kms_key
    RTDB_MEMORY_LIMIT_BYTES = "3221225472"
    }, local.multi ? {
    RTDB_JOURNAL_URL   = local.journal_url
    RTDB_JOURNAL_SLOTS = tostring(var.node_count)
    RTDB_KEY_DIRECTORY = "/var/lib/rtdb/keys"
    RTDB_DATABASES     = jsonencode(var.databases == null ? { xtdb = uuidv5("dns", "${var.name}.${var.bucket}.rtdb") } : var.databases)
    RTDB_PEERS         = local.peer_discovery_host
    } : {
    RTDB_KAFKA_BOOTSTRAP = var.kafka_bootstrap
    RTDB_KAFKA_TOPIC     = var.kafka_topic
    }, local.classic_candidate == null ? {} : {
    RTDB_CLASSIC_CANDIDATE_INGRESS_PORT = tostring(local.classic_candidate.port)
    }, local.classic_opening == null ? {} : {
    RTDB_CLASSIC_OPENING_INGRESS_PORT = tostring(local.classic_opening.port)
    }, local.native_dependent_custody == null ? {} : {
    RTDB_NATIVE_DEPENDENT_CUSTODY_FILE = "${local.native_dependent_directory}/installation.json"
    RTDB_NATIVE_DEPENDENT_CUSTODY_PORT = tostring(local.native_dependent_ingress == null ? 0 : local.native_dependent_ingress.port)
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
  dynamic "volume" {
    for_each = local.multi ? [true] : []
    content {
      name = "keys"
      efs_volume_configuration {
        file_system_id     = aws_efs_file_system.keys[0].id
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = aws_efs_access_point.keys[0].id
          iam             = "ENABLED"
        }
      }
    }
  }
  dynamic "volume" {
    for_each = local.native_dependent_custody == null ? [] : [local.native_dependent_custody]
    content {
      name = "native-dependent-custody"
      efs_volume_configuration {
        file_system_id     = split("/", volume.value.file_system_arn)[1]
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = split("/", volume.value.access_point_arn)[1]
          iam             = "ENABLED"
        }
      }
    }
  }
  dynamic "volume" {
    for_each = local.consensus_journal == null ? [] : [local.consensus_journal]
    content {
      name = "consensus-journal"
      efs_volume_configuration {
        file_system_id     = split("/", volume.value.file_system_arn)[1]
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = split("/", volume.value.access_point_arn)[1]
          iam             = "ENABLED"
        }
      }
    }
  }
  dynamic "volume" {
    for_each = local.replay_approvals == null ? [] : [local.replay_approvals]
    content {
      name = "original-private-replay"
      efs_volume_configuration {
        file_system_id     = split("/", volume.value.file_system_arn)[1]
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = split("/", volume.value.access_point_arn)[1]
          iam             = "ENABLED"
        }
      }
    }
  }
  container_definitions = jsonencode([{
    name        = "rtdb"
    image       = var.image
    essential   = true
    user        = "20000:20000"
    command     = local.multi ? local.multi_command : ["node", "--file", "/etc/rtdb/cluster-default.yaml"]
    environment = [for name, value in local.environment : { name = name, value = value } if value != null]
    secrets     = [for name, value in local.secrets : { name = name, valueFrom = value }]
    mountPoints = concat(
      local.multi ? [{ sourceVolume = "keys", containerPath = "/var/lib/rtdb/keys", readOnly = false }] : [],
      local.native_dependent_custody == null ? [] : [{ sourceVolume = "native-dependent-custody", containerPath = local.native_dependent_directory, readOnly = true }],
      local.replay_approvals == null ? [] : [{ sourceVolume = "original-private-replay", containerPath = local.replay_approvals_directory, readOnly = true }],
      local.consensus_journal == null ? [] : [{ sourceVolume = "consensus-journal", containerPath = local.consensus_directory, readOnly = true }],
    )
    stopTimeout     = 90
    portMappings    = local.node_ports
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
        (
          local.consensus_journal == null ? try(startswith(var.journal_url, "logkeeper+tls://"), false) :
          var.journal_url == null || var.journal_url == local.consensus_journal_url
        ) && try(toset(keys(var.journal_credentials)) == toset([for slot in range(var.node_count) : tostring(slot)]), false) &&
        try(length(distinct([for config in values(var.journal_credentials) : config.certificate])) == var.node_count, false) &&
        var.node_count <= 8 && var.journal_client_security_group_id != null &&
        var.kafka_bootstrap == null && var.kafka_topic == null
        ) : (
        try(length(trimspace(var.kafka_bootstrap)) > 0, false) && try(length(trimspace(var.kafka_topic)) > 0, false) &&
        var.journal_url == null && var.journal_credentials == null && var.journal_client_security_group_id == null && var.databases == null
      )
      error_message = "Default mode requires Kafka bootstrap and topic. Multi mode requires a Logkeeper TLS URL or a consensus journal file, one distinct journal credential per slot, at most eight nodes and a journal client security group."
    }
    precondition {
      condition     = local.multi || var.consensus_journal == null
      error_message = "A consensus journal requires Multi mode."
    }
    precondition {
      condition = local.consensus_journal == null || try(
        local.consensus_journal.access_point_arn != try(local.native_dependent_custody.access_point_arn, "") &&
        local.consensus_journal.access_point_arn != try(local.replay_approvals.access_point_arn, ""), false
      )
      error_message = "The consensus journal needs its own EFS access point."
    }
    precondition {
      condition     = local.multi || var.multi_configuration_secret == null
      error_message = "A Multi configuration secret requires Multi mode."
    }
    precondition {
      condition     = local.multi || var.classic_candidate_ingress == null
      error_message = "Classic candidate ingress requires Multi mode."
    }
    precondition {
      condition     = var.classic_opening_ingress == null || (local.multi && (local.classic_candidate == null || try(local.classic_candidate.port != local.classic_opening.port && local.classic_candidate.configuration_secret != local.classic_opening.configuration_secret, false)))
      error_message = "Classic opening ingress requires Multi mode and a listener and secret distinct from the Classic candidate ingress."
    }
    precondition {
      condition     = local.multi || var.native_dependent_custody == null
      error_message = "Native dependent custody requires Multi mode."
    }
    precondition {
      condition = var.original_private_replay_approvals == null || try(
        local.custom_multi_configuration && (
          local.native_dependent_custody == null ||
          local.replay_approvals.access_point_arn != local.native_dependent_custody.access_point_arn
        ), false
      )
      error_message = "Original replay approvals require a custom Multi configuration and a separate approval access point."
    }
    precondition {
      condition = local.native_dependent_ingress == null || try(
        local.native_dependent_ingress.port != try(local.classic_candidate.port, null) &&
        local.native_dependent_ingress.port != try(local.classic_opening.port, null), false
      )
      error_message = "Native dependent custody ingress needs a private port distinct from the Classic recovery listeners."
    }
  }
  tags = var.tags
}

resource "aws_ecs_service" "node" {
  name                               = "node"
  cluster                            = aws_ecs_cluster.cluster.arn
  task_definition                    = aws_ecs_task_definition.node.arn
  desired_count                      = var.node_count
  launch_type                        = "FARGATE"
  platform_version                   = "1.4.0"
  deployment_minimum_healthy_percent = local.multi ? (var.node_count == 1 ? 0 : 50) : 100
  deployment_maximum_percent         = local.multi ? 100 : 200
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
    aws_vpc_security_group_ingress_rule.write_peers,
    aws_vpc_security_group_ingress_rule.classic_candidate,
    aws_vpc_security_group_ingress_rule.native_dependent_custody,
    aws_iam_role_policy.native_dependent_custody,
    aws_vpc_security_group_ingress_rule.original_private_replay_approvals,
    aws_iam_role_policy.original_private_replay_approvals,
    aws_vpc_security_group_ingress_rule.consensus_journal,
    aws_iam_role_policy.consensus_journal,
    aws_vpc_security_group_egress_rule.node,
    aws_iam_role_policy.objects,
    aws_iam_role_policy.secrets,
    aws_iam_role_policy_attachment.execution,
    aws_s3_bucket_server_side_encryption_configuration.objects,
    aws_efs_mount_target.keys,
    aws_iam_role_policy.keys,
    aws_efs_file_system_policy.keys,
    aws_vpc_security_group_ingress_rule.keys,
  ]
  tags = var.tags
}

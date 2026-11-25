mock_provider "aws" {
  mock_data "aws_region" {
    defaults = { region = "eu-north-1" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
}

variables {
  name                      = "rtdb-test"
  image                     = "123456789012.dkr.ecr.eu-north-1.amazonaws.com/rtdb@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  vpc_id                    = "vpc-11111111"
  private_subnet_ids        = ["subnet-11111111", "subnet-22222222"]
  client_security_group_ids = { client = "sg-11111111" }
  bucket                    = "rtdb-test-objects"
  kms_key                   = "arn:aws:kms:eu-north-1:123456789012:key/11111111-1111-1111-1111-111111111111"
  tls = {
    hostname        = "rtdb-test.example.internal"
    zone_id         = "Z1111111111111"
    certificate_arn = "arn:aws:acm:eu-north-1:123456789012:certificate/11111111-1111-1111-1111-111111111111"
  }
  root_password_secret = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:root-secret-Qwerty"
}

run "default_uses_kafka_and_only_the_root_secret" {
  command = plan
  variables {
    kafka_bootstrap = "kafka.internal:9092"
    kafka_topic     = "rtdb"
    node_count      = 3
  }
  assert {
    condition = (
      aws_ecs_service.node.desired_count == 3 &&
      length(aws_efs_file_system.keys) == 0 &&
      jsondecode(aws_ecs_task_definition.node.container_definitions)[0].command == ["node", "--file", "/etc/rtdb/cluster-default.yaml"] &&
      length(jsondecode(aws_ecs_task_definition.node.container_definitions)[0].secrets) == 1
    )
    error_message = "The default cluster should run the Kafka node from the common runtime image."
  }
}

run "multi_passes_the_logkeeper_url_and_credential_selectors" {
  command = plan
  variables {
    mode        = "multi"
    journal_url = "logkeeper+tls://keeper-1.example.internal:5448,keeper-2.example.internal:5448,keeper-3.example.internal:5448"
    journal_credentials = {
      "0" = {
        ca_certificate = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:ca-Abcdef"
        certificate    = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:crt-Abcdef"
        private_key    = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:key-Abcdef"
      }
    }
    journal_client_security_group_id = "sg-22222222"
  }
  assert {
    condition = (
      local.environment.RTDB_JOURNAL_URL == var.journal_url &&
      local.secrets.RTDB_JOURNAL_0_PRIVATE_KEY == var.journal_credentials["0"].private_key &&
      local.environment.RTDB_KEY_DIRECTORY == "/var/lib/rtdb/keys" &&
      jsondecode(aws_ecs_task_definition.node.container_definitions)[0].command == ["node", "--multi", "--file", "/etc/rtdb/cluster-multi.yaml"] &&
      !contains(keys(local.environment), "RTDB_KAFKA_BOOTSTRAP")
    )
    error_message = "The Multi cluster must pass Logkeeper's URL and client references unchanged."
  }
}

run "default_rejects_a_journal_url" {
  command = plan
  variables {
    kafka_bootstrap = "kafka.internal:9092"
    kafka_topic     = "rtdb"
    journal_url     = "logkeeper+tls://keeper.internal:5448"
  }
  expect_failures = [aws_ecs_task_definition.node]
}

run "multi_can_use_an_operator_owned_configuration" {
  command = plan
  variables {
    mode        = "multi"
    journal_url = "logkeeper+tls://keeper-1.example.internal:5448,keeper-2.example.internal:5448,keeper-3.example.internal:5448"
    journal_credentials = {
      "0" = {
        ca_certificate = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:ca-Abcdef"
        certificate    = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:crt-Abcdef"
        private_key    = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:key-Abcdef"
      }
    }
    journal_client_security_group_id = "sg-22222222"
    multi_configuration_secret       = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:node-config-Abcdef"
  }
  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.node.container_definitions)[0].command == ["node", "--multi", "--config-env", "RTDB_MULTI_CONFIG_YAML"] &&
      local.secrets.RTDB_MULTI_CONFIG_YAML == var.multi_configuration_secret &&
      contains(local.secret_arns, var.multi_configuration_secret)
    )
    error_message = "The Multi node must load the operator-owned YAML through its ECS secret reference."
  }
}

run "default_rejects_a_multi_configuration_secret" {
  command = plan
  variables {
    kafka_bootstrap            = "kafka.internal:9092"
    kafka_topic                = "rtdb"
    multi_configuration_secret = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:node-config-Abcdef"
  }
  expect_failures = [aws_ecs_task_definition.node]
}

run "multi_admits_classic_relays_on_the_dedicated_private_port" {
  command = plan
  variables {
    mode        = "multi"
    journal_url = "logkeeper+tls://keeper-1.example.internal:5448,keeper-2.example.internal:5448,keeper-3.example.internal:5448"
    journal_credentials = {
      "0" = {
        ca_certificate = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:ca-Abcdef"
        certificate    = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:crt-Abcdef"
        private_key    = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:key-Abcdef"
      }
    }
    journal_client_security_group_id = "sg-22222222"
    classic_candidate_ingress = {
      port                     = 7443
      source_security_group_id = "sg-33333333"
      configuration_secret     = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:classic-ingress-Abcdef"
    }
  }
  assert {
    condition = (
      local.environment.RTDB_CLASSIC_CANDIDATE_INGRESS_PORT == "7443" &&
      local.secrets.RTDB_CLASSIC_CANDIDATE_INGRESS_CONFIG == var.classic_candidate_ingress.configuration_secret &&
      output.classic_candidate_relay_target.entry == local.environment.RTDB_PEERS &&
      output.classic_candidate_relay_target.port == 7443 &&
      aws_vpc_security_group_ingress_rule.classic_candidate[0].referenced_security_group_id == "sg-33333333"
    )
    error_message = "The independent candidate endpoint must use the enrolled relay group and Multi node discovery."
  }
}

run "default_rejects_classic_candidate_ingress" {
  command = plan
  variables {
    kafka_bootstrap = "kafka.internal:9092"
    kafka_topic     = "rtdb"
    classic_candidate_ingress = {
      port                     = 7443
      source_security_group_id = "sg-33333333"
      configuration_secret     = "arn:aws:secretsmanager:eu-north-1:123456789012:secret:classic-ingress-Abcdef"
    }
  }
  expect_failures = [aws_ecs_task_definition.node]
}

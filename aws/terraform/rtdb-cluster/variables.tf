variable "name" {
  description = "Cluster name, unique within this AWS account and region."
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,25}$", var.name))
    error_message = "Use 1–26 lowercase letters, digits or hyphens, starting with a letter."
  }
}

variable "image" {
  description = "Immutable Linux ARM64 RTDB runtime image reference."
  type        = string
  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.image))
    error_message = "Pin the RTDB image by its SHA-256 digest."
  }
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  description = "Routed private subnets for the nodes and their internal Flight endpoint."
  type        = list(string)
  validation {
    condition     = length(distinct(var.private_subnet_ids)) >= 2
    error_message = "Supply private subnets in at least two availability zones."
  }
}

variable "client_security_group_ids" {
  description = "Map from stable client names to security group IDs allowed to connect to Flight."
  type        = map(string)
  validation {
    condition     = length(var.client_security_group_ids) > 0
    error_message = "Supply at least one Flight client security group."
  }
}

variable "mode" {
  description = "Default runs the Kafka driver. Multi uses the RTDB Journal driver."
  type        = string
  default     = "default"
  validation {
    condition     = contains(["default", "multi"], var.mode)
    error_message = "RTDB mode must be default or multi."
  }
}

variable "node_count" {
  type    = number
  default = 1
  validation {
    condition     = var.node_count >= 1 && floor(var.node_count) == var.node_count
    error_message = "Supply a positive whole number of RTDB nodes."
  }
}

variable "bucket" {
  description = "Name of an existing, dedicated RTDB object bucket in this region. This module manages its default encryption configuration."
  type        = string
}

variable "kms_key" {
  description = "ARN of the KMS key for RTDB storage encryption."
  type        = string
}

variable "tls" {
  description = "Internal Flight hostname, private DNS zone and matching ACM certificate."
  type = object({
    hostname        = string
    zone_id         = string
    certificate_arn = string
  })
  nullable = false
}

variable "root_password_secret" {
  description = "ECS Secrets Manager selector for the RTDB root password. Use a bare secret ARN or pin a version."
  type        = string
}

variable "multi_configuration_secret" {
  description = "Optional ECS Secrets Manager selector whose value is the complete Multi node YAML."
  type        = string
  default     = null
  validation {
    condition = var.multi_configuration_secret == null || can(regex(
      "^arn:[a-z0-9-]+:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+-[A-Za-z0-9]{6}(:[^:\\r\\n]*:([^:\\r\\n]*:|:[^:\\r\\n]*))?$",
      var.multi_configuration_secret
    ))
    error_message = "Supply an ECS Secrets Manager selector for the Multi configuration."
  }
}

variable "classic_candidate_ingress" {
  description = "Optional Multi-only ingress for an authorized Classic recovery relay."
  type = object({
    port                     = number
    source_security_group_id = string
    configuration_secret     = string
  })
  default = null
  validation {
    condition = var.classic_candidate_ingress == null || try(
      var.classic_candidate_ingress.port == floor(var.classic_candidate_ingress.port) &&
      var.classic_candidate_ingress.port >= 1024 &&
      var.classic_candidate_ingress.port <= 65535 &&
      !contains([3000, 3001], var.classic_candidate_ingress.port) &&
      can(regex("^sg-([a-f0-9]{8}|[a-f0-9]{17})$", var.classic_candidate_ingress.source_security_group_id)) &&
      can(regex(
        "^arn:[a-z0-9-]+:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+-[A-Za-z0-9]{6}(:[^:\\r\\n]*:([^:\\r\\n]*:|:[^:\\r\\n]*))?$",
        var.classic_candidate_ingress.configuration_secret
      )),
      false
    )
    error_message = "Supply a separate TCP port, the Classic relay security group, and an ECS Secrets Manager selector."
  }
}

variable "classic_opening_ingress" {
  description = "Optional private Multi ingress for the Classic opening operator and read-only reconciler."
  type = object({
    port                      = number
    source_security_group_ids = set(string)
    configuration_secret      = string
  })
  default = null
  validation {
    condition = var.classic_opening_ingress == null || try(
      var.classic_opening_ingress.port == floor(var.classic_opening_ingress.port) &&
      var.classic_opening_ingress.port >= 1024 &&
      var.classic_opening_ingress.port <= 65535 &&
      !contains([3000, 3001], var.classic_opening_ingress.port) &&
      length(var.classic_opening_ingress.source_security_group_ids) > 0 &&
      alltrue([for group in var.classic_opening_ingress.source_security_group_ids :
        can(regex("^sg-([a-f0-9]{8}|[a-f0-9]{17})$", group))
      ]) &&
      can(regex(
        "^arn:[a-z0-9-]+:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+-[A-Za-z0-9]{6}:[^:\\r\\n]*::[A-Za-z0-9-]{32,64}$",
        var.classic_opening_ingress.configuration_secret
      )), false
    )
    error_message = "Supply a separate private TCP port, source security groups and a version-pinned ECS secret for Classic opening."
  }
}

variable "native_dependent_custody" {
  description = "Optional Multi-only private EFS profile mount and ingress for the native dependent custody transport."
  type = object({
    file_system_arn  = string
    access_point_arn = string
    ingress = optional(object({
      port                      = number
      source_security_group_ids = set(string)
    }))
  })
  default = null

  validation {
    condition = var.native_dependent_custody == null || try(
      can(regex("^arn:[a-z0-9-]+:elasticfilesystem:[a-z0-9-]+:[0-9]{12}:file-system/fs-[0-9a-f]+$", var.native_dependent_custody.file_system_arn)) &&
      can(regex("^arn:[a-z0-9-]+:elasticfilesystem:[a-z0-9-]+:[0-9]{12}:access-point/fsap-[0-9a-f]+$", var.native_dependent_custody.access_point_arn)) &&
      slice(split(":", var.native_dependent_custody.file_system_arn), 0, 5) == slice(split(":", var.native_dependent_custody.access_point_arn), 0, 5) &&
      (var.native_dependent_custody.ingress == null || try(
        var.native_dependent_custody.ingress.port == floor(var.native_dependent_custody.ingress.port) &&
        var.native_dependent_custody.ingress.port >= 1024 &&
        var.native_dependent_custody.ingress.port <= 65535 &&
        !contains([3000, 3001], var.native_dependent_custody.ingress.port) &&
        length(var.native_dependent_custody.ingress.source_security_group_ids) > 0 &&
        alltrue([for group in var.native_dependent_custody.ingress.source_security_group_ids :
          can(regex("^sg-([a-f0-9]{8}|[a-f0-9]{17})$", group))
        ]), false
      )), false
    )
    error_message = "Native dependent custody needs a private EFS file system and access point, and any ingress needs a dedicated TCP port and source security groups."
  }
}

variable "original_private_replay_approvals" {
  description = "Optional read-only EFS projection from the independent Original replay approval publisher."
  type = object({
    file_system_arn                = string
    access_point_arn               = string
    mount_target_security_group_id = string
  })
  default = null

  validation {
    condition = var.original_private_replay_approvals == null || try(
      can(regex("^arn:[a-z0-9-]+:elasticfilesystem:[a-z0-9-]+:[0-9]{12}:file-system/fs-[0-9a-f]+$", var.original_private_replay_approvals.file_system_arn)) &&
      can(regex("^arn:[a-z0-9-]+:elasticfilesystem:[a-z0-9-]+:[0-9]{12}:access-point/fsap-[0-9a-f]+$", var.original_private_replay_approvals.access_point_arn)) &&
      slice(split(":", var.original_private_replay_approvals.file_system_arn), 0, 5) == slice(split(":", var.original_private_replay_approvals.access_point_arn), 0, 5) &&
      can(regex("^sg-([a-f0-9]{8}|[a-f0-9]{17})$", var.original_private_replay_approvals.mount_target_security_group_id)),
      false
    )
    error_message = "Original replay approvals need a dedicated private EFS access point and mount target security group."
  }
}

variable "kafka_bootstrap" {
  description = "Comma-separated bootstrap endpoints for default mode."
  type        = string
  default     = null
}

variable "kafka_topic" {
  description = "Default-mode source topic. Provision this topic and its -replica topic before starting the cluster."
  type        = string
  default     = null
}

variable "journal_url" {
  description = "Multi-mode journal URL: a `logkeeper+tls://` URL from the Logkeeper cluster's journal_url output, or `logkeeper-consensus:///run/rtdb/journal/<file>` with `consensus_journal`. Omit it with `consensus_journal` to use its file."
  type        = string
  default     = null
}

variable "consensus_journal" {
  description = "Optional Multi-only read-only EFS projection holding the Logkeeper consensus keeper JournalConfig JSON, mounted at /run/rtdb/journal."
  type = object({
    file_system_arn                = string
    access_point_arn               = string
    mount_target_security_group_id = string
    file                           = optional(string, "journal.json")
  })
  default = null

  validation {
    condition = var.consensus_journal == null || try(
      can(regex("^arn:[a-z0-9-]+:elasticfilesystem:[a-z0-9-]+:[0-9]{12}:file-system/fs-[0-9a-f]+$", var.consensus_journal.file_system_arn)) &&
      can(regex("^arn:[a-z0-9-]+:elasticfilesystem:[a-z0-9-]+:[0-9]{12}:access-point/fsap-[0-9a-f]+$", var.consensus_journal.access_point_arn)) &&
      slice(split(":", var.consensus_journal.file_system_arn), 0, 5) == slice(split(":", var.consensus_journal.access_point_arn), 0, 5) &&
      can(regex("^sg-([a-f0-9]{8}|[a-f0-9]{17})$", var.consensus_journal.mount_target_security_group_id)) &&
      can(regex("^[A-Za-z0-9._-]+\\.json$", var.consensus_journal.file)),
      false
    )
    error_message = "The consensus journal needs a private EFS file system, a dedicated access point, its mount target security group and a plain JSON file name."
  }
}

variable "journal_credentials" {
  description = "One Logkeeper client identity and its ECS Secrets Manager selectors per Multi node, keyed by slot (0, 1, and so on)."
  type = map(object({
    ca_certificate = string
    certificate    = string
    private_key    = string
  }))
  default = null
}

variable "databases" {
  description = "Multi database names mapped to permanent Journal UUIDs. Omit to bootstrap one xtdb database for this cluster."
  type        = map(string)
  default     = null
}

variable "journal_client_security_group_id" {
  description = "Existing security group attached to RTDB nodes and admitted by the Logkeeper module."
  type        = string
  default     = null
}

variable "tags" {
  type    = map(string)
  default = {}
}

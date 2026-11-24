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
  description = "Multi-mode journal URL from the Logkeeper cluster's journal_url output."
  type        = string
  default     = null
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

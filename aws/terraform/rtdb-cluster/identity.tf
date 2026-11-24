data "aws_partition" "current" {}

locals {
  bucket_arn = "arn:${data.aws_partition.current.partition}:s3:::${var.bucket}"
  task_trust = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  secret_arns = distinct([for secret in values(local.secrets) : join(":", slice(split(":", secret), 0, 7))])
}

resource "aws_s3_bucket_server_side_encryption_configuration" "objects" {
  bucket = var.bucket
  rule {
    bucket_key_enabled = true
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${var.name}-node"
  assume_role_policy = local.task_trust
  tags               = var.tags
}

resource "aws_iam_role_policy" "objects" {
  role = aws_iam_role.node.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"], Resource = local.bucket_arn },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], Resource = "${local.bucket_arn}/*" },
      { Effect = "Allow", Action = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"], Resource = var.kms_key },
    ]
  })
}

resource "aws_iam_role" "execution" {
  name               = "${var.name}-execution"
  assume_role_policy = local.task_trust
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "secrets" {
  role = aws_iam_role.execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = "secretsmanager:GetSecretValue", Resource = local.secret_arns },
      { Effect = "Allow", Action = "kms:Decrypt", Resource = var.kms_key },
    ]
  })
}

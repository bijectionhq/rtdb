resource "aws_efs_file_system" "keys" {
  count      = local.multi ? 1 : 0
  encrypted  = true
  kms_key_id = var.kms_key
  tags       = var.tags
  lifecycle { prevent_destroy = true }
}

resource "aws_efs_backup_policy" "keys" {
  count          = local.multi ? 1 : 0
  file_system_id = aws_efs_file_system.keys[0].id
  backup_policy { status = "DISABLED" }
}

resource "aws_security_group" "keys" {
  count  = local.multi ? 1 : 0
  name   = "${var.name}-keys"
  vpc_id = var.vpc_id
  tags   = var.tags
}

resource "aws_vpc_security_group_ingress_rule" "keys" {
  count                        = local.multi ? 1 : 0
  security_group_id            = aws_security_group.keys[0].id
  referenced_security_group_id = aws_security_group.node.id
  ip_protocol                  = "tcp"
  from_port                    = 2049
  to_port                      = 2049
}

resource "aws_efs_mount_target" "keys" {
  for_each        = local.multi ? { for index, subnet in var.private_subnet_ids : tostring(index) => subnet } : {}
  file_system_id  = aws_efs_file_system.keys[0].id
  subnet_id       = each.value
  security_groups = [aws_security_group.keys[0].id]
}

resource "aws_efs_access_point" "keys" {
  count          = local.multi ? 1 : 0
  file_system_id = aws_efs_file_system.keys[0].id
  posix_user {
    uid = 20000
    gid = 20000
  }
  root_directory {
    path = "/keys"
    creation_info {
      owner_uid   = 20000
      owner_gid   = 20000
      permissions = "0700"
    }
  }
  tags = var.tags
}

resource "aws_iam_role_policy" "keys" {
  count = local.multi ? 1 : 0
  role  = aws_iam_role.node.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["elasticfilesystem:ClientMount", "elasticfilesystem:ClientWrite"]
      Resource = aws_efs_file_system.keys[0].arn
      Condition = { StringEquals = {
        "elasticfilesystem:AccessPointArn" = aws_efs_access_point.keys[0].arn
      } }
    }]
  })
}

resource "aws_efs_file_system_policy" "keys" {
  count          = local.multi ? 1 : 0
  file_system_id = aws_efs_file_system.keys[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = aws_iam_role.node.arn }
      Action    = ["elasticfilesystem:ClientMount", "elasticfilesystem:ClientWrite"]
      Resource  = aws_efs_file_system.keys[0].arn
      Condition = {
        Bool         = { "aws:SecureTransport" = "true" }
        StringEquals = { "elasticfilesystem:AccessPointArn" = aws_efs_access_point.keys[0].arn }
      }
    }]
  })
}

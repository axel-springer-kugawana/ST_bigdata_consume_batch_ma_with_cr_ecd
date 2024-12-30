resource "aws_iam_role" "glue_role" {
  name = "${var.bucket}-${var.env}_glue_role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_policy" "glue_get_table_policy" {
  name        = "${var.bucket}-${var.env}_glue_get_table_policy"
  description = "Policy to allow glue:GetTable action"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:*"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_get_table_policy_attachment" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_get_table_policy.arn
}

resource "aws_iam_role_policy_attachment" "s3_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "kinesis_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonKinesisFullAccess"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchFullAccess"
}

resource "aws_iam_role_policy_attachment" "sns_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSNSFullAccess"
}

resource "aws_iam_role_policy_attachment" "iam_read_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/IAMReadOnlyAccess"
}

resource "aws_iam_user" "ma_user" {
  name = "ma-bigdata-user"
  path = "/"
}

resource "aws_iam_access_key" "ma_user_access_key" {
  user = aws_iam_user.ma_user.name
}

resource "aws_secretsmanager_secret" "ma_user_credentials" {
  name = "ma-bigdata-user-credentials"
}

resource "aws_secretsmanager_secret_version" "ma_user_credentials" {
  secret_id     = aws_secretsmanager_secret.ma_user_credentials.id
  secret_string = <<EOF
{
  "access_key_id": "${aws_iam_access_key.ma_user_access_key.id}",
  "secret_access_key": "${aws_iam_access_key.ma_user_access_key.secret}"
}
EOF
}


resource "aws_iam_policy" "ma_bigdata_user_policy" {
  name        = "ma-bigdata-user-policy"
  description = "Policy for ma-bigdata-user to access S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:*"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket}-${var.env}",
          "arn:aws:s3:::${var.bucket}-${var.env}/*",
          "arn:aws:s3:::consume-batch-ma-with-cr-ecd-${var.env}",
          "arn:aws:s3:::consume-batch-ma-with-cr-ecd-${var.env}/*"
        ]
      }
    ]
  })
}


resource "aws_iam_user_policy_attachment" "ma_bigdata_user_policy_attachment" {
  user       = aws_iam_user.ma_user.name
  policy_arn = aws_iam_policy.ma_bigdata_user_policy.arn
}

output "ma_user_credentials_secret_name" {
  value       = aws_secretsmanager_secret.ma_user_credentials.name
  description = "The MA user credentials"
}
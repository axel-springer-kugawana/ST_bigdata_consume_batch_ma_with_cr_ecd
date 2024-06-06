resource "aws_cloudwatch_event_rule" "cw_event_rule" {
  name        = "${var.bucket}-${var.env}-failure-rule"
  description = "Trigger on Glue job state changes"
  event_pattern = jsonencode({
    source      = ["aws.glue"],
    detail-type = ["Glue Job State Change"],
    detail = {
      "jobName" : ["${aws_glue_job.glue-job.name}"],
      "state" : ["FAILED", "ERROR", "TIMEOUT"]
    }
  })
}

resource "aws_sns_topic" "sns_topic" {
  name = "${var.bucket}-${var.env}-glue-sns"
}

resource "aws_sns_topic_subscription" "sns_subscription" {
  topic_arn = aws_sns_topic.sns_topic.arn
  protocol  = "email"
  endpoint  = var.sns_email_address
}

resource "aws_cloudwatch_event_target" "sns_target" {
  target_id = "${var.bucket}-${var.env}-sns-target"
  rule      = aws_cloudwatch_event_rule.cw_event_rule.name
  arn       = aws_sns_topic.sns_topic.arn
}

resource "aws_sns_topic_policy" "default" {
  arn    = aws_sns_topic.sns_topic.arn
  policy = data.aws_iam_policy_document.sns_topic_policy.json
}

data "aws_iam_policy_document" "sns_topic_policy" {
  statement {
    effect  = "Allow"
    actions = ["SNS:Publish"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    resources = [aws_sns_topic.sns_topic.arn]
  }
}
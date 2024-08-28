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

resource "aws_cloudwatch_event_target" "alert_parser" {
  target_id = "${var.bucket}-${var.env}-alert"
  rule      = aws_cloudwatch_event_rule.cw_event_rule.name
  arn       = "arn:aws:lambda:eu-central-1:${var.account_id}:function:ST-bigdata-alerts-parser-${var.env}_lambda"
}

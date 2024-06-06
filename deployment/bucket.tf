resource "aws_s3_bucket" "bucket" {
  bucket = "${var.bucket}-${var.env}"
}

resource "aws_s3_object" "folder" {
  bucket = aws_s3_bucket.bucket.id
  key    = "data/"
}

resource "aws_s3_object" "tmp" {
  bucket = aws_s3_bucket.bucket.id
  key    = "tmp/"
}
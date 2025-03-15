resource "aws_glue_catalog_table" "aws_glue_catalog_table" {
  name          = var.table_name
  database_name = "kafka"

  parameters = {
    "classification"       = "parquet"
    "useGlueParquetWriter" = "true",
    "parquet.compression"  = "SNAPPY"
  }

  table_type = "EXTERNAL_TABLE"

  partition_keys {
    name = "partitionmonth"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${var.bucket}-${var.env}/data/${var.table_name}"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"


    ser_de_info {
      name                  = "my-stream"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }
  }
}

resource "aws_s3_object" "upload-glue-script" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/main.py"
  source = "../../script/main.py"
  etag   = filemd5("../../script/main.py")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-glue-helper" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/helper.py"
  source = "../../script/helper.py"
  etag   = filemd5("../../script/helper.py")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-bundeslaender" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/bundeslaender.csv"
  source = "../../script/static_files/bundeslaender.csv"
  etag   = filemd5("../../script/static_files/bundeslaender.csv")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-stadtlandkreise" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/stadtlandkreise.csv"
  source = "../../script/static_files/stadtlandkreise.csv"
  etag   = filemd5("../../script/static_files/stadtlandkreise.csv")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-attributes_all" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/attributes_all.txt"
  source = "../../script/static_files/attributes_all.txt"
  etag   = filemd5("../../script/static_files/attributes_all.txt")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-classified_cols" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/classified_cols.txt"
  source = "../../script/static_files/classified_cols.txt"
  etag   = filemd5("../../script/static_files/classified_cols.txt")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-basedata_first_query" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/basedata_first_query.sql"
  source = "../../script/static_files/basedata_first_query.sql"
  etag   = filemd5("../../script/static_files/basedata_first_query.sql")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-basedata_df_query" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/basedata_df_query.sql"
  source = "../../script/static_files/basedata_df_query.sql"
  etag   = filemd5("../../script/static_files/basedata_df_query.sql")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-basedata_df_final_query" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/basedata_df_final_query.sql"
  source = "../../script/static_files/basedata_df_final_query.sql"
  etag   = filemd5("../../script/static_files/basedata_df_final_query.sql")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-merge_delete_query" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/merge_delete_query.sql"
  source = "../../script/static_files/merge_delete_query.sql"
  etag   = filemd5("../../script/static_files/merge_delete_query.sql")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-log4j2-properties" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/log4j2.properties"
  source = "../../script/log4j2.properties"
  etag   = filemd5("../../script/log4j2.properties")
}


resource "aws_glue_job" "glue-job" {
  name = "${var.bucket}-${var.env}"

  glue_version = "4.0"
  role_arn     = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${var.bucket}-${var.env}/scripts/main.py"
    python_version  = "3"
  }

  default_arguments = {
    "--partition_date"                   = "yesterday"
    "--extra-py-files"                   = "s3://${var.bucket}-${var.env}/scripts/helper.py"
    "--extra-files"                      = "s3://${var.bucket}-${var.env}/scripts/stadtlandkreise.csv,s3://${var.bucket}-${var.env}/scripts/bundeslaender.csv,s3://${var.bucket}-${var.env}/scripts/attributes_all.txt,s3://${var.bucket}-${var.env}/scripts/classified_cols.txt,s3://${var.bucket}-${var.env}/scripts/basedata_first_query.sql,s3://${var.bucket}-${var.env}/scripts/basedata_df_query.sql,s3://${var.bucket}-${var.env}/scripts/basedata_df_final_query.sql,s3://${var.bucket}-${var.env}/scripts/merge_delete_query.sql,s3://${var.bucket}-${var.env}/scripts/log4j2.properties"
    "--conf"                             = <<-EOT
                                            --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
                                            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
                                            --conf spark.sql.broadcastTimeout=36000
                                            --conf spark.executor.memory=10g
                                            --conf spark.driver.memory=10g
                                            --conf spark.sql.shuffle.partitions=1000
                                            --conf spark.sql.adaptive.enabled=true
                                            EOT
    "--datalake-formats"                 = "delta"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${var.bucket}-${var.env}/tmp"
  }

  worker_type       = var.env == "live" ? "G.8X" : "G.1X"
  number_of_workers = var.env == "live" ? 10 : 2
  max_retries       = 0
}

resource "aws_glue_trigger" "glue_trigger" {
  count    = var.env == "live" ? 1 : 0
  name     = "${var.bucket}-${var.env}-glue-trigger"
  schedule = "cron(0 1 * * ? *)"
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.glue-job.name
  }
}
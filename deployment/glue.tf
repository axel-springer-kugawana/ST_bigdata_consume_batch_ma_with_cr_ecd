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

# upload data files
resource "aws_s3_object" "upload-bundeslaender" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/bundeslaender.csv"
  source = "../../script/static_files/data/bundeslaender.csv"
  etag   = filemd5("../../script/static_files/data/bundeslaender.csv")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-stadtlandkreise" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/stadtlandkreise.csv"
  source = "../../script/static_files/data/stadtlandkreise.csv"
  etag   = filemd5("../../script/static_files/data/stadtlandkreise.csv")

  depends_on = [aws_s3_bucket.bucket]
}

# upload config file
resource "aws_s3_object" "upload-config" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/config.json"
  source = "../../script/config.json"
  etag   = filemd5("../../script/config.json")

  depends_on = [aws_s3_bucket.bucket]
}

# upload query files
resource "aws_s3_object" "upload-basedata_first_query" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/1-basedata_first_query.sql"
  source = "../../script/static_files/queries/1-basedata_first_query.sql"
  etag   = filemd5("../../script/static_files/queries/1-basedata_first_query.sql")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-basedata_df_query" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/2-basedata_df_query.sql"
  source = "../../script/static_files/queries/2-basedata_df_query.sql"
  etag   = filemd5("../../script/static_files/queries/2-basedata_df_query.sql")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-basedata_df_final_query" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/3-basedata_df_final_query.sql"
  source = "../../script/static_files/queries/3-basedata_df_final_query.sql"
  etag   = filemd5("../../script/static_files/queries/3-basedata_df_final_query.sql")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-merge_delete_query" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/0-merge_delete_query.sql"
  source = "../../script/static_files/queries/0-merge_delete_query.sql"
  etag   = filemd5("../../script/static_files/queries/0-merge_delete_query.sql")

  depends_on = [aws_s3_bucket.bucket]
}

resource "aws_s3_object" "upload-log4j2-properties" {
  bucket = "${var.bucket}-${var.env}"
  key    = "scripts/log4j2.properties"
  source = "../../script/static_files/log4j2.properties"
  etag   = filemd5("../../script/static_files/log4j2.properties")
}


resource "aws_glue_job" "glue-job" {
  name = "${var.bucket}-${var.env}"

  glue_version = "5.0"
  role_arn     = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${var.bucket}-${var.env}/scripts/main.py"
    python_version  = "3"
  }

  default_arguments = {
    "--extra-py-files"                   = "s3://${var.bucket}-${var.env}/scripts/helper.py"
    "--extra-files"                      = "s3://${var.bucket}-${var.env}/scripts/stadtlandkreise.csv,s3://${var.bucket}-${var.env}/scripts/bundeslaender.csv,s3://${var.bucket}-${var.env}/scripts/config.json,s3://${var.bucket}-${var.env}/scripts/1-basedata_first_query.sql,s3://${var.bucket}-${var.env}/scripts/2-basedata_df_query.sql,s3://${var.bucket}-${var.env}/scripts/3-basedata_df_final_query.sql,s3://${var.bucket}-${var.env}/scripts/0-merge_delete_query.sql,s3://${var.bucket}-${var.env}/scripts/log4j2.properties"
    "--partition_date"                   = "yesterday"
    "--conf"                             = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.broadcastTimeout=36000 --conf spark.sql.autoBroadcastJoinThreshold=4294967296"
    "--executor-cores"                   = var.env == "live" ? floor(32 * 1.8) : floor(8 * 1.8) # The value should not exceed 2x the number of vCPUs on the worker type, which is 8 on G.1X, 16 on G.2X, 32 on G.4X and 64 on G.8X
    "--datalake-formats"                 = "delta"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${var.bucket}-${var.env}/tmp"
  }

  worker_type       = var.env == "live" ? "G.4X" : "G.1X"
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
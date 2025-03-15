import sys
from datetime import datetime, timedelta

import boto3
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from helper import GlobalVariables, Helper, Queries
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def parse_env_name() -> str:
    client = boto3.client("iam")

    account_alliases = client.list_account_aliases()
    acc_name = account_alliases.get("AccountAliases")[0]
    env_name = acc_name.split("-")[-1]
    return env_name


ENV_NAME = parse_env_name()


def cacheDf(df, glueContext, transformation_ctx) -> DynamicFrame:
    cached_df = df.toDF()
    cached_df = cached_df.cache()

    return DynamicFrame.fromDF(cached_df, glueContext, transformation_ctx)


def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(
        "(select * from source1) UNION " + unionType + " (select * from source2)"
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


def sparkSqlQuery(
    glueContext, query, mapping, transformation_ctx, plan=False
) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    if plan:
        result.explain(mode="formatted")
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


def filter_red_red(df: DynamicFrame, partition_date: str) -> DynamicFrame:
    """
    Filter red_red dataframe
    """
    df = df.toDF()
    result = df.filter(
        (F.col("cleaned_classified_distributionType").isin("RENT", "BUY"))
        & (
            F.col("classified_geo_countrySpecific_de_iwtLegacyGeoID").startswith("108")
            | F.col("classified_geo_countrySpecific_de_iwtLegacyGeoID").startswith(
                "103"
            )
        )
        & (F.col("classified_estateType").isin("HOUSE", "APARTMENT"))
        & (F.col("partitionchangedate") <= F.to_date(F.lit(partition_date)))
    )
    return DynamicFrame.fromDF(result, glueContext, "red_red_filtered")


def update_delete(glueContext, df) -> DynamicFrame:
    columns = df.toDF().columns
    prefixes = ["classified_", "cleaned_", "cleanup", "extracted_", "grenzwert_"]

    # Use dictionary comprehension to create lists of columns based on prefixes
    filtered_columns_wo_prefix = [
        x for prefix in prefixes for x in columns if x.startswith(prefix)
    ]
    filtered_columns_str_wo_prefix = ",".join(filtered_columns_wo_prefix)

    filtered_columns_with_prefix = [
        "b." + x for prefix in prefixes for x in columns if x.startswith(prefix)
    ]
    filtered_columns_str_with_prefix = ",".join(filtered_columns_with_prefix)

    ret_df = sparkSqlQuery(
        glueContext,
        query=Queries.get_merge_delete_query(
            extra_columns_wo_prefix=filtered_columns_str_wo_prefix,
            extra_columns_with_prefix=filtered_columns_str_with_prefix,
        ),
        mapping={"red_red_cleaned": df},
        transformation_ctx="merged_df",
    )
    ret_df = ret_df.drop_fields(paths=["rank"])

    return cacheDf(ret_df, glueContext, "cached_update_delete")


def modifyData(glueContext, df, geoid) -> DynamicFrame:
    ret_df = df.toDF()

    # clean values
    ret_df = ret_df.withColumn(
        "cleaned_classified_structure_rooms_numberofrooms",
        F.round(F.col("cleaned_classified_structure_rooms_numberofrooms")).cast("int"),
    ).withColumn(
        "classified_geo_city",
        F.regexp_replace(F.col("classified_geo_city"), "\\\\\\\\", ""),
    )
    # rename columns
    cols_to_rename = [c for c in ret_df.columns if c.startswith("cleaned_")]
    for old_name in cols_to_rename:
        new_name = old_name.replace("cleaned_", "")
        ret_df = ret_df.withColumnRenamed(old_name, new_name)

    if geoid == 108:
        bundeslaender_df = spark.read.csv(
            "bundeslaender.csv", header=True, inferSchema="true"
        )
        stadtlandkreise_df = spark.read.csv(
            "stadtlandkreise.csv", header=True, inferSchema="true"
        )

        ret_df = (
            ret_df.join(
                bundeslaender_df,
                F.substring(
                    ret_df.classified_geo_countrySpecific_de_iwtLegacyGeoID, 1, 5
                )
                == bundeslaender_df.geoid,
                how="left",
            )
            .drop("geoid")
            .join(
                stadtlandkreise_df,
                F.substring(
                    ret_df.classified_geo_countrySpecific_de_iwtLegacyGeoID, 1, 8
                )
                == stadtlandkreise_df.geoid,
                how="left",
            )
            .drop("geoid", "stadtkreis")
            .withColumnRenamed("bundesland", "geo_state")
            .withColumnRenamed("landkreis", "geo_userDefined_immoWelt_county")
        )
    else:
        ret_df = ret_df.withColumn("geo_state", F.lit(None))
        ret_df = ret_df.withColumn("geo_userDefined_immoWelt_county", F.lit(None))

    ret_df = ret_df.drop(
        "geoid",
        "classified_geo_userDefined_immoWelt_geoid",
        "classified_geo_countrySpecific_de_iwtLegacyGeoID",
    )

    # add additional columns
    ret_df = ret_df.withColumn("partitionGeoid", F.lit(geoid)).withColumn(
        "partitionMonth", F.lit(GlobalVariables.partition_month)
    )

    return DynamicFrame.fromDF(ret_df, glueContext, "attributes")


args = getResolvedOptions(sys.argv, ["JOB_NAME", "partition_date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


if args["partition_date"] == "yesterday":
    partition_date = (datetime.now() - timedelta(days=1)).date()
else:
    partition_date = datetime.strptime(args["partition_date"], "%Y-%m-%d")

GlobalVariables.set_dates(partition_date=partition_date)
partition_date = partition_date.strftime("%Y-%m-%d")

########## get base tables from data catalog
red_red_cleaned_raw = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [f"s3://ingest-stream-red-red-{ENV_NAME}/data/cleaned_new"],
        "recurse": True,
    },
    transformation_ctx="red_red_cleaned_raw",
)

red_red_filtered = filter_red_red(red_red_cleaned_raw, partition_date)
red_red_cleaned = update_delete(glueContext=glueContext, df=red_red_filtered)

red_red_text = glueContext.create_dynamic_frame.from_catalog(
    database="kafka",
    table_name="red_red_text",
    push_down_predicate=f"(partitioncreateddate >= '{GlobalVariables.first_day_month_ago}' and partitioncreateddate <= '{partition_date}')",
    transformation_ctx="red_red_text",
)

red_vd_cleaned_spark_raw = glueContext.create_data_frame.from_catalog(
    database="kafka",
    table_name="red_vd_cleaned",
    transformation_ctx="red_vd_cleaned_spark",
)
red_vd_cleaned_spark = red_vd_cleaned_spark_raw.filter(
    (F.col("aktivab") <= F.to_timestamp(F.lit(partition_date)))
    & (
        (F.col("aktivbis") <= F.to_timestamp(F.lit(partition_date)))
        | (F.col("aktivbis") >= F.to_timestamp(F.lit("2100-01-01")))
    )
)

red_vd_cleaned = DynamicFrame.fromDF(
    red_vd_cleaned_spark, glueContext, "red_vd_cleaned"
)

red_ecd = glueContext.create_dynamic_frame.from_catalog(
    database="kafka",
    table_name="red_ecd_raw",
    push_down_predicate=f"(partitioncreateddate >= '{GlobalVariables.first_day_month_ago}' and partitioncreateddate <= '{partition_date}')",
    transformation_ctx="red_ecd",
)

contactrequests_daily_cr_per_classified = glueContext.create_dynamic_frame.from_catalog(
    database="kinesis",
    table_name="contactrequests_daily_cr_per_classified",
    push_down_predicate=f"(partitioncreateddate >= '{GlobalVariables.first_day_current_month}' and partitioncreateddate <= '{partition_date}')",
    transformation_ctx="contactrequests_daily_cr_per_classified",
)

customeractions_daily_actions_per_classified = glueContext.create_dynamic_frame.from_catalog(
    database="kinesis",
    table_name="customeractions_daily_actions_per_classified",
    push_down_predicate=f"(partitioncreateddate >= '{GlobalVariables.first_day_current_month}' and partitioncreateddate <= '{partition_date}')",
    transformation_ctx="customeractions_daily_actions_per_classified",
)
##########

# MAIN

# GEOID, DATA_COUNTRY, DISTRIBUTION_TYPE, DATA_SOURCE
country_values = [
    (108, "GERMANY", "BUY", "SELL"),
    (108, "GERMANY", "RENT", "RENTAL"),
    (103, "AUSTRIA", "BUY", "SELL"),
    (103, "AUSTRIA", "RENT", "RENTAL"),
]

union_df = None
for geoid, data_country, distribution_type, data_source in country_values:
    print(f"Getting data for: {geoid, data_country, distribution_type, data_source}")
    queries_obj = Queries(distribution_type=distribution_type, geoid=geoid)

    BaseDataFirst = sparkSqlQuery(
        glueContext=glueContext,
        query=queries_obj.get_BaseData_first_query(),
        mapping={
            "red_red_cleaned": red_red_cleaned,
            "red_red_text": red_red_text,
        },
        transformation_ctx="BaseData_first",
    )
    BaseDataFirst = cacheDf(BaseDataFirst, glueContext, "basedata_first_cached")
    print("Done fetching base data first")

    BaseData = sparkSqlQuery(
        glueContext=glueContext,
        query=queries_obj.get_BaseData_df_query(),
        mapping={
            "BaseDataFirst": BaseDataFirst,
            "red_ecd": red_ecd,
            "red_vd_cleaned": red_vd_cleaned,
            "contactrequests_daily_cr_per_classified": contactrequests_daily_cr_per_classified,
            "customeractions_daily_actions_per_classified": customeractions_daily_actions_per_classified,
        },
        transformation_ctx="BaseData_df",
    )
    BaseData = cacheDf(BaseData, glueContext, "basedata_cached")
    print("Done fetching base data cached")

    BaseData_final_df = sparkSqlQuery(
        glueContext=glueContext,
        query=queries_obj.get_BaseData_final_df_query(),
        mapping={
            "BaseDataFirst": BaseDataFirst,
            "BaseData": BaseData,
        },
        transformation_ctx="BaseData_final_df",
    )
    print("Done fetching base data final")

    BaseData_final_df = BaseData_final_df.drop_fields(
        paths=GlobalVariables.cols_to_drop_basedata
    )
    BaseData_final_df = modifyData(
        glueContext=glueContext,
        df=BaseData_final_df,
        geoid=geoid,
    )

    if union_df is None:
        union_df = BaseData_final_df
    else:
        union_df = sparkUnion(
            glueContext,
            unionType="ALL",
            mapping={"source1": union_df, "source2": BaseData_final_df},
            transformation_ctx="Union_node",
        )

    print("Union df done")

    csv_df = BaseData_final_df.drop_fields(paths=GlobalVariables.cols_to_drop_json)
    json_df = Helper.modifyDataJson(
        glueContext=glueContext, df=csv_df, distribution_type=distribution_type
    )

    s3_path_json = f"s3://consume-batch-ma-with-cr-ecd-{ENV_NAME}/data/{data_country.lower()}/{distribution_type.lower()}/json/partitioncreateddate={partition_date}"
    AmazonS3_node1714127201181 = glueContext.write_dynamic_frame.from_options(
        frame=json_df,
        connection_type="s3",
        format="json",
        connection_options={"compression": "gzip", "path": s3_path_json},
        transformation_ctx="AmazonS3_node1714127201181",
    )

    s3_path_csv = f"s3://consume-batch-ma-with-cr-ecd-{ENV_NAME}/data/{data_country.lower()}/{distribution_type.lower()}/csv/partitioncreateddate={partition_date}"
    AmazonS3_node1714127201182 = glueContext.write_dynamic_frame.from_options(
        frame=csv_df.coalesce(1),
        connection_type="s3",
        format="csv",
        connection_options={"compression": "gzip", "path": s3_path_csv},
        transformation_ctx="AmazonS3_node1714127201181",
    )
    print("Done with json")

# delete insert instead of replacewhere
glueContext.purge_table(
    database="kafka",
    table_name="offers_ma_geo",
    options={
        "partitionPredicate": f"(partitionmonth == '{GlobalVariables.partition_month}')",
        "retentionPeriod": 1,
    },
)
print("Done purging table")

AWSGlueDataCatalog_node1709799333156 = glueContext.write_dynamic_frame.from_catalog(
    frame=union_df,
    database="kafka",
    table_name="offers_ma_geo",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
        "partitionKeys": ["partitionMonth"],
    },
    transformation_ctx="AWSGlueDataCatalog_node1709799333156",
)
print("Done writing to table")

job.commit()

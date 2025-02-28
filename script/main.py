import json
import logging
import sys
from datetime import datetime, timedelta

import boto3
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from dateutil.relativedelta import relativedelta
from helper import Helper, Queries
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def parse_env_name() -> str:
    """
    Parse the environment name from the account alias
    """
    client = boto3.client("iam")

    account_alliases = client.list_account_aliases()
    acc_name = account_alliases.get("AccountAliases")[0]
    env_name = acc_name.split("-")[-1]
    return env_name


def cacheDf(
    df: DynamicFrame, glueContext: GlueContext, transformation_ctx: str
) -> DynamicFrame:
    """
    Cache the dataframe to optimize performance
    """
    cached_df = df.toDF()
    cached_df = cached_df.cache()

    return DynamicFrame.fromDF(cached_df, glueContext, transformation_ctx)


def sparkUnion(
    glueContext: GlueContext, unionType: str, mapping: dict, transformation_ctx: str
) -> DynamicFrame:
    """
    Spark union operation
    """
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(
        "(select * from source1) UNION " + unionType + " (select * from source2)"
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


def sparkSqlQuery(
    glueContext: GlueContext,
    query: str,
    mapping: dict,
    transformation_ctx: str,
    plan: bool = False,
) -> DynamicFrame:
    """
    Spark SQL query operation
    """
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    if plan:
        result.explain(mode="formatted")
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


def update_delete(glueContext: GlueContext, df: DynamicFrame) -> DynamicFrame:
    """
    Clean red_red deleted records and prepare it
    """
    columns = df.toDF().columns

    # Use dictionary comprehension to create lists of columns based on prefixes
    filtered_columns_str_wo_prefix = ",".join(
        x
        for prefix in config["validPrefixValues"]
        for x in columns
        if x.startswith(prefix)
    )
    filtered_columns_str_with_prefix = ",".join(
        "b." + x
        for prefix in config["validPrefixValues"]
        for x in columns
        if x.startswith(prefix)
    )

    ret_df = sparkSqlQuery(
        glueContext,
        query=Queries.get_merge_delete_query(
            extra_columns_wo_prefix=filtered_columns_str_wo_prefix,
            extra_columns_with_prefix=filtered_columns_str_with_prefix,
            first_day_past=first_day_past,
            first_day_next_month=first_day_next_month,
        ),
        mapping={"red_red_cleaned": df},
        transformation_ctx="merged_df",
    )
    ret_df = ret_df.drop_fields(paths=["rank"])

    return cacheDf(ret_df, glueContext, "cached_update_delete")


def join_csv_static_data(df: DataFrame) -> DataFrame:
    bundeslaender_df = spark.read.csv(
        "bundeslaender.csv", header=True, inferSchema="true"
    )
    stadtlandkreise_df = spark.read.csv(
        "stadtlandkreise.csv", header=True, inferSchema="true"
    )

    ret_df = (
        df.join(
            F.broadcast(bundeslaender_df),
            F.substring(df.classified_geo_countrySpecific_de_iwtLegacyGeoID, 1, 5)
            == bundeslaender_df.geoid,
            how="left",
        )
        .drop("geoid")
        .join(
            F.broadcast(stadtlandkreise_df),
            F.substring(df.classified_geo_countrySpecific_de_iwtLegacyGeoID, 1, 8)
            == stadtlandkreise_df.geoid,
            how="left",
        )
        .drop("geoid", "stadtkreis")
        .withColumnRenamed("bundesland", "geo_state")
        .withColumnRenamed("landkreis", "geo_userDefined_immoWelt_county")
    )
    return ret_df


def modify_data(
    glueContext: GlueContext, df: DynamicFrame, geoid: int, partition_month: str
) -> DynamicFrame:
    """
    Custom transformation for the data
    """
    ret_df = df.toDF()

    # Clean the columns
    ret_df = ret_df.withColumn(
        "cleaned_classified_structure_rooms_numberofrooms",
        F.round(F.col("cleaned_classified_structure_rooms_numberofrooms")).cast("int"),
    ).withColumn(
        "classified_geo_city",
        F.regexp_replace(F.col("classified_geo_city"), "\\\\\\\\", ""),
    )

    # Remove the prefix from the column names
    cols_to_rename = [c for c in ret_df.columns if c.startswith("cleaned_")]
    for old_name in cols_to_rename:
        new_name = old_name.replace("cleaned_", "")
        ret_df = ret_df.withColumnRenamed(old_name, new_name)

    if geoid == 108:
        ret_df = join_csv_static_data(ret_df)
    else:
        ret_df = ret_df.withColumn("geo_state", F.lit(None))
        ret_df = ret_df.withColumn("geo_userDefined_immoWelt_county", F.lit(None))

    ret_df = ret_df.drop(*config["geoDropColumns"])

    # add additional columns
    ret_df = ret_df.withColumn("partitionGeoid", F.lit(geoid)).withColumn(
        "partitionMonth", F.lit(partition_month)
    )

    return DynamicFrame.fromDF(ret_df, glueContext, "attributes")


def set_date_values(
    partition_date: datetime.date, days_ago: int
) -> tuple[str, str, str, str]:
    """
    Set date values for the queries
    """
    first_day_current_month = partition_date.replace(day=1).strftime("%Y-%m-%d")
    first_day_next_month = (
        (partition_date.replace(day=1) + relativedelta(months=+1))
        .replace(day=1)
        .strftime("%Y-%m-%d")
    )
    first_day_past = (
        partition_date.replace(day=1) + relativedelta(days=-days_ago)
    ).strftime("%Y-%m-%d")
    partition_month = partition_date.strftime("%Y-%m")

    return (
        first_day_current_month,
        first_day_next_month,
        first_day_past,
        partition_month,
    )


# load arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "partition_date"])
sc = SparkContext()
glueContext = GlueContext(sc)

spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# set logger
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

ENV_NAME = parse_env_name()

# load config
with open("config.json") as f:
    config = json.load(f)

if args["partition_date"] == "yesterday":
    partition_date = (datetime.now() - timedelta(days=1)).date()
else:
    partition_date = args["partition_date"].strptime("%Y-%m-%d")

# set date values
(
    first_day_current_month,
    first_day_next_month,
    first_day_past,
    partition_month,
) = set_date_values(partition_date=partition_date, days_ago=7)

# getting base tables from data catalog
# NumPartitions = numSlotsPerExecutor * numExecutors
# (10-1) * 16 = 144
additional_options = {"hashfield": "partitioncreateddate", "hashpartitions": "144"}
red_red_cleaned_draft = glueContext.create_dynamic_frame.from_catalog(
    database="kafka",
    table_name="red_red_cleaned",
    transformation_ctx="red_red_cleaned_draft",
)
red_red_cleaned = update_delete(glueContext=glueContext, df=red_red_cleaned_draft)
raise Exception(
        f"DEBUG: Stopping after red_red_cleaned: cnt {red_red_cleaned.count()}"
    )

red_red_text = glueContext.create_dynamic_frame.from_catalog(
    database="kafka",
    table_name="red_red_text",
    push_down_predicate=f"(partitioncreateddate>=to_date('{first_day_past}') and partitioncreateddate<to_date('{first_day_next_month}'))",
    transformation_ctx="red_red_text",
)

red_vd_cleaned_spark = glueContext.create_data_frame.from_catalog(
    database="kafka",
    table_name="red_vd_cleaned",
    transformation_ctx="red_vd_cleaned_spark",
)
red_vd_cleaned = DynamicFrame.fromDF(
    red_vd_cleaned_spark, glueContext, "red_vd_cleaned"
)

red_ecd = glueContext.create_dynamic_frame.from_catalog(
    database="kafka",
    table_name="red_ecd_raw",
    push_down_predicate=f"(partitioncreateddate>=to_date('{first_day_past}') and partitioncreateddate<to_date('{first_day_next_month}'))",
    transformation_ctx="red_ecd",
)

contactrequests_daily_cr_per_classified = glueContext.create_dynamic_frame.from_catalog(
    database="kinesis",
    table_name="contactrequests_daily_cr_per_classified",
    push_down_predicate=f"(partitioncreateddate>=to_date('{first_day_current_month}') and partitioncreateddate<to_date('{first_day_next_month}'))",
    transformation_ctx="contactrequests_daily_cr_per_classified",
)

customeractions_daily_actions_per_classified = glueContext.create_dynamic_frame.from_catalog(
    database="kinesis",
    table_name="customeractions_daily_actions_per_classified",
    push_down_predicate=f"(partitioncreateddate>=to_date('{first_day_current_month}') and partitioncreateddate<to_date('{first_day_next_month}'))",
    transformation_ctx="customeractions_daily_actions_per_classified",
)
# end of getting base tables

union_df = None


# loop over possible values
for row in config["countryValues"]:
    geoid = row["geoid"]
    country_name = row["country_name"]
    distribution_type = row["distribution_type"]
    data_source = row["data_source"]
    logger.error(
        f"DEBUG: Getting data for: {geoid, country_name, distribution_type, data_source}"
    )

    queries_obj = Queries(
        distribution_type, geoid, first_day_current_month, first_day_next_month
    )

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
    logger.error("DEBUG: Done fetching base data first")

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
    logger.error("DEBUG: Done fetching base data cached")

    BaseData_final_df = sparkSqlQuery(
        glueContext=glueContext,
        query=queries_obj.get_BaseData_final_df_query(),
        mapping={
            "BaseDataFirst": BaseDataFirst,
            "BaseData": BaseData,
        },
        transformation_ctx="BaseData_final_df",
    )
    logger.error("DEBUG: Done fetching base data final")

    BaseData_final_df = BaseData_final_df.drop_fields(
        paths=config["colsToDropBaseData"]
    )

    BaseData_final_df = modify_data(
        glueContext=glueContext,
        df=BaseData_final_df,
        geoid=geoid,
        partition_month=partition_month,
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

    logger.error("DEBUG: Union df done")

    csv_df = BaseData_final_df.drop_fields(paths=config["colsToDropJson"])
    json_df = Helper.modify_data_json(
        glueContext=glueContext, df=csv_df, distribution_type=distribution_type
    )

    s3_path_json = f"s3://consume-batch-ma-with-cr-ecd-{ENV_NAME}/data/{country_name.lower()}/{distribution_type.lower()}/json/partitioncreateddate={partition_date}"
    AmazonS3_node1714127201181 = glueContext.write_dynamic_frame.from_options(
        frame=json_df,
        connection_type="s3",
        format="json",
        connection_options={"compression": "gzip", "path": s3_path_json},
        transformation_ctx="AmazonS3_node1714127201181",
    )

    s3_path_csv = f"s3://consume-batch-ma-with-cr-ecd-{ENV_NAME}/data/{country_name.lower()}/{distribution_type.lower()}/csv/partitioncreateddate={partition_date}"
    AmazonS3_node1714127201182 = glueContext.write_dynamic_frame.from_options(
        frame=csv_df.coalesce(1),
        connection_type="s3",
        format="csv",
        connection_options={"compression": "gzip", "path": s3_path_csv},
        transformation_ctx="AmazonS3_node1714127201181",
    )
    logger.error("DEBUG: Done with json")

# delete insert instead of replacewhere
glueContext.purge_table(
    database="kafka",
    table_name="offers_ma_geo",
    options={
        "partitionPredicate": f"(partitionmonth == '{partition_month}')",
        "retentionPeriod": 1,
    },
)
logger.error("DEBUG: Done purging table")

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
logger.error("DEBUG: Done writing to table")

job.commit()

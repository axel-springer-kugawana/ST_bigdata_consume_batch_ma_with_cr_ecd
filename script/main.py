import sys
from pyspark.sql import functions as F
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3

from helper import GlobalVariables, Queries, Helper


def parse_env_name() -> str:
    client = boto3.client('iam')

    account_alliases = client.list_account_aliases()
    acc_name = account_alliases.get('AccountAliases')[0]
    env_name = acc_name.split('-')[-1]
    return env_name

ENV_NAME = parse_env_name()

def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql("(select * from source1) UNION " + unionType + " (select * from source2)")
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

def modifyData(glueContext, df, geoid) -> DynamicFrame:
    ret_df = df.toDF()

    # clean values
    ret_df = ret_df.withColumn("cleaned_classified_structure_rooms_numberofrooms", F.round(F.col("cleaned_classified_structure_rooms_numberofrooms")).cast('int'))\
                   .withColumn("classified_geo_city", F.regexp_replace(F.col("classified_geo_city"), "\\\\\\\\",""))\
    
    # rename columns
    cols_to_rename = [c for c in ret_df.columns if c.startswith('cleaned_')]
    for old_name in cols_to_rename:
        new_name = old_name.replace('cleaned_', '')
        ret_df = ret_df.withColumnRenamed(old_name, new_name)

    if geoid == 108:
        bundeslaender_df = spark.read.csv('bundeslaender.csv', header=True, inferSchema="true")
        stadtlandkreise_df = spark.read.csv('stadtlandkreise.csv', header=True, inferSchema="true")

        ret_df = ret_df.join(bundeslaender_df, F.substring(ret_df.classified_geo_countrySpecific_de_iwtLegacyGeoID,1,5)==bundeslaender_df.geoid,how='left')\
                        .drop('geoid')\
                        .join(stadtlandkreise_df, F.substring(ret_df.classified_geo_countrySpecific_de_iwtLegacyGeoID,1,8)==stadtlandkreise_df.geoid,how='left')\
                        .drop('geoid','stadtkreis')\
                        .withColumnRenamed('bundesland','geo_state')\
                        .withColumnRenamed('landkreis','geo_userDefined_immoWelt_county')
    else:
        ret_df = ret_df.withColumn('geo_state', F.lit(None))
        ret_df = ret_df.withColumn('geo_userDefined_immoWelt_county', F.lit(None))
    
    ret_df = ret_df.drop('geoid','classified_geo_userDefined_immoWelt_geoid','classified_geo_countrySpecific_de_iwtLegacyGeoID')

    # add additional columns
    ret_df = ret_df\
          .withColumn("partitionGeoid",F.lit(geoid))\
          .withColumn("partitionMonth",F.lit(GlobalVariables.partition_month))
    
    return DynamicFrame.fromDF(ret_df, glueContext, 'attributes')

# Construct the when clause dynamically
def add_column_from_mapping(df, mapping, column, otherwise):
    # Construct the when clause dynamically
    when_clause = None
    for estate_type, sub_type_col in mapping.items():
        condition = (F.col("classified_estateType") == estate_type)
        if when_clause is None:
            when_clause = F.when(condition & F.col(sub_type_col).isNotNull(), F.col(sub_type_col))
        else:
            when_clause = when_clause.when(condition & F.col(sub_type_col).isNotNull(), F.col(sub_type_col))
        when_clause = when_clause.when(condition & F.col(sub_type_col).isNull(), F.lit(estate_type))

    # Add the otherwise clause
    when_clause = when_clause.otherwise(F.lit(otherwise))
    df = df.withColumn(column, when_clause)
    return df
    
def modifyDataJson(glueContext, df, distribution_type) ->  DynamicFrame:
    ret_df = df.toDF()
    ret_df = add_column_from_mapping(
        df=ret_df,
        mapping=Helper.estate_type_mapping, 
        column="subTypes",
        otherwise="NOT_APPLICABLE"
    )
    
    if distribution_type == 'BUY':
        ret_df = ret_df.withColumn("prices_buy_price_amount",F.col("classified_prices_buy_price_amount"))\
                    .withColumn("prices_buy_price_currency",F.col("classified_prices_currency"))\
                    .withColumn("prices_buy_serviceCharge_amount",
                                    F.when(F.col("classified_prices_buy_operatingCosts_amount").isNotNull(),F.col ("classified_prices_buy_operatingCosts_amount"))
                                    .otherwise(0.00).cast("float")
                                )\
                    .withColumn("prices_buy_serviceCharge_currency",F.col("classified_prices_currency"))
    else:  # RENT
        ret_df = ret_df.withColumn("prices_rent_baseRent_amount",F.col("classified_prices_rent_baseRent_amount"))\
                    .withColumn("prices_rent_baseRent_currency",F.col("classified_prices_currency"))\
                    .withColumn("prices_rent_operatingCosts_amount",F.col("classified_prices_rent_operatingCosts_amount"))\
                    .withColumn("prices_rent_operatingCosts_currency",F.col("classified_prices_currency"))
    
    # special columns
    ret_df = ret_df.withColumn("spaces_residential_floorNo",F.when(F.col("classified_structure_building_floorNumber").isNotNull(),
                                                                          F.col("classified_structure_building_floorNumber"))
                                                                          .otherwise(0)
                                                                          )\
                    .withColumn("spaces_parking_garage",F.when(F.col("classified_structure_parkingLots_garage").isNotNull(),
                                                                          F.col("classified_structure_parkingLots_garage"))
                                                                          .otherwise(0))\
                    .withColumn("spaces_parking_carport",F.when(F.col("classified_structure_parkingLots_carport").isNotNull(),
                                                                          F.col("classified_structure_parkingLots_carport"))
                                                                          .otherwise(0))\
                    .withColumn("spaces_parking_undergroundGarage",F.when(F.col("classified_structure_parkingLots_underground").isNotNull(),
                                                                          F.col("classified_structure_parkingLots_underground"))
                                                                          .otherwise(0))\
                    .withColumn("spaces_parking_duplex",F.when(F.col("classified_structure_parkingLots_duplex").isNotNull(),
                                                                          F.col("classified_structure_parkingLots_duplex"))
                                                                          .otherwise(0))\
                    .withColumn("spaces_parking_carPark",F.when(F.col("classified_structure_parkingLots_carPark").isNotNull(),
                                                                          F.col("classified_structure_parkingLots_carPark"))
                                                                          .otherwise(0))\
                    .withColumn("spaces_parking_mixed",
                                (F.when(F.col("classified_structure_parkingLots_mixed").isNotNull(),
                                        F.col("classified_structure_parkingLots_mixed")).otherwise(0)) +
                                (F.when(F.col("classified_structure_parkingLots_outside").isNotNull(),
                                        F.col("classified_structure_parkingLots_outside")).otherwise(0)) +
                                (F.when(F.col("classified_structure_parkingLots_streetParking").isNotNull(),
                                        F.col("classified_structure_parkingLots_streetParking")).otherwise(0)) +
                                (F.when(F.col("classified_structure_parkingLots_doubleGarage").isNotNull(),
                                        F.col("classified_structure_parkingLots_doubleGarage")).otherwise(0)) +
                                (F.when(F.col("classified_structure_parkingLots_garageArea").isNotNull(),
                                        F.col("classified_structure_parkingLots_garageArea")).otherwise(0)) +
                                (F.when(F.col("classified_structure_parkingLots_boatDock").isNotNull(),
                                        F.col("classified_structure_parkingLots_boatDock")).otherwise(0))   
                               )\
                    .withColumn("attributes_roomCount_numberOfRooms",F.col("classified_structure_rooms_numberOfRooms"))\
                    .withColumn("attributes_roomCount_numberOfTerraces",F.when(F.col("classified_structure_rooms_numberOfTerraces").isNotNull(),
                                                                          F.col("classified_structure_rooms_numberOfTerraces").cast("int"))
                                                                          .otherwise(0))\
                    .withColumn("attributes_roomCount_numberOfBalconies",F.when(F.col("classified_structure_rooms_numberOfBalconies").isNotNull(),
                                                                          F.col("classified_structure_rooms_numberOfBalconies").cast("int"))
                                                                          .otherwise(0))\
                   .withColumn("features_elevator_person",F.when(F.col("classified_structure_building_elevator_person").isNotNull(),
                                                                          F.col("classified_structure_building_elevator_person"))
                                                                          .otherwise("NO"))\
                    .withColumn("features_garden",F.when(F.col("classified_features_garden").isNotNull(),
                                                                          F.col("classified_features_garden"))
                                                                          .otherwise("NO"))\
                    .withColumn("features_bath_bathtub",F.when(F.col("classified_structure_building_bath_bathtub").isNotNull(),
                                                                          F.col("classified_structure_building_bath_bathtub"))
                                                                          .otherwise("NO"))\
                    .withColumn("features_bath_shower",F.when(F.col("classified_structure_building_bath_shower").isNotNull(),
                                                                          F.col("classified_structure_building_bath_shower"))
                                                                          .otherwise("NO"))\
                    .withColumn("features_bath_window",F.when(F.col("classified_structure_building_bath_window").isNotNull(),
                                                                          F.col("classified_structure_building_bath_window"))
                                                                          .otherwise("NO"))\
                    .withColumn("features_bath_guestToilet",F.when(F.col("classified_structure_building_bath_guestToilet").isNotNull(),
                                                                          F.col("classified_structure_building_bath_guestToilet"))
                                                                          .otherwise("NO"))\
                    .withColumn("features_bath_separateBathAndToilet",F.when(F.col("classified_structure_building_bath_separateBathAndToilet").isNotNull(),
                                                                          F.col("classified_structure_building_bath_separateBathAndToilet"))
                                                                          .otherwise("NO"))\
                    .withColumn("features_kitchen_kitchenType",F.when(F.col("classified_structure_building_kitchen_kitchenType").isNotNull(),
                                                                          F.col("classified_structure_building_kitchen_kitchenType"))
                                                                          .otherwise("NONE"))\
                    .withColumn("features_residential_loggia",F.when(F.col("classified_features_residential_loggia").isNotNull(),
                                                                                F.col("classified_features_residential_loggia"))
                                                                                .otherwise("NO"))\
                    .withColumn("features_residential_winterGarden",F.lit("NO"))\
                    .withColumn("features_residential_rooftopTerrace",F.lit("NO"))\
                    .withColumn("features_residential_chimney",F.when(F.col("classified_features_residential_chimney").isNotNull(),
                                                                          F.col("classified_features_residential_chimney"))
                                                                          .otherwise("NO"))\
                   .withColumn("features_furnished",F.when(F.col("classified_features_furnished").isNotNull(),
                                                                          F.col("classified_features_furnished"))
                                                                          .otherwise("NOT_APPLICABLE"))\
                    .withColumn("conditions_userDefined_immoWelt_research_ageState",
                                F.col("userDefined_immoWelt_research_ageState"))\
                    .withColumn("conditions_userDefined_immoWelt_research_buildState",
                                F.col("userDefined_immoWelt_research_buildState"))\
                    .withColumn("energy_energyType_heatMethod",F.when(F.col("classified_energy_energyType_heatMethod").isNotNull(),
                                                                          F.col("classified_energy_energyType_heatMethod"))
                                                                          .otherwise("NOT_APPLICABLE"))\
                    .withColumn("energy_energyType_heatForm_stove",F.when(F.col("classified_energy_energyType_heatForm_stove").isNotNull(),
                                                                          F.col("classified_energy_energyType_heatForm_stove"))
                                                                          .otherwise(False))\
                    .withColumn("energy_energyType_heatForm_floor",F.when(F.col("classified_energy_energyType_heatForm_floor").isNotNull(),
                                                                          F.col("classified_energy_energyType_heatForm_floor"))
                                                                          .otherwise(False))\
                    .withColumn("energy_energyType_energyGeneration",F.when(F.col("classified_energy_energyType_energyGeneration").isNotNull(),
                                                                          F.col("classified_energy_energyType_energyGeneration"))
                                                                          .otherwise("NOT_APPLICABLE"))\
                    .withColumn("energy_energyType_energySource",
                                F.when(F.col("classified_energy_energyType_energySource_coal").isNotNull(),F.lit("coal"))
                                .when(F.col("classified_energy_energyType_energySource_oil").isNotNull(),F.lit("oil"))
                                .when(F.col("classified_energy_energyType_energySource_gas").isNotNull(),F.lit("gas"))
                                .when(F.col("classified_energy_energyType_energySource_electric").isNotNull(),F.lit("electric"))
                                .when(F.col("classified_energy_energyType_energySource_solarHeat").isNotNull(),F.lit("solarHeat"))
                                .when(F.col("classified_energy_energyType_energySource_geothermal").isNotNull(),F.lit("geothermal"))
                                .when(F.col("classified_energy_energyType_energySource_wood").isNotNull(),F.lit("wood"))
                                .when(F.col("classified_energy_energyType_energySource_woodPellet").isNotNull(),F.lit("woodPellet"))
                                .otherwise(F.lit("NOT_APPLICABLE"))
                               )\
                    .withColumn("userDefined_immoWelt_contact-requests",
                                F.when(F.col("userDefined_immoWelt_contact_requests").isNotNull(),
                                       F.col("userDefined_immoWelt_contact_requests"))
                                .otherwise(0))\
                    .withColumn("userDefined_immoWelt_iw-contact-requests",
                                F.when(F.col("userDefined_immoWelt_iw_contact_requests").isNotNull(),
                                       F.col("userDefined_immoWelt_iw_contact_requests"))
                                .otherwise(0))\
                    .withColumn("userDefined_immoWelt_in-contact-requests",
                                F.when(F.col("userDefined_immoWelt_in_contact_requests").isNotNull(),
                                       F.col("userDefined_immoWelt_in_contact_requests"))
                                .otherwise(0))\
                    .withColumn("userDefined_immoWelt_expose-visits",
                                F.when(F.col("userDefined_immoWelt_expose_visits").isNotNull(),
                                       F.col("userDefined_immoWelt_expose_visits"))
                                .otherwise(0))\
                    .withColumn("userDefined_immoWelt_iw-expose-visits",
                                F.when(F.col("userDefined_immoWelt_iw_expose_visits").isNotNull(),
                                       F.col("userDefined_immoWelt_iw_expose_visits"))
                                .otherwise(0))\
                    .withColumn("userDefined_immoWelt_in-expose-visits",
                                F.when(F.col("userDefined_immoWelt_in_expose_visits").isNotNull(),
                                       F.col("userDefined_immoWelt_in_expose_visits"))
                                .otherwise(0))  
    
    ### ADDING MISSING ATTRIBUTES
    for colname in GlobalVariables.cols_remove_classified:
        ret_df = ret_df.withColumnRenamed(
            f'classified_{colname}',
            colname,
        )

    cols_to_drop = [c for c in ret_df.columns if c.startswith('classified_') or c.startswith('userDefined_')]
    ret_df = ret_df.drop(*cols_to_drop)

    select_col_list = Helper.nest_schema(ret_df.schema)
    ret_df = ret_df.select(*select_col_list)
    if 'subTypes' in ret_df.columns:
        ret_df = Helper.construct_MapType(ret_df, key_col="estateType", value_col="subTypes", new_col_name="subTypes")

    ret_df = ret_df.coalesce(1)

    return DynamicFrame.fromDF(ret_df, glueContext, 'json')


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

########## get base tables from data catalog
red_red_cleaned = glueContext.create_dynamic_frame.from_catalog(
    database="kafka",
    table_name="red_red_cleaned",
    push_down_predicate=f"(partitioncreateddate>=to_date('{GlobalVariables.first_day_3_months_ago}') and partitioncreateddate<to_date('{GlobalVariables.first_day_next_month}'))",
    transformation_ctx="red_red_cleaned"
)

red_vd_cleaned_spark = glueContext.create_data_frame.from_catalog(
    database="kafka",
    table_name="red_vd_cleaned",
    transformation_ctx="red_vd_cleaned_spark"
)
red_vd_cleaned = DynamicFrame.fromDF(red_vd_cleaned_spark, glueContext, 'red_vd_cleaned')

red_ecd = glueContext.create_dynamic_frame.from_catalog(
    database="kafka",
    table_name="red_ecd_raw",
    push_down_predicate=f"(partitioncreateddate>=to_date('{GlobalVariables.first_day_3_months_ago}') and partitioncreateddate<to_date('{GlobalVariables.first_day_next_month}'))",
    transformation_ctx="red_ecd"
)

contactrequests_daily_cr_per_classified = glueContext.create_dynamic_frame.from_catalog(
    database="kinesis",
    table_name="contactrequests_daily_cr_per_classified",
    push_down_predicate=f"(partitioncreateddate>=to_date('{GlobalVariables.first_day_current_month}') and partitioncreateddate<to_date('{GlobalVariables.first_day_next_month}'))",
    transformation_ctx="contactrequests_daily_cr_per_classified"
)

customeractions_daily_actions_per_classified = glueContext.create_dynamic_frame.from_catalog(
    database="kinesis",
    table_name="customeractions_daily_actions_per_classified",
    push_down_predicate=f"(partitioncreateddate>=to_date('{GlobalVariables.first_day_current_month}') and partitioncreateddate<to_date('{GlobalVariables.first_day_next_month}'))",
    transformation_ctx="customeractions_daily_actions_per_classified"
)
########## 

# MAIN

# GEOID, DATA_COUNTRY, DISTRIBUTION_TYPE, DATA_SOURCE
country_values = [
    (108, 'GERMANY', 'BUY', 'SELL'),
    (103, 'AUSTRIA', 'BUY', 'SELL'),
    #(108, 'GERMANY', 'RENT', 'RENTAL'),
    #(103, 'AUSTRIA', 'RENT', 'RENTAL'),
]

union_df = None
for geoid, data_country, distribution_type, data_source in country_values:
    print(f'Getting data for: {geoid, data_country, distribution_type, data_source}')
    queries_obj = Queries(
        distribution_type=distribution_type,
        geoid=geoid
    )

    BaseData_df = sparkSqlQuery(
        glueContext=glueContext,
        query=queries_obj.get_BaseData_df_query(),
        mapping={
            "red_red_cleaned": red_red_cleaned,
            "red_ecd": red_ecd,
            "red_vd_cleaned": red_vd_cleaned,
            "contactrequests_daily_cr_per_classified": contactrequests_daily_cr_per_classified,
            "customeractions_daily_actions_per_classified": customeractions_daily_actions_per_classified
        },
        transformation_ctx="BaseData_df",
    )
    print('Done fetching base data')
    BaseData_df = BaseData_df.drop_fields(paths=GlobalVariables.cols_to_drop_basedata)
    BaseData_df = modifyData(
        glueContext=glueContext, 
        df=BaseData_df,
        geoid=geoid,
    )

    if union_df is None:
        union_df = BaseData_df
    else:
        union_df = sparkUnion(
            glueContext,
            unionType = "ALL",
            mapping = {
                "source1": union_df,
                "source2": BaseData_df
            },
            transformation_ctx = "Union_node"
        )

    print('Union df done')

    csv_df = BaseData_df.drop_fields(paths=GlobalVariables.cols_to_drop_json)
    json_df = modifyDataJson(
        glueContext=glueContext, 
        df=csv_df,
        distribution_type=distribution_type
    )

    s3_path_json = f"s3://consume-batch-ma-with-cr-ecd-{ENV_NAME}/data/{data_country.lower()}/{distribution_type.lower()}/json/partitioncreateddate={GlobalVariables.today}"
    AmazonS3_node1714127201181 = glueContext.write_dynamic_frame.from_options(
        frame=json_df, 
        connection_type="s3", 
        format="json",
        connection_options={
            "compression": "gzip",
            "path": s3_path_json
        },
        transformation_ctx="AmazonS3_node1714127201181"
    )

    s3_path_csv = f"s3://consume-batch-ma-with-cr-ecd-{ENV_NAME}/data/{data_country.lower()}/{distribution_type.lower()}/csv/partitioncreateddate={GlobalVariables.today}"
    AmazonS3_node1714127201182 = glueContext.write_dynamic_frame.from_options(
        frame=csv_df.coalesce(1),
        connection_type="s3",
        format="csv", 
        connection_options={
            "compression": "gzip",
            "path": s3_path_csv
        },
        transformation_ctx="AmazonS3_node1714127201181"
    )
    print('Done with json')

# delete insert instead of replacewhere
glueContext.purge_table(
    database="kafka", 
    table_name="offers_ma_geo", 
    options={
        "partitionPredicate": f"(partitionmonth == '{GlobalVariables.partition_month}')", 
        "retentionPeriod": 1
    }
)
print("Done purging table")

print('Final schema:')
union_df.printSchema()

AWSGlueDataCatalog_node1709799333156 = glueContext.write_dynamic_frame.from_catalog(
    frame=union_df, 
    database="kafka", 
    table_name="offers_ma_geo", 
    additional_options={
        "enableUpdateCatalog": True, 
        "updateBehavior": "UPDATE_IN_DATABASE", 
        "partitionKeys": ["partitionMonth"]
    }, 
    transformation_ctx="AWSGlueDataCatalog_node1709799333156"
)
print('Done writing to table')

job.commit()

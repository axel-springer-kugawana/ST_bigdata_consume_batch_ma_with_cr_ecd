import os
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from awsglue import DynamicFrame
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, struct


class Helper:

    estate_type_mapping = {
        "HOUSE": "classified_estateSubTypes_house",
        "APARTMENT": "classified_estateSubTypes_apartment",
    }

    @staticmethod
    def nest_schema(schema, separator="_"):
        """Converts a flattened schema to a nested schema."""

        def build_tree(schema):
            """Groups fields within columns into a tree (represented as a dictionary)."""
            result = {}
            for field in schema.fields:
                n_columns = len(field.name.split(separator))
                current = result
                if n_columns == 1:  # Column is a single field
                    current[field.name] = field.name
                else:  # Column contains multiple fields
                    for i, column_name in enumerate(field.name.split(separator)):
                        if i == n_columns - 1:  # Last field
                            current[column_name] = field.name
                        else:
                            if column_name not in current:
                                current[column_name] = {}
                            current = current[column_name]
            return result

        def to_columns(tree):
            """Convert the tree into a column selection expression."""
            for name, value in tree.items():
                if type(value) == dict:
                    yield struct(list(to_columns(value))).alias(name)
                else:
                    yield col(value).alias(name)

        tree = build_tree(schema)

        return list(to_columns(tree))

    @staticmethod
    def construct_MapType(df, key_col, value_col, new_col_name):
        df = df.withColumn(
            new_col_name,
            F.when(
                col(value_col).isNotNull(),
                F.create_map(F.lower(col(key_col)), col(value_col)),
            ).otherwise(F.create_map(F.lower(col(key_col)), F.lit("NOT_APPLICABLE"))),
        )
        return df

    @staticmethod
    def read_and_format_sql_query(file_path, **kwargs):
        with open(file_path, "r") as f:
            sql_template = f.read()

        # Replace placeholders in the SQL template with actual values
        formatted_sql = sql_template.format(**kwargs)

        return formatted_sql

    @staticmethod
    # Construct the when clause dynamically
    def add_column_from_mapping(df, mapping, column, otherwise):
        # Construct the when clause dynamically
        when_clause = None
        for estate_type, sub_type_col in mapping.items():
            condition = F.col("classified_estateType") == estate_type
            if when_clause is None:
                when_clause = F.when(
                    condition & F.col(sub_type_col).isNotNull(), F.col(sub_type_col)
                )
            else:
                when_clause = when_clause.when(
                    condition & F.col(sub_type_col).isNotNull(), F.col(sub_type_col)
                )
            when_clause = when_clause.when(
                condition & F.col(sub_type_col).isNull(), F.lit(estate_type)
            )

        # Add the otherwise clause
        when_clause = when_clause.otherwise(F.lit(otherwise))
        df = df.withColumn(column, when_clause)
        return df

    @staticmethod
    def modifyDataJson(glueContext, df, distribution_type) -> DynamicFrame:
        ret_df = df.toDF()
        ret_df = Helper.add_column_from_mapping(
            df=ret_df,
            mapping=Helper.estate_type_mapping,
            column="subTypes",
            otherwise="NOT_APPLICABLE",
        )

        if distribution_type == "BUY":
            ret_df = (
                ret_df.withColumn(
                    "prices_buy_price_amount",
                    F.col("classified_prices_buy_price_amount"),
                )
                .withColumn(
                    "prices_buy_price_currency", F.col("classified_prices_currency")
                )
                .withColumn(
                    "prices_buy_serviceCharge_amount",
                    F.when(
                        F.col(
                            "classified_prices_buy_operatingCosts_amount"
                        ).isNotNull(),
                        F.col("classified_prices_buy_operatingCosts_amount"),
                    )
                    .otherwise(0.00)
                    .cast("float"),
                )
                .withColumn(
                    "prices_buy_serviceCharge_currency",
                    F.col("classified_prices_currency"),
                )
            )
        else:  # RENT
            ret_df = (
                ret_df.withColumn(
                    "prices_rent_baseRent_amount",
                    F.col("classified_prices_rent_baseRent_amount"),
                )
                .withColumn(
                    "prices_rent_baseRent_currency", F.col("classified_prices_currency")
                )
                .withColumn(
                    "prices_rent_operatingCosts_amount",
                    F.col("classified_prices_rent_operatingCosts_amount"),
                )
                .withColumn(
                    "prices_rent_operatingCosts_currency",
                    F.col("classified_prices_currency"),
                )
            )

        # special columns
        ret_df = (
            ret_df.withColumn(
                "spaces_residential_livingSpace",
                F.when(
                    (F.col("classified_spaces_residential_livingSpace").isNotNull())
                    & (
                        F.trim(F.col("classified_spaces_residential_livingSpace")) != ""
                    ),
                    F.col("classified_spaces_residential_livingSpace"),
                )
                .otherwise(0.0)
                .cast("float"),
            )
            .withColumn(
                "spaces_residential_plotSpace",
                F.when(
                    (F.col("classified_spaces_residential_plotSpace").isNotNull())
                    & (F.trim(F.col("classified_spaces_residential_plotSpace")) != ""),
                    F.col("classified_spaces_residential_plotSpace"),
                )
                .otherwise(0.0)
                .cast("float"),
            )
            .withColumn(
                "spaces_residential_floorNo",
                F.when(
                    F.col("classified_structure_building_floorNumber").isNotNull(),
                    F.col("classified_structure_building_floorNumber"),
                ).otherwise(0),
            )
            .withColumn(
                "spaces_parking_garage",
                F.when(
                    F.col("classified_structure_parkingLots_garage").isNotNull(),
                    F.col("classified_structure_parkingLots_garage"),
                ).otherwise(0),
            )
            .withColumn(
                "spaces_parking_carport",
                F.when(
                    F.col("classified_structure_parkingLots_carport").isNotNull(),
                    F.col("classified_structure_parkingLots_carport"),
                ).otherwise(0),
            )
            .withColumn(
                "spaces_parking_undergroundGarage",
                F.when(
                    F.col("classified_structure_parkingLots_underground").isNotNull(),
                    F.col("classified_structure_parkingLots_underground"),
                ).otherwise(0),
            )
            .withColumn(
                "spaces_parking_duplex",
                F.when(
                    F.col("classified_structure_parkingLots_duplex").isNotNull(),
                    F.col("classified_structure_parkingLots_duplex"),
                ).otherwise(0),
            )
            .withColumn(
                "spaces_parking_carPark",
                F.when(
                    F.col("classified_structure_parkingLots_carPark").isNotNull(),
                    F.col("classified_structure_parkingLots_carPark"),
                ).otherwise(0),
            )
            .withColumn(
                "spaces_parking_mixed",
                (
                    F.when(
                        F.col("classified_structure_parkingLots_mixed").isNotNull(),
                        F.col("classified_structure_parkingLots_mixed"),
                    ).otherwise(0)
                )
                + (
                    F.when(
                        F.col("classified_structure_parkingLots_outside").isNotNull(),
                        F.col("classified_structure_parkingLots_outside"),
                    ).otherwise(0)
                )
                + (
                    F.when(
                        F.col(
                            "classified_structure_parkingLots_streetParking"
                        ).isNotNull(),
                        F.col("classified_structure_parkingLots_streetParking"),
                    ).otherwise(0)
                )
                + (
                    F.when(
                        F.col(
                            "classified_structure_parkingLots_doubleGarage"
                        ).isNotNull(),
                        F.col("classified_structure_parkingLots_doubleGarage"),
                    ).otherwise(0)
                )
                + (
                    F.when(
                        F.col(
                            "classified_structure_parkingLots_garageArea"
                        ).isNotNull(),
                        F.col("classified_structure_parkingLots_garageArea"),
                    ).otherwise(0)
                )
                + (
                    F.when(
                        F.col("classified_structure_parkingLots_boatDock").isNotNull(),
                        F.col("classified_structure_parkingLots_boatDock"),
                    ).otherwise(0)
                ),
            )
            .withColumn(
                "attributes_roomCount_numberOfRooms",
                F.col("classified_structure_rooms_numberOfRooms"),
            )
            .withColumn(
                "attributes_roomCount_numberOfTerraces",
                F.when(
                    F.col("classified_structure_rooms_numberOfTerraces").isNotNull(),
                    F.col("classified_structure_rooms_numberOfTerraces").cast("int"),
                ).otherwise(0),
            )
            .withColumn(
                "attributes_roomCount_numberOfBalconies",
                F.when(
                    F.col("classified_structure_rooms_numberOfBalconies").isNotNull(),
                    F.col("classified_structure_rooms_numberOfBalconies").cast("int"),
                ).otherwise(0),
            )
            .withColumn(
                "features_elevator_person",
                F.when(
                    F.col("classified_structure_building_elevator_person").isNotNull(),
                    F.col("classified_structure_building_elevator_person"),
                ).otherwise("NO"),
            )
            .withColumn(
                "features_garden",
                F.when(
                    F.col("classified_features_garden").isNotNull(),
                    F.col("classified_features_garden"),
                ).otherwise("NO"),
            )
            .withColumn(
                "features_bath_bathtub",
                F.when(
                    F.col("classified_structure_building_bath_bathtub").isNotNull(),
                    F.col("classified_structure_building_bath_bathtub"),
                ).otherwise("NO"),
            )
            .withColumn(
                "features_bath_shower",
                F.when(
                    F.col("classified_structure_building_bath_shower").isNotNull(),
                    F.col("classified_structure_building_bath_shower"),
                ).otherwise("NO"),
            )
            .withColumn(
                "features_bath_window",
                F.when(
                    F.col("classified_structure_building_bath_window").isNotNull(),
                    F.col("classified_structure_building_bath_window"),
                ).otherwise("NO"),
            )
            .withColumn(
                "features_bath_guestToilet",
                F.when(
                    F.col("classified_structure_building_bath_guestToilet").isNotNull(),
                    F.col("classified_structure_building_bath_guestToilet"),
                ).otherwise("NO"),
            )
            .withColumn(
                "features_bath_separateBathAndToilet",
                F.when(
                    F.col(
                        "classified_structure_building_bath_separateBathAndToilet"
                    ).isNotNull(),
                    F.col("classified_structure_building_bath_separateBathAndToilet"),
                ).otherwise("NO"),
            )
            .withColumn(
                "features_kitchen_kitchenType",
                F.when(
                    F.col(
                        "classified_structure_building_kitchen_kitchenType"
                    ).isNotNull(),
                    F.col("classified_structure_building_kitchen_kitchenType"),
                ).otherwise("NONE"),
            )
            .withColumn(
                "features_residential_loggia",
                F.when(
                    F.col("classified_features_residential_loggia").isNotNull(),
                    F.col("classified_features_residential_loggia"),
                ).otherwise("NO"),
            )
            .withColumn("features_residential_winterGarden", F.lit("NO"))
            .withColumn("features_residential_rooftopTerrace", F.lit("NO"))
            .withColumn(
                "features_residential_chimney",
                F.when(
                    F.col("classified_features_residential_chimney").isNotNull(),
                    F.col("classified_features_residential_chimney"),
                ).otherwise("NO"),
            )
            .withColumn(
                "features_furnished",
                F.when(
                    F.col("classified_features_furnished").isNotNull(),
                    F.col("classified_features_furnished"),
                ).otherwise("NOT_APPLICABLE"),
            )
            .withColumn(
                "conditions_userDefined_immoWelt_research_ageState",
                F.col("userDefined_immoWelt_research_ageState"),
            )
            .withColumn(
                "conditions_userDefined_immoWelt_research_buildState",
                F.col("userDefined_immoWelt_research_buildState"),
            )
            .withColumn(
                "energy_energyType_heatMethod",
                F.when(
                    F.col("classified_energy_energyType_heatMethod").isNotNull(),
                    F.col("classified_energy_energyType_heatMethod"),
                ).otherwise("NOT_APPLICABLE"),
            )
            .withColumn(
                "energy_energyType_heatForm_stove",
                F.when(
                    F.col("classified_energy_energyType_heatForm_stove").isNotNull(),
                    F.col("classified_energy_energyType_heatForm_stove"),
                ).otherwise(False),
            )
            .withColumn(
                "energy_energyType_heatForm_floor",
                F.when(
                    F.col("classified_energy_energyType_heatForm_floor").isNotNull(),
                    F.col("classified_energy_energyType_heatForm_floor"),
                ).otherwise(False),
            )
            .withColumn(
                "energy_energyType_energyGeneration",
                F.when(
                    F.col("classified_energy_energyType_energyGeneration").isNotNull(),
                    F.col("classified_energy_energyType_energyGeneration"),
                ).otherwise("NOT_APPLICABLE"),
            )
            .withColumn(
                "energy_energyType_energySource",
                F.when(
                    F.col("classified_energy_energyType_energySource_coal").isNotNull(),
                    F.lit("coal"),
                )
                .when(
                    F.col("classified_energy_energyType_energySource_oil").isNotNull(),
                    F.lit("oil"),
                )
                .when(
                    F.col("classified_energy_energyType_energySource_gas").isNotNull(),
                    F.lit("gas"),
                )
                .when(
                    F.col(
                        "classified_energy_energyType_energySource_electric"
                    ).isNotNull(),
                    F.lit("electric"),
                )
                .when(
                    F.col(
                        "classified_energy_energyType_energySource_solarHeat"
                    ).isNotNull(),
                    F.lit("solarHeat"),
                )
                .when(
                    F.col(
                        "classified_energy_energyType_energySource_geothermal"
                    ).isNotNull(),
                    F.lit("geothermal"),
                )
                .when(
                    F.col("classified_energy_energyType_energySource_wood").isNotNull(),
                    F.lit("wood"),
                )
                .when(
                    F.col(
                        "classified_energy_energyType_energySource_woodPellet"
                    ).isNotNull(),
                    F.lit("woodPellet"),
                )
                .otherwise(F.lit("NOT_APPLICABLE")),
            )
            .withColumn(
                "userDefined_immoWelt_contact-requests",
                F.when(
                    F.col("userDefined_immoWelt_contact_requests").isNotNull(),
                    F.col("userDefined_immoWelt_contact_requests"),
                ).otherwise(0),
            )
            .withColumn(
                "userDefined_immoWelt_iw-contact-requests",
                F.when(
                    F.col("userDefined_immoWelt_iw_contact_requests").isNotNull(),
                    F.col("userDefined_immoWelt_iw_contact_requests"),
                ).otherwise(0),
            )
            .withColumn(
                "userDefined_immoWelt_in-contact-requests",
                F.when(
                    F.col("userDefined_immoWelt_in_contact_requests").isNotNull(),
                    F.col("userDefined_immoWelt_in_contact_requests"),
                ).otherwise(0),
            )
            .withColumn(
                "userDefined_immoWelt_expose-visits",
                F.when(
                    F.col("userDefined_immoWelt_expose_visits").isNotNull(),
                    F.col("userDefined_immoWelt_expose_visits"),
                ).otherwise(0),
            )
            .withColumn(
                "userDefined_immoWelt_iw-expose-visits",
                F.when(
                    F.col("userDefined_immoWelt_iw_expose_visits").isNotNull(),
                    F.col("userDefined_immoWelt_iw_expose_visits"),
                ).otherwise(0),
            )
            .withColumn(
                "userDefined_immoWelt_in-expose-visits",
                F.when(
                    F.col("userDefined_immoWelt_in_expose_visits").isNotNull(),
                    F.col("userDefined_immoWelt_in_expose_visits"),
                ).otherwise(0),
            )
        )

        ### ADDING MISSING ATTRIBUTES
        for colname in GlobalVariables.cols_remove_classified:
            ret_df = ret_df.withColumnRenamed(
                f"classified_{colname}",
                colname,
            )

        cols_to_keep = [
            "userDefined_immoWelt_contact-requests",
            "userDefined_immoWelt_iw-contact-requests",
            "userDefined_immoWelt_in-contact-requests",
            "userDefined_immoWelt_expose-visits",
            "userDefined_immoWelt_iw-expose-visits",
            "userDefined_immoWelt_in-expose-visits",
        ]
        cols_to_drop = [
            c
            for c in ret_df.columns
            if c.startswith("classified_")
            or (c.startswith("userDefined_") and c not in cols_to_keep)
        ]
        ret_df = ret_df.drop(*cols_to_drop)

        # add debugging columns
        ret_df = (
            ret_df.withColumn("metaData_changeLog_timestamp", F.current_timestamp())
            .withColumn("metaData_changeLog_system", F.lit("AwsGlueExportScript"))
            .withColumn("metaData_changeLog_version", F.lit("1.1"))
            .withColumn("metaData_changeLog_operation", F.lit("UPDATE"))
            .withColumn("metaData_changeLog_note", F.lit("Exported to json file"))
        )

        select_col_list = Helper.nest_schema(ret_df.schema)
        ret_df = ret_df.select(*select_col_list)
        if "subTypes" in ret_df.columns:
            ret_df = Helper.construct_MapType(
                ret_df,
                key_col="estateType",
                value_col="subTypes",
                new_col_name="subTypes",
            )

        ret_df = ret_df.coalesce(1)

        return DynamicFrame.fromDF(ret_df, glueContext, "json")


class GlobalVariables:

    DATA_SOURCE = "IWT"
    DATA_KIND = "LISTINGS"
    DATA_SCHEMA = "AVIVIMMO11"

    with open("attributes_all.txt", "r") as f:
        attributes_all = f.read().splitlines()

    cols_to_drop_basedata = ["rank", "rankAll", "rank_all", "rank_current_month"]

    cols_to_drop_json = [
        "id",
        "baseRank",
        "partitionMonth",
        "partitionGeoid",
        "partitionChangeDate",
        "cleanupdataproblems",
    ]

    with open("classified_cols.txt", "r") as f:
        cols_remove_classified = f.read().splitlines()

    @classmethod
    def set_dates(cls, partition_date):
        # First day of current month
        cls.first_day_current_month = partition_date.replace(day=1).strftime("%Y-%m-%d")

        # First day of next month
        cls.first_day_next_month = (
            (partition_date.replace(day=1) + relativedelta(months=+1))
            .replace(day=1)
            .strftime("%Y-%m-%d")
        )

        # First day of 3 months ago
        cls.first_day_3_months_ago = partition_date.replace(day=1) + relativedelta(
            months=-3
        )
        cls.first_day_3_months_ago = cls.first_day_3_months_ago.strftime("%Y-%m-%d")

        cls.partition_month = partition_date.strftime("%Y-%m")


class Variables:

    def __init__(self, distribution_type: str) -> None:
        if distribution_type == "BUY":
            self.price_amount_column = "cleaned_classified_prices_buy_price_amount"
            self.operating_cost_column = (
                "cleaned_classified_prices_buy_operatingCosts_amount"
            )
        else:  # rent
            self.price_amount_column = "cleaned_classified_prices_rent_baseRent_amount"
            self.operating_cost_column = (
                "cleaned_classified_prices_rent_operatingCosts_amount"
            )

        self.attributes_all = GlobalVariables.attributes_all + [
            self.price_amount_column,
            self.operating_cost_column,
        ]
        self.attributes_all_string = ", ".join(self.attributes_all)

        # remove original name and keep only alias
        # e.g. classified_energy_countryspecific_de_energycertificate.heatingconsumption[0] classified_energy_countryspecific_de_energycertificate_heatingconsumption - > classified_energy_countryspecific_de_energycertificate_heatingconsumption

        self.attributes_all_clenaed = [x.split()[-1] for x in self.attributes_all]
        self.attributes_all_cleaned_string = ", ".join(self.attributes_all_clenaed)


class Queries(Variables):

    def __init__(self, distribution_type: str, geoid: int) -> None:
        super().__init__(distribution_type)
        self.distribution_type = distribution_type
        self.geoid = geoid

    @staticmethod
    def get_merge_delete_query(
        extra_columns_wo_prefix, extra_columns_with_prefix
    ) -> str:
        merge_delete_query = Helper.read_and_format_sql_query(
            file_path="merge_delete_query.sql",
            extra_columns_wo_prefix=extra_columns_wo_prefix,
            extra_columns_with_prefix=extra_columns_with_prefix,
            first_day_3_months_ago=GlobalVariables.first_day_3_months_ago,
            first_day_next_month=GlobalVariables.first_day_next_month,
        )

        return merge_delete_query

    def get_BaseData_first_query(self) -> str:
        BaseData_first_query = Helper.read_and_format_sql_query(
            file_path="basedata_first_query.sql",
            attributes_all_cleaned_string=self.attributes_all_cleaned_string,
            geoid=self.geoid,
            distribution_type=self.distribution_type,
        )

        return BaseData_first_query

    def get_BaseData_df_query(self) -> str:
        BaseData_df_query = Helper.read_and_format_sql_query(
            file_path="basedata_df_query.sql",
            price_amount_column=self.price_amount_column,
            first_day_current_month=GlobalVariables.first_day_current_month,
            first_day_next_month=GlobalVariables.first_day_next_month,
        )

        return BaseData_df_query

    def get_BaseData_final_df_query(self) -> str:
        BaseData_df_query = Helper.read_and_format_sql_query(
            file_path="basedata_df_final_query.sql",
            price_amount_column=self.price_amount_column,
            first_day_current_month=GlobalVariables.first_day_current_month,
        )

        return BaseData_df_query

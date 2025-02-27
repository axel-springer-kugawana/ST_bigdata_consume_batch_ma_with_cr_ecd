import json

import pyspark.sql.functions as F
from awsglue import DynamicFrame
from pyspark.sql.functions import col, struct

# load config
with open("config.json") as f:
    config = json.load(f)


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
    def construct_map_type(df, key_col, value_col, new_col_name):
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
    def modify_data_json(glueContext, df, distribution_type) -> DynamicFrame:
        ret_df = df.toDF()
        ret_df = Helper.add_column_from_mapping(
            df=ret_df,
            mapping=Helper.estate_type_mapping,
            column="subTypes",
            otherwise="NOT_APPLICABLE",
        )

        # BUY
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
                    F.coalesce(
                        F.col("classified_prices_buy_operatingCosts_amount"),
                        F.lit(0.00),
                    ).cast("float"),
                )
                .withColumn(
                    "prices_buy_serviceCharge_currency",
                    F.col("classified_prices_currency"),
                )
            )
        # RENT
        else:
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

        ### ADDING MISSING ATTRIBUTES
        for colname in config["classifiedCols"]:
            ret_df = ret_df.withColumnRenamed(
                f"classified_{colname}",
                colname,
            )

        cols_to_drop = [
            c
            for c in ret_df.columns
            if c.startswith("classified_")
            or (c.startswith("userDefined_") and c not in config["colsToKeep"])
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
            ret_df = Helper.construct_map_type(
                ret_df,
                key_col="estateType",
                value_col="subTypes",
                new_col_name="subTypes",
            )

        ret_df = ret_df.coalesce(1)

        return DynamicFrame.fromDF(ret_df, glueContext, "json")


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

        self.attributes_all = config["attributesAll"] + [
            self.price_amount_column,
            self.operating_cost_column,
        ]
        self.attributes_all_string = ", ".join(self.attributes_all)

        # remove original name and keep only alias
        # e.g. classified_energy_countryspecific_de_energycertificate.heatingconsumption[0] classified_energy_countryspecific_de_energycertificate_heatingconsumption - > classified_energy_countryspecific_de_energycertificate_heatingconsumption
        self.attributes_all_cleaned = [x.split()[-1] for x in self.attributes_all]
        self.attributes_all_cleaned_string = ", ".join(self.attributes_all_cleaned)


class Queries(Variables):

    def __init__(
        self,
        distribution_type: str,
        geoid: int,
        first_day_current_month: str,
        first_day_next_month: str,
    ) -> None:
        super().__init__(distribution_type)
        self.distribution_type = distribution_type
        self.geoid = geoid
        self.first_day_current_month = first_day_current_month
        self.first_day_next_month = first_day_next_month

    @staticmethod
    def get_merge_delete_query(
        extra_columns_wo_prefix,
        extra_columns_with_prefix,
        first_day_past,
        first_day_next_month,
    ) -> str:
        merge_delete_query = Helper.read_and_format_sql_query(
            file_path="0-merge_delete_query.sql",
            extra_columns_wo_prefix=extra_columns_wo_prefix,
            extra_columns_with_prefix=extra_columns_with_prefix,
            first_day_past=first_day_past,
            first_day_next_month=first_day_next_month,
        )

        return merge_delete_query

    def get_BaseData_first_query(self) -> str:
        BaseData_first_query = Helper.read_and_format_sql_query(
            file_path="1-basedata_first_query.sql",
            attributes_all_cleaned_string=self.attributes_all_cleaned_string,
            geoid=self.geoid,
            distribution_type=self.distribution_type,
        )

        return BaseData_first_query

    def get_BaseData_df_query(self) -> str:
        BaseData_df_query = Helper.read_and_format_sql_query(
            file_path="2-basedata_df_query.sql",
            price_amount_column=self.price_amount_column,
            first_day_current_month=self.first_day_current_month,
            first_day_next_month=self.first_day_next_month,
        )

        return BaseData_df_query

    def get_BaseData_final_df_query(self) -> str:
        BaseData_df_query = Helper.read_and_format_sql_query(
            file_path="3-basedata_df_final_query.sql",
            price_amount_column=self.price_amount_column,
            first_day_current_month=self.first_day_current_month,
        )

        return BaseData_df_query

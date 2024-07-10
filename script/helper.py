from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, struct
import pyspark.sql.functions as F


class Helper:

    estate_type_mapping = {
        "HOUSE": "classified_estateSubTypes_house",
        "APARTMENT": "classified_estateSubTypes_apartment",
    }

    @staticmethod
    def nest_schema(schema, separator = "_"):
        print(f'Schema: {schema}')
        """Converts a flattened schema to a nested schema."""
        def build_tree(schema):
            """ Groups fields within columns into a tree (represented as a dictionary). """
            result = {}
            for field in schema.fields:
                print(f'current field: {field}')
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
            """ Convert the tree into a column selection expression. """
            for name, value in tree.items():
                if type(value) == dict:
                    yield struct(list(to_columns(value))).alias(name)
                else:
                    yield col(value).alias(name)
        
        tree = build_tree(schema)
        
        return list(to_columns(tree))
    
    @staticmethod
    def construct_MapType(df, key_col, value_col, new_col_name):
        df = df.withColumn(new_col_name,
                F.when(
                    col(value_col).isNotNull(), 
                    F.create_map(F.lower(col(key_col)), col(value_col))
                ).otherwise(
                    F.create_map(F.lower(col(key_col)), F.lit('NOT_APPLICABLE'))
                )
            )
        return df
    
    @staticmethod
    def read_and_format_sql_query(file_path, **kwargs):
        with open(file_path, 'r') as f:
            sql_template = f.read()

        # Replace placeholders in the SQL template with actual values
        formatted_sql = sql_template.format(**kwargs)

        return formatted_sql


class GlobalVariables:

    DATA_SOURCE = 'IWT'
    DATA_KIND = 'LISTINGS'
    DATA_SCHEMA = 'AVIVIMMO11'

    # First day of current month
    first_day_current_month = datetime.now().replace(day=1).strftime('%Y-%m-%d')

    # First day of next month
    first_day_next_month = (datetime.now().replace(day=1) + relativedelta(months=+1)).replace(day=1).strftime('%Y-%m-%d')

    # First day of 3 months ago
    first_day_3_months_ago = datetime.now().replace(day=1) + relativedelta(months=-3)
    first_day_3_months_ago = first_day_3_months_ago.strftime('%Y-%m-%d')
    
    partition_month = datetime.now().strftime('%Y-%m')
    today = datetime.now().strftime('%Y-%m-%d')

    with open('attributes_all.txt', 'r') as f:
        attributes_all = f.read().splitlines()

    cols_to_drop_basedata = [
        'rank',
        'rankAll',
        'rank_all',
        'rank_current_month'
    ]

    cols_to_drop_json = [
        "id",
        "partitionMonth",
        "partitionGeoid",
        "partitionChangeDate",
        "cleanupdataproblems",
        "fraudLevelId",
    ]

    with open('classified_cols.txt', 'r') as f:
        cols_remove_classified = f.read().splitlines()


class Variables:

    def __init__(self, distribution_type: str) -> None:
        if distribution_type == 'BUY':
            self.price_amount_column = 'cleaned_classified_prices_buy_price_amount'
            self.operating_cost_column = 'cleaned_classified_prices_buy_operatingCosts_amount'
        else:  # rent
            self.price_amount_column = 'cleaned_classified_prices_rent_baseRent_amount'
            self.operating_cost_column = 'cleaned_classified_prices_rent_operatingCosts_amount'

        self.attributes_all = GlobalVariables.attributes_all + [self.price_amount_column, self.operating_cost_column]
        self.attributes_all_string = ', '.join(self.attributes_all)

        # remove original name and keep only alias
        # e.g. classified_energy_countryspecific_de_energycertificate.heatingconsumption[0] classified_energy_countryspecific_de_energycertificate_heatingconsumption - > classified_energy_countryspecific_de_energycertificate_heatingconsumption

        self.attributes_all_clenaed = [x.split()[-1] for x in self.attributes_all]
        self.attributes_all_cleaned_string = ', '.join(self.attributes_all_clenaed)


class Queries(Variables):
    
    def __init__(self, distribution_type: str, geoid: int) -> None:
        super().__init__(distribution_type)
        self.distribution_type = distribution_type
        self.geoid = geoid
    
    @staticmethod
    def get_merge_delete_query(extra_columns_wo_prefix, extra_columns_with_prefix) -> str:
        merge_delete_query = Helper.read_and_format_sql_query(
            file_path="merge_delete_query.sql",
            extra_columns_wo_prefix=extra_columns_wo_prefix,
            extra_columns_with_prefix=extra_columns_with_prefix,
            first_day_3_months_ago=GlobalVariables.first_day_3_months_ago,
            first_day_next_month=GlobalVariables.first_day_next_month
        )

        return merge_delete_query

    def get_BaseData_df_query(self) -> str:
        BaseData_df_query = Helper.read_and_format_sql_query(
            file_path="basedata_df_query.sql",
            attributes_all_string=self.attributes_all_string,
            geoid=self.geoid,
            distribution_type=self.distribution_type,
            attributes_all_cleaned_string=self.attributes_all_cleaned_string,
            price_amount_column=self.price_amount_column,
            first_day_current_month=GlobalVariables.first_day_current_month,
            first_day_next_month=GlobalVariables.first_day_next_month
        )

        return BaseData_df_query

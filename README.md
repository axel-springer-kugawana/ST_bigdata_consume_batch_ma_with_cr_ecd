# ST_bigdata_consume_batch_ma_with_cr_ecd

Job name: consume-batch-ma-with-cr-ecd-{dev/preview/live}

Layer: Consume

Tables used:

* kafka.red_red_cleaned
* kafka.red_vd_cleaned
* kafka.red_ecd_raw
* kinesis.contactrequests_daily_cr_per_classified
* kinesis.customeractions_daily_actions_per_classified

Successors: TBD

GitHub link: [https://github.com/axel-springer-kugawana/ST_bigdata_consume_batch_ma_with_cr_ecd]()

## Overview

The pipeline consists of several components:

1. **Loading tables**: Load multiple tables into Dynamic Frames with push down predicate option to prepare them for further processing.
2. **Setting country values**: Preparing a list with values for geoid, country name, distribution type and data source to process data according to those values.
3. **Custom Transformation:** Custom transformation logic implemented in PySpark for further data processing, including renaming fields and casting data types.
4. **Concat Dynamic Frames**: Perform union-like operations to get a single DynamicFrame with values for every loop iteration.
5. **Writing to S3**: Write DynamicFrame for each itteration to S3, compressed in json and csv format.
6. **AWS Glue Data Catalog Integration:** Writing transformed data to an AWS Glue Data Catalog table, enabling querying and analysis.

WITH BaseDataMax AS
(
    SELECT 
        classified_metaData_classifiedId,
        max(classified_metaData_changeDate) max_metaData_changeDate
    FROM BaseData
    GROUP BY classified_metaData_classifiedId
),

-- without attribute filtering
BaseDataAllMax AS
(
    SELECT 
        classified_metaData_classifiedId,
        max(classified_metaData_changeDate) max_metaData_changeDate
    FROM BaseDataFirst
    WHERE classified_metaData_changeDate < '{first_day_current_month}'
    GROUP BY classified_metaData_classifiedId
),

-- check for classifiedIds that have NEWER records (without attribute filtering)
BaseDataInvalid AS
(
    SELECT max.classified_metaData_classifiedId
    FROM BaseDataMax max
    LEFT JOIN BaseDataAllMax allMax 
        ON max.classified_metaData_classifiedId = allMax.classified_metaData_classifiedId
    WHERE max.max_metaData_changeDate < allMax.max_metaData_changeDate
),

-- eliminate invalid classifiedIds
BaseData_final AS
(
    SELECT *
    FROM BaseData
    WHERE classified_metaData_classifiedId NOT IN (
        SELECT classified_metaData_classifiedId
        FROM BaseDataInvalid
    )
    -- moved from final query to reduce the number of records
    AND fraudLevelId <= 0
),

-- prepare final output
PriceChanges AS (
    -- Get all price changes within the required month
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY classified_metaData_classifiedId, {price_amount_column} 
            ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC
        ) AS row_num
    FROM BaseData_final
    WHERE partitionChangeDate >= '{first_day_current_month}'
),
PreviousPrice AS (
    -- Get the last price before the required time period
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY classified_metaData_classifiedId 
            ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC
        ) AS row_num
    FROM BaseData_final
    WHERE partitionChangeDate < '{first_day_current_month}'
)
SELECT *
FROM (
    SELECT * FROM PriceChanges WHERE row_num = 1
    UNION ALL
    SELECT * FROM PreviousPrice WHERE row_num = 1
) AS CombinedResults


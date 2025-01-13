WITH BaseDataAll AS
(
    SELECT 
        classified_metaData_classifiedId,
        classified_metaData_changeDate
    FROM BaseDataFirst 
),

-- LAST/max record
-- with attribute filtering
BaseDataMax AS
(
    SELECT 
        classified_metaData_classifiedId,
        max(classified_metaData_changeDate) max_metaData_changeDate
    FROM BaseData
    GROUP BY classified_metaData_classifiedId
),

-- LAST/max record before required month (ma_date_1)
-- without attribute filtering
BaseDataAllMax AS
(
    SELECT 
        classified_metaData_classifiedId,
        max(classified_metaData_changeDate) max_metaData_changeDate
    FROM BaseDataAll
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
)

-- with BaseData do the following ...
-- get all price changes for the specified month
-- DO A UNION of 
-- 1. price changes WITION the required time period (PARTITION BY classifiedId, price)
-- 2. last price change BEFORE the required time period
-- ensure to keep only the LAST record of duplicates
SELECT DISTINCT *
FROM (
    SELECT 
        *,
        dense_rank() OVER (PARTITION BY classified_metaData_classifiedId, {price_amount_column} 
                            ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) as rankAll
    FROM (
        -- get ALL price changes WITHIN required month
        SELECT *
        FROM (
            SELECT *,
                dense_rank() OVER (PARTITION BY classified_metaData_classifiedId, {price_amount_column} 
                                    ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) as rank
            FROM BaseData_final
            WHERE partitionChangeDate >= '{first_day_current_month}'  
        )
        WHERE rank = 1

        UNION ALL

        -- get LAST price BEFORE the required time period = price at the beginning of the required time period
        SELECT *
        FROM (
            SELECT *,
                dense_rank() OVER (PARTITION BY classified_metaData_classifiedId 
                                    ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) as rank
            FROM BaseData_final
            WHERE partitionChangeDate < '{first_day_current_month}'
        )
        WHERE rank = 1
    )
)
WHERE
    rankAll = 1
    AND fraudLevelId <= 0
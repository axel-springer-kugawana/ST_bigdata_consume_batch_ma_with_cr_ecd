WITH BaseDataFirst AS (
    SELECT *,
           dense_rank() OVER (
               PARTITION BY classified_metaData_classifiedId, to_date(classified_metaData_changeDate)
               ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC
           ) AS baseRank
    FROM red_red_cleaned
),
BaseDataAll AS (
    SELECT DISTINCT classified_metaData_classifiedId, classified_metaData_changeDate
    FROM BaseDataFirst
    WHERE baseRank = 1
      AND classified_geo_countrySpecific_de_iwtLegacyGeoID LIKE '{geoid}%'
      AND cleanupdataproblems <= 3
      AND cleaned_classified_distributionType = '{distribution_type}'
),
BaseDataFiltered AS (
    SELECT DISTINCT {attributes_all_cleaned_string}, 
                    ecd.lastFraudLevelId AS fraudLevelId, 
                    IFNULL(mapcr.noContactRequests, 0) AS userDefined_immoWelt_contact_requests,
                    IFNULL(mapcr.noIwContactRequests, 0) AS userDefined_immoWelt_iw_contact_requests,
                    IFNULL(mapcr.noInContactRequests, 0) AS userDefined_immoWelt_in_contact_requests,
                    IFNULL(mapev.noExposeVisits, 0) AS userDefined_immoWelt_expose_visits,
                    IFNULL(mapev.noIwExposeVisits, 0) AS userDefined_immoWelt_iw_expose_visits,
                    IFNULL(mapev.noInExposeVisits, 0) AS userDefined_immoWelt_in_expose_visits
    FROM BaseDataFirst AS oc
    LEFT JOIN (
        SELECT globalObjectKey, 
               MAX(changeDate) AS lastChangeDate,
               MAX_BY(controlData.FraudLevelId, changeDate) AS lastFraudLevelId
        FROM red_ecd
        WHERE operation != 'Delete'
        GROUP BY globalObjectKey
    ) AS ecd ON oc.classified_metaData_classifiedId = ecd.globalObjectKey
    LEFT JOIN (
        SELECT classifiedId, 
               SUM(IFNULL(emailContactRequest, 0)) AS noContactRequests,
               SUM(IFNULL(emailContactRequestIW, 0)) AS noIwContactRequests,
               SUM(IFNULL(emailContactRequestIN, 0)) AS noInContactRequests
        FROM contactrequests_daily_cr_per_classified
        GROUP BY classifiedId
    ) AS mapcr ON oc.classified_metaData_classifiedId = mapcr.classifiedId
    LEFT JOIN (
        SELECT classifiedId, 
               SUM(IFNULL(exposeVisits, 0)) AS noExposeVisits,
               SUM(IFNULL(exposeVisitsIW, 0)) AS noIwExposeVisits,
               SUM(IFNULL(exposeVisitsIN, 0)) AS noInExposeVisits
        FROM customeractions_daily_actions_per_classified
        GROUP BY classifiedId
    ) AS mapev ON oc.classified_metaData_classifiedId = mapev.classifiedId
    WHERE baseRank = 1
      AND classified_geo_countrySpecific_de_iwtLegacyGeoID LIKE '{geoid}%'
      AND cleanupdataproblems <= 3
      AND (cleaned_classified_structure_building_floorNumber >= 0 OR cleaned_classified_structure_building_floorNumber IS NULL)
      AND (classified_management_rent_certificateOfEligibilityNeeded IS NULL OR classified_management_rent_certificateOfEligibilityNeeded != 'YES')
      AND (classified_features_residential_flatSharePossible IS NULL OR classified_features_residential_flatSharePossible != 'YES')
      AND cleaned_classified_geo_postalcode != ''
      AND cleaned_classified_distributionType = '{distribution_type}'
      AND (classified_features_furnished IS NULL OR classified_features_furnished = 'NOT_APPLICABLE')
      AND classified_estateType IN ('HOUSE', 'APARTMENT')
      AND cleaned_classified_prices_buy_price_amount > 1
      AND cleaned_classified_spaces_residential_livingSpace IS NOT NULL
      AND cleaned_classified_spaces_residential_livingSpace BETWEEN 5 AND 500
      AND cleaned_classified_prices_buy_price_amount / cleaned_classified_spaces_residential_livingSpace > 1
      AND cleaned_classified_structure_rooms_numberOfRooms <= 20
      AND classified_management_isForInvestment IS NOT TRUE
),
ActiveOffers AS (
    SELECT DISTINCT classifiedId
    FROM red_vd_cleaned
    WHERE aktivbis >= TO_DATE('{first_day_current_month}')
      AND aktivab < TO_DATE('{first_day_next_month}')
      AND classifiedId LIKE '%'
),
BaseData AS (
    SELECT *
    FROM BaseDataFiltered
    WHERE classified_metaData_classifiedId IN (SELECT classifiedId FROM ActiveOffers)
),
BaseDataMax AS (
    SELECT classified_metaData_classifiedId, 
           MAX(classified_metaData_changeDate) AS max_metaData_changeDate
    FROM BaseData
    GROUP BY classified_metaData_classifiedId
),
BaseDataAllMax AS (
    SELECT classified_metaData_classifiedId, 
           MAX(classified_metaData_changeDate) AS max_metaData_changeDate
    FROM BaseDataAll
    WHERE classified_metaData_changeDate < '{first_day_current_month}'
    GROUP BY classified_metaData_classifiedId
),
BaseDataInvalid AS (
    SELECT max.classified_metaData_classifiedId
    FROM BaseDataMax AS max
    LEFT JOIN BaseDataAllMax AS allMax
    ON max.classified_metaData_classifiedId = allMax.classified_metaData_classifiedId
    WHERE max.max_metaData_changeDate < allMax.max_metaData_changeDate
),
BaseData2 AS (
    SELECT *
    FROM BaseData
    WHERE classified_metaData_classifiedId NOT IN (SELECT classified_metaData_classifiedId FROM BaseDataInvalid)
),
PriceChangesWithinMonth AS (
    SELECT DISTINCT *,
           dense_rank() OVER (
               PARTITION BY classified_metaData_classifiedId, cleaned_classified_prices_buy_price_amount 
               ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC
           ) AS rank
    FROM BaseData2
    WHERE partitionChangeDate >= '{first_day_current_month}'
),
LastPriceBeforeMonth AS (
    SELECT DISTINCT *,
           dense_rank() OVER (
               PARTITION BY classified_metaData_classifiedId 
               ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC
           ) AS rank
    FROM BaseData2
    WHERE partitionChangeDate < '{first_day_current_month}'
)
SELECT DISTINCT *
FROM (
    SELECT DISTINCT *,
           dense_rank() OVER (
               PARTITION BY classified_metaData_classifiedId, cleaned_classified_prices_buy_price_amount 
               ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC
           ) AS rankAll
    FROM (
        SELECT * FROM PriceChangesWithinMonth WHERE rank = 1
        UNION
        SELECT * FROM LastPriceBeforeMonth WHERE rank = 1
    )
) AS final
WHERE rankAll = 1
AND fraudLevelId <= 0
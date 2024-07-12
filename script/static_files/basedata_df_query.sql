WITH BaseDataFirst AS
(
SELECT *,
       dense_rank() OVER (PARTITION BY classified_metaData_classifiedId, to_date(classified_metaData_changeDate)
                          ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) as baseRank
FROM red_red_cleaned
),


-- ALL data independent of attribute filters
BaseDataAll AS
(
-- select from offers_cleaned
SELECT DISTINCT classified_metaData_classifiedId, classified_metaData_changeDate
FROM BaseDataFirst
WHERE baseRank = 1
  AND classified_geo_countrySpecific_de_iwtLegacyGeoID like '{geoid}%'
  AND cleanupdataproblems <= 3
  AND cleaned_classified_distributionType == '{distribution_type}'
),


BaseData AS
(
-- select from offers_cleaned
-- from several monthsBack to the end of required month
-- filtered
SELECT DISTINCT *
FROM
(
SELECT DISTINCT oc.*, ecd.lastFraudLevelId fraudLevelId, 
                ifnull(mapcr.noContactRequests,0) userDefined_immoWelt_contact_requests,
                ifnull(mapcr.noIwContactRequests,0) userDefined_immoWelt_iw_contact_requests,
                ifnull(mapcr.noInContactRequests,0) userDefined_immoWelt_in_contact_requests,
                ifnull(mapev.noExposeVisits,0) userDefined_immoWelt_expose_visits,
                ifnull(mapev.noIwExposeVisits,0) userDefined_immoWelt_iw_expose_visits,
                ifnull(mapev.noInExposeVisits,0) userDefined_immoWelt_in_expose_visits
FROM
(
SELECT DISTINCT {attributes_all_cleaned_string}
FROM BaseDataFirst
WHERE baseRank = 1
  AND classified_geo_countrySpecific_de_iwtLegacyGeoID like '{geoid}%'
  AND cleanupdataproblems <= 3
  AND (cleaned_classified_structure_building_floorNumber >= 0 or 
       cleaned_classified_structure_building_floorNumber is NULL)
  AND (classified_management_rent_certificateOfEligibilityNeeded is null or 
       classified_management_rent_certificateOfEligibilityNeeded != 'YES')
  AND (classified_features_residential_flatSharePossible is NULL or 
       classified_features_residential_flatSharePossible != 'YES')
  AND cleaned_classified_geo_postalcode != ''

  --AND userDefined_immoWelt_fraudLevelId <= 0
  AND cleaned_classified_distributionType == '{distribution_type}'
  AND (classified_features_furnished is NULL or
       classified_features_furnished = 'NOT_APPLICABLE')
  AND classified_estateType IN ('HOUSE', 'APARTMENT')
  AND cleaned_classified_prices_buy_price_amount > 1
  AND cleaned_classified_spaces_residential_livingSpace is not NULL 
  AND cleaned_classified_spaces_residential_livingSpace BETWEEN 5 AND 500
  AND cleaned_classified_prices_buy_price_amount / cleaned_classified_spaces_residential_livingSpace > 1.
  AND cleaned_classified_structure_rooms_numberOfRooms <= 20
  -- 'Renditeobjekt'
  AND classified_management_isForInvestment IS NOT True
) oc

-- join left with latest ecd fraud information
LEFT JOIN
(
SELECT globalObjectKey, max(changeDate) lastChangeDate,
       max_by(controlData.FraudLevelId, changeDate) lastFraudLevelId
FROM red_ecd
  WHERE operation != 'Delete'
GROUP BY globalObjectKey
) ecd ON oc.classified_metaData_classifiedId = ecd.globalObjectKey



-- join with crs contact request data (only email)
LEFT JOIN
(
SELECT classifiedId, 
       sum(ifnull(emailContactRequest,0)) noContactRequests,
       sum(ifnull(emailContactRequestIW,0)) noIwContactRequests,
       sum(ifnull(emailContactRequestIN,0)) noInContactRequests
FROM contactrequests_daily_cr_per_classified
GROUP BY classifiedId
) mapcr 
ON oc.classified_metaData_classifiedId = mapcr.classifiedId



-- join with customeractions expose visits data 
LEFT JOIN
(
SELECT classifiedId, 
       sum(ifnull(exposeVisits,0)) noExposeVisits,
       sum(ifnull(exposeVisitsIW,0)) noIwExposeVisits,
       sum(ifnull(exposeVisitsIN,0)) noInExposeVisits
FROM customeractions_daily_actions_per_classified
GROUP BY classifiedId
) mapev 
ON oc.classified_metaData_classifiedId = mapev.classifiedId

)


  -- if required: regard only offers that are active (within activity periods)
  WHERE classified_metaData_classifiedId IN 
(
SELECT distinct classifiedId
FROM red_vd_cleaned
WHERE aktivbis >= to_date('{first_day_current_month}')
  AND aktivab < to_date('{first_day_next_month}')
  AND classifiedId like '%'
)

),



-- LAST/max record
-- with attribute filtering
BaseDataMax AS
(
SELECT classified_metaData_classifiedId, max(classified_metaData_changeDate) max_metaData_changeDate
FROM BaseData
GROUP BY classified_metaData_classifiedId
),

-- LAST/max record before required month (ma_date_1)
-- without attribute filtering
BaseDataAllMax AS
(
SELECT classified_metaData_classifiedId, max(classified_metaData_changeDate) max_metaData_changeDate
FROM BaseDataAll
WHERE classified_metaData_changeDate < < '{first_day_current_month}'
GROUP BY classified_metaData_classifiedId
),


-- check for classifiedIds that have NEWER records (without attribute filtering)
BaseDataInvalid AS
(
SELECT max.classified_metaData_classifiedId
FROM
(
SELECT *
FROM BaseDataMax
) max
LEFT JOIN
(
SELECT *
FROM BaseDataAllMax
) allMax
ON max.classified_metaData_classifiedId = allMax.classified_metaData_classifiedId
WHERE max.max_metaData_changeDate < allMax.max_metaData_changeDate
),


-- eliminate invalid classifiedIds
BaseData2 AS
(
SELECT *
FROM BaseData
WHERE classified_metaData_classifiedId NOT IN 
(
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
FROM 
(
SELECT DISTINCT *,
       dense_rank() OVER (PARTITION BY classified_metaData_classifiedId,cleaned_classified_prices_buy_price_amount 
                          ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) as rankAll
FROM

(
-- get ALL price changes WITHIN required month
SELECT DISTINCT *
FROM 
(
SELECT DISTINCT *,
       dense_rank() OVER (PARTITION BY classified_metaData_classifiedId,cleaned_classified_prices_buy_price_amount 
                          ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) as rank
FROM BaseData2
WHERE partitionChangeDate >= '{first_day_current_month}'
)
WHERE rank = 1


UNION

  
-- get the LAST price BEFORE the required time period - equals first price for required time period
SELECT DISTINCT *
FROM 
(
SELECT DISTINCT *,
       dense_rank() OVER (PARTITION BY classified_metaData_classifiedId 
                          ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) as rank
FROM BaseData2
WHERE partitionChangeDate < '{first_day_current_month}'
)
WHERE rank = 1

)
)
WHERE rankAll = 1
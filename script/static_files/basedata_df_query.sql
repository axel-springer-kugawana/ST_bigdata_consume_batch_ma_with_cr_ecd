with BaseDataFirst as (
    select 
    {attributes_all_string}, 
    DENSE_RANK() OVER (PARTITION BY classified_metadata_classifiedid, to_date(classified_metadata_changedate) 
                    ORDER BY classified_metadata_changedate DESC, partitionChangeDate DESC) as baseRank
    from red_red_cleaned
), 

BaseDataAll as (
    SELECT DISTINCT 
        classified_metaData_classifiedId, 
        classified_metaData_changeDate
    FROM BaseDataFirst
    WHERE 
        baseRank = 1
        AND classified_geo_countrySpecific_de_iwtLegacyGeoID like '{geoid}%'
        AND cleanupdataproblems <= 3
        AND cleaned_classified_distributionType = '{distribution_type}'
),

oc as (
    SELECT 
        DISTINCT {attributes_all_cleaned_string}
    FROM 
        BaseDataFirst
    WHERE 
        baseRank = 1
        AND classified_geo_countrySpecific_de_iwtLegacyGeoID like '{geoid}%'
        AND cleanupdataproblems <= 3
        AND cleaned_classified_distributionType = '{distribution_type}'
        AND (cleaned_classified_structure_building_floorNumber >= 0 or 
            cleaned_classified_structure_building_floorNumber is NULL)
        AND (classified_management_rent_certificateOfEligibilityNeeded is null or 
            classified_management_rent_certificateOfEligibilityNeeded != 'YES')
        AND (classified_features_residential_flatSharePossible is NULL or 
            classified_features_residential_flatSharePossible != 'YES')
        AND cleaned_classified_geo_postalcode != ''
        AND (classified_features_furnished is NULL or
            classified_features_furnished = 'NOT_APPLICABLE')
        AND classified_estateType IN ('HOUSE', 'APARTMENT')
        AND cleaned_classified_prices_buy_price_amount > 1
        AND cleaned_classified_spaces_residential_livingSpace is not NULL 
        AND cleaned_classified_spaces_residential_livingSpace BETWEEN 5 AND 500
        AND cleaned_classified_prices_buy_price_amount / cleaned_classified_spaces_residential_livingSpace > 1.
        AND cleaned_classified_structure_rooms_numberOfRooms <= 20
        AND classified_management_isForInvestment is not true
),

ecd as (
    SELECT 
        globalObjectKey,
        max(changeDate) lastChangeDate,
        max_by(controlData.FraudLevelId, changeDate) lastFraudLevelId
    FROM red_ecd
    WHERE operation != 'Delete'
    GROUP BY globalObjectKey
),

mapcr as (
    SELECT 
        classifiedId, 
        sum(coalesce(emailContactRequest,0)) noContactRequests,
        sum(coalesce(emailContactRequestIW,0)) noIwContactRequests,
        sum(coalesce(emailContactRequestIN,0)) noInContactRequests
    FROM contactrequests_daily_cr_per_classified
    GROUP BY classifiedId
), 

mapev as (
    SELECT 
        classifiedId, 
        sum(coalesce(exposeVisits,0)) noExposeVisits,
        sum(coalesce(exposeVisitsIW,0)) noIwExposeVisits,
        sum(coalesce(exposeVisitsIN,0)) noInExposeVisits
    FROM customeractions_daily_actions_per_classified
    GROUP BY classifiedId
),

BaseData AS (
    SELECT DISTINCT 
        oc.*,
        ecd.lastFraudLevelId fraudLevelId, 
        coalesce(mapcr.noContactRequests,0) userDefined_immoWelt_contact_requests,
        coalesce(mapcr.noIwContactRequests,0) userDefined_immoWelt_iw_contact_requests,
        coalesce(mapcr.noInContactRequests,0) userDefined_immoWelt_in_contact_requests,
        coalesce(mapev.noExposeVisits,0) userDefined_immoWelt_expose_visits,
        coalesce(mapev.noIwExposeVisits,0) userDefined_immoWelt_iw_expose_visits,
        coalesce(mapev.noInExposeVisits,0) userDefined_immoWelt_in_expose_visits
    FROM oc
    INNER JOIN red_vd_cleaned rc
        ON oc.classified_metaData_classifiedId = rc.classifiedId
        AND aktivbis >= to_date('{first_day_current_month}')
        AND aktivab < to_date('{first_day_next_month}')
    LEFT JOIN ecd 
        ON oc.classified_metaData_classifiedId = ecd.globalObjectKey
    LEFT JOIN mapcr 
        ON oc.classified_metaData_classifiedId = mapcr.classifiedId
    LEFT JOIN mapev 
        ON oc.classified_metaData_classifiedId = mapev.classifiedId
),

BaseDataMax AS
(
    SELECT 
        classified_metaData_classifiedId,
        max(classified_metaData_changeDate) max_metaData_changeDate
    FROM BaseData
    GROUP BY classified_metaData_classifiedId
),

BaseDataAllMax AS (
    SELECT 
        classified_metaData_classifiedId,
        max(classified_metaData_changeDate) max_metaData_changeDate
    FROM BaseDataAll
    WHERE classified_metaData_changeDate < '{first_day_current_month}'
    GROUP BY classified_metaData_classifiedId
),

BaseDataInvalid AS
(
    SELECT max.classified_metaData_classifiedId
    FROM BaseDataMax max
    LEFT JOIN BaseDataAllMax allMax
        ON max.classified_metaData_classifiedId = allMax.classified_metaData_classifiedId
    WHERE 
        max.max_metaData_changeDate < allMax.max_metaData_changeDate
),

BaseDataCleaned AS
(
    SELECT bd.*
    from BaseData bd
    left join BaseDataInvalid bdi
        on bd.classified_metaData_classifiedId = bdi.classified_metaData_classifiedId
    where bdi.classified_metaData_classifiedId is null
),

CurrentMonthChanges as (
    SELECT 
        *,
        DENSE_RANK() OVER (PARTITION BY classified_metaData_classifiedId,cleaned_classified_prices_buy_price_amount 
                            ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) as rank_current_month
    FROM BaseDataCleaned
    WHERE partitionChangeDate >= '{first_day_current_month}'
),

LastPriceBeforeMonth as (
    SELECT 
        *,
        DENSE_RANK() OVER (PARTITION BY classified_metaData_classifiedId 
                            ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) as rank_last_price_before_month
    FROM BaseDataCleaned
    WHERE partitionChangeDate < '{first_day_current_month}'
)

SELECT
    *
FROM (
    SELECT 
        *,
        DENSE_RANK() OVER (PARTITION BY classified_metaData_classifiedId, {price_amount_column} 
                        ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) AS rank_all
    FROM (
        SELECT *
        FROM CurrentMonthChanges
        WHERE rank_current_month = 1

        UNION

        SELECT *
        FROM LastPriceBeforeMonth
        WHERE rank_last_price_before_month = 1
    ) AS all_changes
) AS final_changes
WHERE 
rank_all = 1
AND fraudLevelId <= 0

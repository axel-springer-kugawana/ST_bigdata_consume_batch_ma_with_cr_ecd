SELECT 
    oc.*,
    ecd.lastFraudLevelId fraudLevelId,
    ifnull(mapcr.noContactRequests,0) userDefined_immoWelt_contact_requests,
    ifnull(mapcr.noIwContactRequests,0) userDefined_immoWelt_iw_contact_requests,
    ifnull(mapcr.noInContactRequests,0) userDefined_immoWelt_in_contact_requests,
    ifnull(mapev.noExposeVisits,0) userDefined_immoWelt_expose_visits,
    ifnull(mapev.noIwExposeVisits,0) userDefined_immoWelt_iw_expose_visits,
    ifnull(mapev.noInExposeVisits,0) userDefined_immoWelt_in_expose_visits
FROM (
    SELECT 
        {attributes_all_cleaned_string}
    FROM BaseDataFirst
    WHERE
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
        AND {price_amount_column} > 1
        AND cleaned_classified_spaces_residential_livingSpace is not NULL 
        AND cleaned_classified_spaces_residential_livingSpace BETWEEN 5 AND 500
        AND {price_amount_column} / cleaned_classified_spaces_residential_livingSpace > 1.
        AND cleaned_classified_structure_rooms_numberOfRooms <= 20
        AND classified_management_isForInvestment IS NOT True
) oc

-- join left with latest ecd fraud information
LEFT JOIN (
    SELECT 
        globalObjectKey,
        max(changeDate) lastChangeDate,
        max_by(controlData.FraudLevelId, changeDate) lastFraudLevelId
    FROM red_ecd
    WHERE operation != 'Delete'
    GROUP BY globalObjectKey
) ecd ON oc.classified_metaData_classifiedId = ecd.globalObjectKey

-- join with crs contact request data (only email)
LEFT JOIN (
    SELECT 
        classifiedId, 
        sum(ifnull(emailContactRequest,0)) noContactRequests,
        sum(ifnull(emailContactRequestIW,0)) noIwContactRequests,
        sum(ifnull(emailContactRequestIN,0)) noInContactRequests
    FROM contactrequests_daily_cr_per_classified
    GROUP BY classifiedId
) mapcr ON oc.classified_metaData_classifiedId = mapcr.classifiedId

-- join with customeractions expose visits data 
LEFT JOIN (
    SELECT
        classifiedId, 
        sum(ifnull(exposeVisits,0)) noExposeVisits,
        sum(ifnull(exposeVisitsIW,0)) noIwExposeVisits,
        sum(ifnull(exposeVisitsIN,0)) noInExposeVisits
    FROM customeractions_daily_actions_per_classified
    GROUP BY classifiedId
) mapev ON oc.classified_metaData_classifiedId = mapev.classifiedId

-- if required: regard only offers that are active (within activity periods)
WHERE classified_metaData_classifiedId IN (
    SELECT classifiedId
    FROM red_vd_cleaned
    WHERE
        aktivbis >= to_date('{first_day_current_month}')
        AND aktivab < to_date('{first_day_next_month}')
        AND classifiedId like '%'
)
WITH red_vd_cid as (
    SELECT classifiedId
    FROM red_vd_cleaned
    WHERE
        aktivbis >= to_date('{first_day_current_month}')
        AND aktivab < to_date('{first_day_next_month}')
),

oc as (
    SELECT 
        bdf.*
    FROM BaseDataFirst bdf
    INNER JOIN red_vd_cid rvd
        ON bdf.classified_metaData_classifiedId = rvd.classifiedId
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

contact_requests as (
    SELECT 
        classifiedId, 
        SUM(COALESCE(emailContactRequest, 0)) noContactRequests,
        SUM(COALESCE(emailContactRequestIW, 0)) noIwContactRequests,
        SUM(COALESCE(emailContactRequestIN, 0)) noInContactRequests
    FROM contactrequests_daily_cr_per_classified
    GROUP BY classifiedId
),

expose_visits as (
    SELECT
        classifiedId, 
        SUM(COALESCE(exposeVisits, 0)) noExposeVisits,
        SUM(COALESCE(exposeVisitsIW, 0)) noIwExposeVisits,
        SUM(COALESCE(exposeVisitsIN, 0)) noInExposeVisits
    FROM customeractions_daily_actions_per_classified
    GROUP BY classifiedId
)

SELECT 
    oc.*,
    ecd.lastFraudLevelId fraudLevelId,
    COALESCE(contact_requests.noContactRequests, 0) userDefined_immoWelt_contact_requests,
    COALESCE(contact_requests.noIwContactRequests, 0) userDefined_immoWelt_iw_contact_requests,
    COALESCE(contact_requests.noInContactRequests, 0) userDefined_immoWelt_in_contact_requests,
    COALESCE(expose_visits.noExposeVisits, 0) userDefined_immoWelt_expose_visits,
    COALESCE(expose_visits.noIwExposeVisits, 0) userDefined_immoWelt_iw_expose_visits,
    COALESCE(expose_visits.noInExposeVisits, 0) userDefined_immoWelt_in_expose_visits

FROM oc
-- join left with latest ecd fraud information
LEFT JOIN ecd ON oc.classified_metaData_classifiedId = ecd.globalObjectKey
-- join with crs contact request data (only email)
LEFT JOIN contact_requests ON oc.classified_metaData_classifiedId = contact_requests.classifiedId
-- join with customeractions expose visits data 
LEFT JOIN expose_visits ON oc.classified_metaData_classifiedId = expose_visits.classifiedId
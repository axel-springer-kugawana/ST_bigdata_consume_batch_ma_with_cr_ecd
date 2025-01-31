WITH final AS (
    SELECT 
        *,
        dense_rank() OVER (PARTITION BY classified_metaData_classifiedId, to_date(classified_metaData_changeDate)
                            ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) AS baseRank
    FROM red_red_cleaned
)
SELECT *
FROM final 
WHERE
    baseRank = 1
    AND classified_geo_countrySpecific_de_iwtLegacyGeoID like '{geoid}%'
    AND cleanupdataproblems <= 3
    AND cleaned_classified_distributionType = '{distribution_type}'
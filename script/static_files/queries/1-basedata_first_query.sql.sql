WITH rrc AS (
    SELECT 
        {attributes_all_cleaned_string},
        dense_rank() OVER (PARTITION BY classified_metaData_classifiedId, to_date(classified_metaData_changeDate)
                            ORDER BY classified_metaData_changeDate DESC, partitionChangeDate DESC) AS baseRank
    FROM red_red_cleaned
),

rrc_filtered AS (
    SELECT 
        *
    FROM rrc
    WHERE
        baseRank = 1
        AND classified_geo_countrySpecific_de_iwtLegacyGeoID like '{geoid}%'
        AND cleanupdataproblems <= 3
        AND cleaned_classified_distributionType = '{distribution_type}'
), 

final as (
    select 
        rrc.*,
        rrt.classified_texts_headline_de,
        rrt.classified_texts_description_de,
        rrt.classified_texts_features_de,
        rrt.classified_texts_area_de,
        rrt.classified_texts_extendedinformation_de,
        rrt.classified_texts_locationnote_de,
        rrt.classified_texts_regionalnote_de,
        rrt.classified_texts_transportation_bus_de
    from rrc_filtered rrc
    left join red_red_text rrt
        on rrc.id = rrt.id
)

SELECT *
FROM final 

    
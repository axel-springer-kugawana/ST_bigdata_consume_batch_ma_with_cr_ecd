select
    *
from red_red_cleaned_raw
where 
    cleaned_classified_distributionType in ('RENT', 'BUY')
    and (
        classified_geo_countrySpecific_de_iwtLegacyGeoID like '108%'
        or classified_geo_countrySpecific_de_iwtLegacyGeoID like '103%'
    )
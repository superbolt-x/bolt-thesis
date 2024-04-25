{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT
account_id,
campaign_name,
campaign_id,
campaign_status,
CASE WHEN campaign_name ~* 'search' AND campaign_name !~* 'brand' THEN 'Campaign Type: Search Unbranded'
    ELSE campaign_type_default
END as campaign_type_default,
CASE WHEN campaign_name ~* 'Retargeting' OR campaign_name ~* 'Brand' OR campaign_name ~* 'Blend' THEN 'Retargeting'
    ELSE 'Prospecting'
END AS campaign_targeting,
CASE WHEN campaign_name ~* 'PMax' AND campaign_targeting ~* 'Retargeting' THEN 'PMax Retargeting'
    WHEN campaign_name ~* 'PMax' AND campaign_targeting ~* 'Prospecting' THEN 'PMax Prospecting'
    WHEN campaign_name ~* 'Search' AND campaign_targeting ~* 'Retargeting' THEN 'Search Retargeting'
    WHEN campaign_name ~* 'Search' AND campaign_targeting ~* 'Prospecting' THEN 'Search Prospecting'
    WHEN campaign_name ~* 'Discovery' AND campaign_targeting ~* 'Retargeting' THEN 'Discovery Retargeting'
    WHEN campaign_name ~* 'Discovery' AND campaign_targeting ~* 'Prospecting' THEN 'Discovery Prospecting'
    WHEN campaign_name ~* 'Youtube' AND campaign_targeting ~* 'Retargeting' THEN 'Youtube Retargeting'
    WHEN campaign_name ~* 'Youtube' AND campaign_targeting ~* 'Prospecting' THEN 'Youtube Prospecting'
END AS campaign_type_custom,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share,
video_views
FROM {{ ref('googleads_performance_by_campaign') }}

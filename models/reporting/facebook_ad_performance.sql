{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
"offsite_conversion.fb_pixel_custom.signup_success" as signups,
CASE WHEN date BETWEEN '2024-01-01' AND '2024-04-30' THEN leads ELSE "offsite_conversion.fb_pixel_custom.consultation_payment" END as consultation_payment
FROM {{ ref('facebook_performance_by_ad') }}
LEFT JOIN (SELECT ad_id, date::date as date, COALESCE(SUM(value),0) as leads FROM {{ source('facebook_raw','ads_insights_actions') }} WHERE action_type = 'lead' GROUP BY 1,2)
    USING(ad_id, date)

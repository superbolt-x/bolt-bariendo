{{ config (
    alias = target.database + '_facebook_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
link_clicks,
"offsite_conversion.fb_pixel_custom.signup_success" as signups,
"offsite_conversion.fb_pixel_custom.consultation_payment" as consultation_payment
FROM {{ ref('facebook_performance_by_campaign') }}

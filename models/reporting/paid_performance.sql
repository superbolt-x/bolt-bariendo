{{ config (
    alias = target.database + '_paid_performance'
)}}

WITH paid_data as 
    (SELECT 'Facebook' as channel, date, 
        COALESCE(SUM(spend),0) as spend, 
        COALESCE(SUM(link_clicks),0) as clicks, 
        COALESCE(SUM(impressions),0) as impressions,
        COALESCE(SUM(signups),0) as signups,
        COALESCE(SUM(consultation_payment),0) as consultation_payment
    FROM {{ source('reporting','facebook_ad_performance') }}
    GROUP BY 1,2
    
    UNION ALL
    
    SELECT 'Google Ads' as channel, date, 
        COALESCE(SUM(spend),0) as spend, 
        COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(impressions),0) as impressions,
        COALESCE(SUM(signups),0) as signups,
        COALESCE(SUM(consultation_payment),0) as consultation_payment
    FROM {{ source('reporting','googleads_campaign_performance') }}
    GROUP BY 1,2)

SELECT channel,
    date,
    spend,
    clicks,
    impressions,
    signups,
    consultation_payment
FROM paid_data
ORDER BY date DESC

{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
    
SELECT
account_id,
ad_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
signups, 
consultation_payment
FROM {{ ref('googleads_performance_by_ad') }}
LEFT JOIN 
    (SELECT 'day' as date_granularity,
        DATE_TRUNC('day',date::date) as date,
        customer_id as account_id, campaign_id, campaign_name, ad_group_name, ad_group_id, ad_id, 
        COALESCE(SUM(CASE WHEN conversion_action_category = 'SIGNUP' THEN conversions END),0) as signups,
        COALESCE(SUM(CASE WHEN conversion_action_category = 'PURCHASE' THEN conversions END),0) as consultation_payment
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6,7,8
    UNION ALL
    SELECT 'week' as date_granularity,
        DATE_TRUNC('week',date::date) as date,
        customer_id as account_id, campaign_id, campaign_name, ad_group_name, ad_group_id, ad_id, 
        COALESCE(SUM(CASE WHEN conversion_action_category = 'SIGNUP' THEN conversions END),0) as signups,
        COALESCE(SUM(CASE WHEN conversion_action_category = 'PURCHASE' THEN conversions END),0) as consultation_payment
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6,7,8
    UNION ALL
    SELECT 'month' as date_granularity,
        DATE_TRUNC('month',date::date) as date,
        customer_id as account_id, campaign_id, campaign_name, ad_group_name, ad_group_id, ad_id, 
        COALESCE(SUM(CASE WHEN conversion_action_category = 'SIGNUP' THEN conversions END),0) as signups,
        COALESCE(SUM(CASE WHEN conversion_action_category = 'PURCHASE' THEN conversions END),0) as consultation_payment
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6,7,8
    UNION ALL
    SELECT 'quarter' as date_granularity,
        DATE_TRUNC('quarter',date::date) as date,
        customer_id as account_id, campaign_id, campaign_name, ad_group_name, ad_group_id, ad_id, 
        COALESCE(SUM(CASE WHEN conversion_action_category = 'SIGNUP' THEN conversions END),0) as signups,
        COALESCE(SUM(CASE WHEN conversion_action_category = 'PURCHASE' THEN conversions END),0) as consultation_payment
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6,7,8
    UNION ALL
    SELECT 'year' as date_granularity,
        DATE_TRUNC('year',date::date) as date,
        customer_id as account_id, campaign_id, campaign_name, ad_group_name, ad_group_id, ad_id, 
        COALESCE(SUM(CASE WHEN conversion_action_category = 'SIGNUP' THEN conversions END),0) as signups,
        COALESCE(SUM(CASE WHEN conversion_action_category = 'PURCHASE' THEN conversions END),0) as consultation_payment
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6,7,8
    ) USING(date_granularity, date, account_id, campaign_id, campaign_name, ad_group_name, ad_group_id, ad_id)

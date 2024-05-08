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
conversions_value as revenue
FROM {{ ref('googleads_performance_by_ad') }}
LEFT JOIN 
    ({%- for date_granularity in date_granularity_list %}
        SELECT '{{date_granularity}}' as date_granularity,
            {{date_granularity}} as date,
            customer_id as account_id, id as campaign_id,
            COALESCE(SUM(CASE WHEN conversion_action_category = 'SIGNUP' THEN conversions END),0) as signups,
            COALESCE(SUM(CASE WHEN conversion_action_category = 'PURCHASE' THEN conversions END),0) as consultation_payment
        FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
        GROUP BY 1,2,3,4
        {% if not loop.last %}UNION ALL
        {% endif %}
    {% endfor %}) USING(date_granularity, date, account_id, campaign_id)

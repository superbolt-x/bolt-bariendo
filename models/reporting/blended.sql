{{ config(
    alias = target.database ~ '_blended'
) }}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    consults AS (
        SELECT *, {{ get_date_parts('last_payment_date') }}
        FROM {{ source('s3_raw', 'consults') }}
    ),
    signups AS (
        SELECT *, {{ get_date_parts('first_signup_date') }}
        FROM {{ source('s3_raw', 'signups') }}
    ),
    posthog_consults AS (
        {{ posthog_granular('consults','last_payment_date','posthog_consults') }}
    ),
    posthog_signups AS (
        {{ posthog_granular('signups','first_signup_date','posthog_signups') }}
    ),
    platform_data AS (
        SELECT 
            date, date_granularity, 'Google' AS channel,
            SUM(spend) AS spend,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(signups) AS signups,
            SUM(consultation_payment) AS consults,
            0 AS posthog_signups,
            0 AS posthog_consults
        FROM {{ source('reporting', 'googleads_campaign_performance') }}
        GROUP BY 1,2,3

        UNION ALL

        SELECT 
            date, date_granularity, 'Meta' AS channel,
            SUM(spend), SUM(impressions), SUM(link_clicks), SUM(signups),
            SUM(consultation_payment),
            0,0
        FROM {{ source('reporting', 'facebook_ad_performance') }}
        GROUP BY 1,2,3
    ),
    combined_data AS (
        SELECT * FROM platform_data
        UNION ALL
        SELECT * FROM posthog_signups
        UNION ALL
        SELECT * FROM posthog_consults
    )

SELECT
    date,
    date_granularity,
    channel,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(signups) AS signups,
    SUM(consults) AS consults,
    SUM(posthog_signups) AS posthog_signups,
    SUM(posthog_consults) AS posthog_consults
FROM combined_data
WHERE date >= '2024-05-01'
GROUP BY 1,2,3
ORDER BY date DESC, date_granularity, channel

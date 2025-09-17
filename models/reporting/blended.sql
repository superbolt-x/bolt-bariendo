{{ config(
    alias = target.database ~ '_blended'
) }}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
-- Raw sources
consults AS (
    SELECT *, {{ get_date_parts('last_payment_date') }}
    FROM {{ source('s3_raw', 'consults') }}
),
signups AS (
    SELECT *, {{ get_date_parts('first_signup_date') }}
    FROM {{ source('s3_raw', 'signups') }}
),

-- Posthog consults
posthog_consults AS (
    {% for date_granularity in date_granularity_list %}
    SELECT
        DATE_TRUNC('{{ date_granularity }}', last_payment_date) AS date,
        '{{ date_granularity }}' AS date_granularity,
        CASE
            WHEN last_utm_campaign !~* 'gbp-listing' THEN 'Organic'
            WHEN last_utm_source IN ('facebook','fb') THEN 'Meta'
            WHEN last_utm_source IN ('google','youtube') THEN 'Google'
            ELSE 'Other'
        END AS channel,
        0 AS spend,
        0 AS impressions,
        0 AS clicks,
        0 AS signups,
        0 AS consults,
        0 AS posthog_signups,
        COUNT(*) AS posthog_consults
    FROM consults
    GROUP BY 1,2,3
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),

-- Posthog signups
posthog_signups AS (
    {% for date_granularity in date_granularity_list %}
    SELECT
        DATE_TRUNC('{{ date_granularity }}', first_signup_date) AS date,
        '{{ date_granularity }}' AS date_granularity,
        CASE
            WHEN last_utm_campaign !~* 'gbp-listing' THEN 'Organic'
            WHEN last_utm_source IN ('facebook','fb') THEN 'Meta'
            WHEN last_utm_source IN ('google','youtube') THEN 'Google'
            ELSE 'Other'
        END AS channel,
        0 AS spend,
        0 AS impressions,
        0 AS clicks,
        0 AS signups,
        0 AS consults,
        COUNT(*) AS posthog_signups,
        0 AS posthog_consults
    FROM signups
    GROUP BY 1,2,3
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),

-- Paid platform data
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

-- Combine all data (all have identical columns)
combined_data AS (
    SELECT * FROM platform_data
    UNION ALL
    SELECT * FROM posthog_signups
    UNION ALL
    SELECT * FROM posthog_consults
)   -- <--- no trailing comma here

-- Final output
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

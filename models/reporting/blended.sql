{{ config (
    alias = target.database + '_blended'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set channels = ['Google', 'Meta', 'Other'] -%}

WITH 
    posthog_consults_initial AS (
        SELECT *, {{ get_date_parts('last_payment_date') }}
        FROM {{ source('s3_raw', 'consults') }}
    ),
    posthog_signups_initial AS (
        SELECT *, {{ get_date_parts('first_signup_date') }}
        FROM {{ source('s3_raw', 'signups') }}
    ),
    posthog_consults_granular AS (
        {% for date_granularity in date_granularity_list %}
        SELECT 
            {{ date_granularity }} AS date,
            '{{ date_granularity }}' AS date_granularity,
            CASE
                WHEN last_utm_source IN ('facebook', 'fb') THEN 'Meta'
                WHEN last_utm_source IN ('google', 'youtube') THEN 'Google'
                ELSE 'Other'
            END AS channel,
            0 AS spend,
            0 AS impressions,
            0 AS clicks,
            0 AS signups,
            COUNT(*) AS posthog_consults,
            SUM(CASE WHEN last_utm_campaign !~* 'gbp-listing' THEN 1 ELSE 0 END) AS posthog_nonorganic_consults,
            0 AS posthog_signups
        FROM posthog_consults_initial
        GROUP BY 1, 2, 3
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    ),
    posthog_signups_granular AS (
        {% for date_granularity in date_granularity_list %}
        SELECT 
            {{ date_granularity }} AS date,
            '{{ date_granularity }}' AS date_granularity,
            CASE
                WHEN last_utm_source IN ('facebook', 'fb') THEN 'Meta'
                WHEN last_utm_source IN ('google', 'youtube') THEN 'Google'
                ELSE 'Other'
            END AS channel,
            0 AS spend,
            0 AS impressions,
            0 AS clicks,
            0 as signups,
            0 AS posthog_consults,
            0 AS posthog_nonorganic_consults,
            COUNT(*) AS posthog_signups
        FROM posthog_signups_initial
        GROUP BY 1, 2, 3
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    ),
    platform_data AS (
        SELECT 
            date,
            date_granularity,
            'Google' as channel,
            SUM(spend) AS spend,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(signups) AS signups,
            0 AS posthog_signups,
            0 AS posthog_consults,
            0 AS posthog_nonorganic_consults
        FROM {{ source('reporting', 'googleads_campaign_performance') }}
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT 
            date,
            date_granularity,
            'Meta' channel,
            SUM(spend) AS spend,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(signups) AS signups,
            0 AS posthog_signups,
            0 AS posthog_consults,
            0 AS posthog_nonorganic_consults
        FROM {{ source('reporting', 'facebook_ad_performance') }}
        GROUP BY 1, 2, 3 
    ),
    combined_data AS (
        SELECT 
            date,
            date_granularity,
            COALESCE(channel, 'Other') AS channel,
            SUM(spend) AS spend,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(signups) AS signups,
            0 AS posthog_signups,
            0 AS posthog_consults,
            0 AS posthog_nonorganic_consults
        FROM platform_data
        GROUP BY 1, 2, 3
        
        UNION ALL
        
        SELECT 
            date,
            date_granularity,
            COALESCE(channel, 'Other') AS channel,
            0 AS spend,
            0 AS impressions,
            0 AS clicks,
            0 AS signups,
            SUM(posthog_signups) AS posthog_signups,
            0 AS posthog_consults,
            0 AS posthog_nonorganic_consults
        FROM posthog_signups_granular
        GROUP BY 1, 2, 3
        
        UNION ALL
        
        SELECT 
            date,
            date_granularity,
            COALESCE(channel, 'Other') AS channel,
            0 AS spend,
            0 AS impressions,
            0 AS clicks,
            0 AS signups,
            0 AS posthog_signups,
            SUM(posthog_consults) AS posthog_consults,
            SUM(posthog_nonorganic_consults) AS posthog_nonorganic_consults
        FROM posthog_consults_granular
        GROUP BY 1, 2, 3
    )

SELECT
    date,
    date_granularity,
    channel,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(signups) AS signups,
    SUM(posthog_signups) AS posthog_signups,
    SUM(posthog_consults) AS posthog_consults,
    SUM(posthog_nonorganic_consults) AS posthog_nonorganic_consults
FROM combined_data
WHERE date >= '2024-08-01'
GROUP BY 1, 2, 3
ORDER BY date DESC, date_granularity, channel
{{ config (
    alias = target.database + '_marketing_performance_blended'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set channels = ['Google', 'Meta', 'Other'] -%}

WITH 
    {% for date_granularity in date_granularity_list %}
    /* Google Ads Data */
    google_{{ date_granularity }} AS (
        SELECT 
            date_trunc('{{ date_granularity }}', date) AS date,
            '{{ date_granularity }}' AS date_granularity,
            'Google' AS channel,
            SUM(spend) AS spend,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(signups) AS signups,
            SUM(consultation_payment) AS consults
        FROM {{ source('reporting', 'googleads_campaign_performance') }}
        GROUP BY 1, 2, 3
    ),

    /* Meta Ads Data */
    meta_{{ date_granularity }} AS (
        SELECT 
            date_trunc('{{ date_granularity }}', date) AS date,
            '{{ date_granularity }}' AS date_granularity,
            'Meta' AS channel,
            SUM(spend) AS spend,
            SUM(impressions) AS impressions,
            SUM(link_clicks) AS clicks,
            SUM(signups) AS signups,
            SUM(consultation_payment) AS consults
        FROM {{ source('reporting', 'facebook_ad_performance') }}
        GROUP BY 1, 2, 3
    ),
    
    /* PostHog Consults Data */
    posthog_consults_{{ date_granularity }} AS (
        SELECT 
            date_trunc('{{ date_granularity }}', last_payment_date) AS date,
            '{{ date_granularity }}' AS date_granularity,
            CASE
                WHEN last_utm_source IN ('facebook', 'fb') THEN 'Meta'
                WHEN last_utm_source IN ('google', 'youtube') THEN 'Google'
                WHEN last_utm_source IS NULL THEN 'Other'
                ELSE 'Other'
            END AS channel,
            COUNT(*) AS posthog_consults
        FROM {{ source('s3_raw', 'consults') }}
        GROUP BY 1, 2, 3
    ),
    
    /* PostHog Signups Data */
    posthog_signups_{{ date_granularity }} AS (
        SELECT 
            date_trunc('{{ date_granularity }}', first_signup_date) AS date,
            '{{ date_granularity }}' AS date_granularity,
            CASE
                WHEN last_utm_source IN ('facebook', 'fb') THEN 'Meta'
                WHEN last_utm_source IN ('google', 'youtube') THEN 'Google'
                WHEN last_utm_source IS NULL THEN 'Other'
                ELSE 'Other'
            END AS channel,
            COUNT(*) AS posthog_signups
        FROM {{ source('s3_raw', 'signups') }}
        GROUP BY 1, 2, 3
    ),
    
    /* PostHog Non-Organic Consults Data */
    posthog_nonorganic_consults_{{ date_granularity }} AS (
        SELECT 
            date_trunc('{{ date_granularity }}', last_payment_date) AS date,
            '{{ date_granularity }}' AS date_granularity,
            CASE
                WHEN last_utm_source IN ('facebook', 'fb') THEN 'Meta'
                WHEN last_utm_source IN ('google', 'youtube') THEN 'Google'
                WHEN last_utm_source IS NULL THEN 'Other'
                ELSE 'Other'
            END AS channel,
            COUNT(*) AS posthog_nonorganic_consults
        FROM {{ source('s3_raw', 'consults') }}
        WHERE last_utm_campaign !~* 'gbp-listing'
        GROUP BY 1, 2, 3
    ),
    {% endfor %}
    
    /* Union all spend data across granularities */
    spend_data AS (
        {% for date_granularity in date_granularity_list %}
            {% for channel in channels %}
                SELECT * FROM {{ channel|lower }}_{{ date_granularity }}
                {% if not loop.last or not loop.parent.last %}
                UNION ALL
                {% endif %}
            {% endfor %}
        {% endfor %}
    ),
    
    /* Union all PostHog data across granularities */
    posthog_data AS (
        {% for date_granularity in date_granularity_list %}
            SELECT 
                ph_consults.date,
                ph_consults.date_granularity,
                ph_consults.channel,
                COALESCE(ph_consults.posthog_consults, 0) AS posthog_consults,
                COALESCE(ph_signups.posthog_signups, 0) AS posthog_signups,
                COALESCE(ph_nonorganic.posthog_nonorganic_consults, 0) AS posthog_nonorganic_consults
            FROM posthog_consults_{{ date_granularity }} ph_consults
            LEFT JOIN posthog_signups_{{ date_granularity }} ph_signups 
                ON ph_consults.date = ph_signups.date 
                AND ph_consults.date_granularity = ph_signups.date_granularity 
                AND ph_consults.channel = ph_signups.channel
            LEFT JOIN posthog_nonorganic_consults_{{ date_granularity }} ph_nonorganic 
                ON ph_consults.date = ph_nonorganic.date 
                AND ph_consults.date_granularity = ph_nonorganic.date_granularity 
                AND ph_consults.channel = ph_nonorganic.channel
            {% if not loop.last %}
            UNION ALL
            {% endif %}
        {% endfor %}
    )

/* Final output joining spend data with PostHog data */
SELECT
    spend_data.date::date AS date,
    spend_data.date_granularity,
    spend_data.channel,
    spend_data.spend,
    spend_data.impressions,
    spend_data.clicks,
    spend_data.signups,
    spend_data.consults,
    COALESCE(posthog_data.posthog_signups, 0) AS posthog_signups,
    COALESCE(posthog_data.posthog_consults, 0) AS posthog_consults,
    COALESCE(posthog_data.posthog_nonorganic_consults, 0) AS posthog_nonorganic_consults
FROM spend_data
LEFT JOIN posthog_data 
    ON spend_data.date = posthog_data.date 
    AND spend_data.date_granularity = posthog_data.date_granularity 
    AND spend_data.channel = posthog_data.channel
WHERE spend_data.date >= '2024-08-01'
ORDER BY spend_data.date DESC, spend_data.date_granularity, spend_data.channel
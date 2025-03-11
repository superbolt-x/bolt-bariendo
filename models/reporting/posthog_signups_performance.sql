{{ config (
    alias = target.database + '_posthog_signups_performance'
)}}

SELECT
    CASE 
        WHEN last_utm_source IN ('facebook','fb') THEN 'Meta' 
        WHEN last_utm_source IN ('google','youtube') THEN 'Google'
        WHEN last_utm_source IS NULL THEN 'Other'
        ELSE 'Other' 
    END AS channel,
    first_signup_date::date,
    first_utm_event_date::date,
    first_utm_source,
    first_utm_campaign,
    last_utm_event_date::date,
    last_utm_source,
    last_utm_campaign
FROM {{ source('s3_raw', 'signups') }}
ORDER BY first_utm_event_date DESC

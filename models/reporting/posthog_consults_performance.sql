{{ config (
    alias = target.database + '_posthog_consults_performance'
)}}

SELECT
    CASE 
        WHEN last_utm_source IN ('facebook','fb') THEN 'Meta' 
        WHEN last_utm_source IN ('google','youtube') THEN 'Google'
        WHEN last_utm_source IS NULL THEN 'Other'
        ELSE 'Other' 
    END AS channel,
    first_utm_event_date,
    first_utm_source,
    last_utm_source,
    first_utm_campaign,
    last_utm_event_date,
    last_utm_campaign,
    first_payment_date,
    last_payment_date,
    hours_from_last_utm_event_to_payment
FROM {{ source('s3_raw', 'consults') }}
ORDER BY last_utm_event_date DESC

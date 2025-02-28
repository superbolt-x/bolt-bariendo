{{ config (
    alias = target.database + '_posthog_performance'
)}}
WITH posthog_data AS (
    SELECT 
        r.*,  -- Prioritize all columns from signups
        c.first_payment_date,
        c.last_payment_date,
        c.hours_from_last_utm_event_to_payment
    FROM s3_raw.signups r
    LEFT JOIN s3_raw.consults c USING (pkey)
)
SELECT
    CASE 
        WHEN last_utm_source IN ('facebook','fb') THEN 'Meta' 
        WHEN last_utm_source IN ('google','youtube') THEN 'Google'
        WHEN last_utm_source IS NULL THEN 'Other'
        ELSE 'Other' 
    END AS channel,
    first_payment_date,
    last_payment_date,
    first_signup_date,
    last_signup_date,
    first_utm_event_date,
    first_utm_source,
    first_utm_campaign,
    last_utm_event_date,
    last_utm_source,
    last_utm_campaign,
    hours_from_last_utm_event_to_payment
FROM posthog_data
ORDER BY last_payment_date

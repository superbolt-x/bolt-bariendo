{{ config (
    alias = target.database + '_posthog_performance'
)}}
WITH posthog_data AS (
    SELECT 
        COALESCE(s.pkey, c.pkey) AS pkey,
        COALESCE(s.first_signup_date, c.first_signup_date) AS first_signup_date,
        COALESCE(s.first_utm_event_date, c.first_utm_event_date) AS first_utm_event_date,
        COALESCE(s.first_utm_source, c.first_utm_source) AS first_utm_source,
        COALESCE(s.first_utm_campaign, c.first_utm_campaign) AS first_utm_campaign,
        COALESCE(s.last_utm_event_date, c.last_utm_event_date) AS last_utm_event_date,
        COALESCE(s.last_utm_source, c.last_utm_source) AS last_utm_source,
        COALESCE(s.last_utm_campaign, c.last_utm_campaign) AS last_utm_campaign,
        c.first_payment_date,
        c.last_payment_date,
        c.hours_from_last_utm_event_to_payment
    FROM s3_raw.signups s
    FULL JOIN s3_raw.consults c USING (pkey)
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
    first_utm_event_date,
    first_utm_source,
    first_utm_campaign,
    last_utm_event_date,
    last_utm_source,
    last_utm_campaign,
    hours_from_last_utm_event_to_payment
FROM posthog_data
ORDER BY last_payment_date

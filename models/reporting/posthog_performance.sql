{{ config (
    alias = target.database + '_posthog_performance'
)}}

WITH posthog_data as (
    SELECT * FROM s3_raw.signups
    LEFT JOIN s3_raw.consults USING (pkey)
)

SELECT
    case 
        when last_utm_source in ('facebook','fb') then 'Meta' 
        when last_utm_source in ('google','youtube') then 'Google'
        when last_utm_source is null then 'Other'
    else 'Other' end as channel,
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

{{ config (
    alias = target.database + '_posthog_performance'
)}}

WITH posthog_payment_data AS (
    SELECT 
        date_trunc('day', last_utm_event_date) as date,
        'day' as date_granularity,
        CASE 
            WHEN last_utm_source IN ('facebook', 'fb') THEN 'Meta'
            WHEN last_utm_source IN ('google', 'youtube') THEN 'Google'
            ELSE 'Other' 
        END as channel,
        person_id,
        first_payment_date,
        last_payment_date,
        first_utm_event_date,
        first_utm_source,
        first_utm_campaign,
        last_utm_event_date,
        last_utm_source,
        last_utm_campaign,
        hours_from_last_utm_event_to_payment
    FROM {{ source('s3_raw','consults') }}
    
    UNION ALL
    
    SELECT 
        date_trunc('week', last_utm_event_date) as date,
        'week' as date_granularity,
        CASE 
            WHEN last_utm_source IN ('facebook', 'fb') THEN 'Meta'
            WHEN last_utm_source IN ('google', 'youtube') THEN 'Google'
            ELSE 'Other' 
        END as channel,
        person_id,
        first_payment_date,
        last_payment_date,
        first_utm_event_date,
        first_utm_source,
        first_utm_campaign,
        last_utm_event_date,
        last_utm_source,
        last_utm_campaign,
        hours_from_last_utm_event_to_payment
    FROM {{ source('s3_raw','consults') }}
    
    UNION ALL
    
    SELECT 
        date_trunc('month', last_utm_event_date) as date,
        'month' as date_granularity,
        CASE 
            WHEN last_utm_source IN ('facebook', 'fb') THEN 'Meta'
            WHEN last_utm_source IN ('google', 'youtube') THEN 'Google'
            ELSE 'Other' 
        END as channel,
        person_id,
        first_payment_date,
        last_payment_date,
        first_utm_event_date,
        first_utm_source,
        first_utm_campaign,
        last_utm_event_date,
        last_utm_source,
        last_utm_campaign,
        hours_from_last_utm_event_to_payment
    FROM {{ source('s3_raw','consults') }}
),

signup_data AS (
    SELECT 
        date_trunc('day', last_utm_event_date) as date,
        'day' as date_granularity,
        CASE 
            WHEN last_utm_source IN ('facebook', 'fb') THEN 'Meta'
            WHEN last_utm_source IN ('google', 'youtube') THEN 'Google'
            ELSE 'Other' 
        END as channel,
        person_id,
        first_signup_date,
        first_utm_event_date,
        first_utm_source,
        first_utm_campaign,
        last_utm_event_date,
        last_utm_source,
        last_utm_campaign,
        hours_from_last_utm_event_to_signup
    FROM {{ source('s3_raw','signups') }}
),

utm_payment_summary AS (
    SELECT
        date::date as date,
        date_granularity,
        channel,
        COUNT(DISTINCT person_id) as total_customers,
        MIN(first_payment_date) as earliest_payment,
        MAX(last_payment_date) as latest_payment,
        COUNT(*) as payment_count,
        AVG(hours_from_last_utm_event_to_payment) as avg_hours_to_payment
    FROM posthog_payment_data
    GROUP BY date, date_granularity, channel
)

SELECT
    pps.date,
    pps.date_granularity,
    pps.channel,
    pps.total_customers,
    pps.payment_count,
    pps.earliest_payment,
    pps.latest_payment,
    pps.avg_hours_to_payment,
    pd.first_payment_date,
    pd.last_payment_date,
    pd.first_utm_event_date,
    pd.first_utm_source,
    pd.first_utm_campaign,
    pd.last_utm_event_date,
    pd.last_utm_source,
    pd.last_utm_campaign,
    pd.hours_from_last_utm_event_to_payment
FROM utm_payment_summary pps
JOIN posthog_payment_data pd 
    ON pps.date = pd.date 
    AND pps.date_granularity = pd.date_granularity 
    AND pps.channel = pd.channel
ORDER BY pps.date DESC, pps.channel

{{ config (
    alias = target.database + '_posthog_signups_performance'
)}}

WITH initial_google_data as
    (SELECT *, SPLIT_PART(first_utm_campaign::varchar,'-cross-network',1) as first_campaign_id, SPLIT_PART(last_utm_campaign::varchar,'-cross-network',1) as last_campaign_id
    FROM {{ source('s3_raw', 'signups') }}
    WHERE last_utm_source IN ('google','youtube')
    ),

    google_data as
    (SELECT CASE WHEN last_utm_source IN ('google','youtube') THEN 'Google' ELSE 'Other' END AS channel,
        first_signup_date::date, first_utm_event_date::date, first_utm_source, last_utm_source, gc.first_utm_campaign, last_utm_event_date::date, gc.last_utm_campaign
    FROM initial_google_data
    LEFT JOIN 
        (SELECT count(*), campaign_id::varchar as first_campaign_id, campaign_id::varchar as last_campaign_id, campaign_name as first_utm_campaign, campaign_name as last_utm_campaign 
        FROM {{ source('reporting', 'googleads_campaign_performance') }} 
        GROUP BY 2,3,4,5) gc
    USING (first_campaign_id, last_campaign_id)
    WHERE channel = 'Google'
    ),

    other_data as
    (SELECT CASE 
            WHEN last_utm_source IN ('facebook','fb') THEN 'Meta' 
            WHEN last_utm_source IN ('google','youtube') THEN 'Google'
            WHEN last_utm_source IS NULL THEN 'Other'
            ELSE 'Other' 
        END AS channel,
        first_signup_date::date, first_utm_event_date::date, first_utm_source, last_utm_source, g.first_utm_campaign, last_utm_event_date::date, g.last_utm_campaign
    FROM {{ source('s3_raw', 'signups') }}
    WHERE channel != 'Google'
    )
    
SELECT 
    channel,
    first_signup_date
    first_utm_event_date,
    first_utm_source,
    last_utm_source,
    first_utm_campaign,
    last_utm_event_date,
    last_utm_campaign
FROM 
    (SELECT * FROM google_data
    UNION ALL
    SELECT * FROM other_data)
ORDER BY first_utm_event_date DESC

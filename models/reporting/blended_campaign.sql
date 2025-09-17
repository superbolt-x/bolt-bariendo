{{ config(
    alias = target.database ~ '_blended_campaign'
) }}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH spend_data AS (
    SELECT date, date_granularity, 'Google' AS channel, campaign_name, 
           MIN(campaign_id::text) AS campaign_id,
           SUM(spend) AS spend, SUM(impressions) AS impressions, SUM(clicks) AS clicks,
           SUM(signups) AS signups, SUM(consultation_payment) AS consultation_payment
    FROM reporting.bariendo_googleads_campaign_performance
    GROUP BY 1,2,3,4
    UNION ALL
    SELECT date, date_granularity, 'Meta' AS channel, campaign_name, 
           MIN(campaign_id::text) AS campaign_id,
           SUM(spend), SUM(impressions), SUM(link_clicks), SUM(signups), SUM(consultation_payment)
    FROM reporting.bariendo_facebook_ad_performance
    GROUP BY 1,2,3,4
),

posthog_consults_data AS (
    {{ posthog_granular_campaign('reporting.bariendo_posthog_consults_performance', 'last_payment_date', 'posthog_consults') }}
),

posthog_signups_data AS (
    {{ posthog_granular_campaign('reporting.bariendo_posthog_signups_performance', 'first_signup_date', 'posthog_signups') }}
),

campaign_id_lookup AS (
    SELECT DISTINCT channel, campaign_name, MIN(campaign_id) AS campaign_id
    FROM spend_data
    WHERE campaign_id IS NOT NULL AND campaign_id != 'unknown'
    GROUP BY channel, campaign_name
),

all_campaigns AS (
    SELECT DISTINCT date, date_granularity, channel, campaign_name FROM spend_data
    UNION
    SELECT DISTINCT date, date_granularity, channel, campaign_name FROM posthog_consults_data
    UNION
    SELECT DISTINCT date, date_granularity, channel, campaign_name FROM posthog_signups_data
),

blended_data AS (
    SELECT
        ac.date::date AS date,
        ac.date_granularity,
        ac.channel,
        ac.campaign_name,
        COALESCE(sd.campaign_id, cil.campaign_id, 'unknown') AS campaign_id,
        COALESCE(sd.spend, 0) AS spend,
        COALESCE(sd.impressions, 0) AS impressions,
        COALESCE(sd.clicks, 0) AS clicks,
        COALESCE(sd.signups, 0) AS signups,
        COALESCE(sd.consultation_payment, 0) AS consultation_payment,
        COALESCE(psg.posthog_signups, 0) AS posthog_signups,
        COALESCE(pc.posthog_consults, 0) AS posthog_consults
    FROM all_campaigns ac
    LEFT JOIN spend_data sd
        ON ac.date = sd.date
       AND ac.date_granularity = sd.date_granularity
       AND ac.channel = sd.channel
       AND ac.campaign_name = sd.campaign_name
    LEFT JOIN campaign_id_lookup cil
        ON ac.channel = cil.channel
       AND ac.campaign_name = cil.campaign_name
    LEFT JOIN posthog_signups_data psg
        ON ac.date = psg.date
       AND ac.date_granularity = psg.date_granularity
       AND ac.channel = psg.channel
       AND ac.campaign_name = psg.campaign_name
    LEFT JOIN posthog_consults_data pc
        ON ac.date = pc.date
       AND ac.date_granularity = pc.date_granularity
       AND ac.channel = pc.channel
       AND ac.campaign_name = pc.campaign_name
    WHERE ac.date >= '2024-08-01'
)

SELECT 
    date,
    date_granularity,
    channel,
    campaign_name,
    COALESCE(MAX(CASE WHEN campaign_id != 'unknown' THEN campaign_id END), 'unknown') AS campaign_id,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(signups) AS signups,
    SUM(consultation_payment) AS consultation_payment,
    SUM(posthog_signups) AS posthog_signups,
    SUM(posthog_consults) AS posthog_consults
FROM blended_data
GROUP BY 1,2,3,4
ORDER BY date DESC, channel, campaign_name

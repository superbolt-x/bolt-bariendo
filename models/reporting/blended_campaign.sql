{{ config(
    alias = target.database ~ '_blended_campaign'
) }}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH spend_data AS (
    SELECT date, date_granularity, 'Google' AS channel, campaign_name, 
           MIN(campaign_id::text) AS campaign_id,
           SUM(spend) AS spend, SUM(impressions) AS impressions, SUM(clicks) AS clicks,
           SUM(signups) AS signups, SUM(consultation_payment) AS consultation_payment,
           0 AS posthog_signups, 0 AS posthog_consults
    FROM reporting.bariendo_googleads_campaign_performance
    GROUP BY 1,2,3,4

    UNION ALL

    SELECT date, date_granularity, 'Meta' AS channel, campaign_name, 
           MIN(campaign_id::text) AS campaign_id,
           SUM(spend), SUM(impressions), SUM(link_clicks), SUM(signups), SUM(consultation_payment),
           0,0
    FROM reporting.bariendo_facebook_ad_performance
    GROUP BY 1,2,3,4
),

-- Posthog consults with aligned schema
posthog_consults_data AS (
    {% for date_granularity in date_granularity_list %}
    SELECT 
        DATE_TRUNC('{{ date_granularity }}', last_payment_date) AS date,
        '{{ date_granularity }}' AS date_granularity,
        CASE
            WHEN last_utm_campaign !~* 'gbp-listing' THEN 'Organic'
            WHEN p.channel = 'Meta' THEN 'Meta'
            WHEN p.channel = 'Google' THEN 'Google'
            ELSE 'Other'
        END AS channel,
        CASE
            WHEN p.channel = 'Meta' THEN 
                COALESCE(fb_lookup.campaign_name,
                         REPLACE(REPLACE(REPLACE(last_utm_campaign, '- Adv ', '- Adv+ '), '  ', ' '), 'Campaign Campaign', 'Campaign'))
            WHEN p.channel = 'Google' THEN COALESCE(g_lookup.campaign_name, last_utm_campaign)
            ELSE last_utm_campaign
        END AS campaign_name,
        NULL::text AS campaign_id,
        0 AS spend, 0 AS impressions, 0 AS clicks, 0 AS signups, 0 AS consultation_payment,
        0 AS posthog_signups,
        COUNT(*) AS posthog_consults
    FROM reporting.bariendo_posthog_consults_performance p
    LEFT JOIN (
        SELECT DISTINCT campaign_id::text, campaign_name
        FROM reporting.bariendo_facebook_ad_performance
    ) fb_lookup ON p.last_utm_campaign = fb_lookup.campaign_id::text
               AND p.channel = 'Meta'
    LEFT JOIN (
        SELECT DISTINCT campaign_id::text, campaign_name
        FROM reporting.bariendo_googleads_campaign_performance
    ) g_lookup ON p.last_utm_campaign = g_lookup.campaign_id::text
               AND p.channel = 'Google'
    WHERE last_utm_campaign IS NOT NULL
    GROUP BY 1,2,3,4
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),

-- Posthog signups with aligned schema
posthog_signups_data AS (
    {% for date_granularity in date_granularity_list %}
    SELECT 
        DATE_TRUNC('{{ date_granularity }}', first_signup_date) AS date,
        '{{ date_granularity }}' AS date_granularity,
        CASE
            WHEN last_utm_campaign !~* 'gbp-listing' THEN 'Organic'
            WHEN p.channel = 'Meta' THEN 'Meta'
            WHEN p.channel = 'Google' THEN 'Google'
            ELSE 'Other'
        END AS channel,
        CASE
            WHEN p.channel = 'Meta' THEN 
                COALESCE(fb_lookup.campaign_name,
                         REPLACE(REPLACE(REPLACE(last_utm_campaign, '- Adv ', '- Adv+ '), '  ', ' '), 'Campaign Campaign', 'Campaign'))
            WHEN p.channel = 'Google' THEN COALESCE(g_lookup.campaign_name, last_utm_campaign)
            ELSE last_utm_campaign
        END AS campaign_name,
        NULL::text AS campaign_id,
        0 AS spend, 0 AS impressions, 0 AS clicks, 0 AS signups, 0 AS consultation_payment,
        COUNT(*) AS posthog_signups,
        0 AS posthog_consults
    FROM reporting.bariendo_posthog_signups_performance p
    LEFT JOIN (
        SELECT DISTINCT campaign_id::text, campaign_name
        FROM reporting.bariendo_facebook_ad_performance
    ) fb_lookup ON p.last_utm_campaign = fb_lookup.campaign_id::text
               AND p.channel = 'Meta'
    LEFT JOIN (
        SELECT DISTINCT campaign_id::text, campaign_name
        FROM reporting.bariendo_googleads_campaign_performance
    ) g_lookup ON p.last_utm_campaign = g_lookup.campaign_id::text
               AND p.channel = 'Google'
    WHERE last_utm_campaign IS NOT NULL
    GROUP BY 1,2,3,4
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),

-- Ensure all campaigns are represented
all_campaigns AS (
    SELECT DISTINCT date, date_granularity, channel, campaign_name FROM spend_data
    UNION
    SELECT DISTINCT date, date_granularity, channel, campaign_name FROM posthog_consults_data
    UNION
    SELECT DISTINCT date, date_granularity, channel, campaign_name FROM posthog_signups_data
),

-- Campaign ID lookup
campaign_id_lookup AS (
    SELECT DISTINCT channel, campaign_name, MIN(campaign_id) AS campaign_id
    FROM spend_data
    WHERE campaign_id IS NOT NULL AND campaign_id != 'unknown'
    GROUP BY channel, campaign_name
),

-- Final blend
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

{% macro posthog_granular_campaign(source_table, date_col, metric) %}
    {% for date_granularity in date_granularity_list %}
    SELECT 
        DATE_TRUNC('{{ date_granularity }}', {{ date_col }}) AS date,
        '{{ date_granularity }}' AS date_granularity,
        CASE
            WHEN last_utm_campaign !~* 'gbp-listing' THEN 'Organic'
            WHEN channel = 'Meta' THEN 'Meta'
            WHEN channel = 'Google' THEN 'Google'
            ELSE 'Other'
        END AS channel,
        CASE
            WHEN channel = 'Meta' THEN 
                COALESCE(
                    fb_lookup.campaign_name,
                    REPLACE(REPLACE(REPLACE(last_utm_campaign, '- Adv ', '- Adv+ '), '  ', ' '), 'Campaign Campaign', 'Campaign')
                )
            WHEN channel = 'Google' THEN 
                COALESCE(g_lookup.campaign_name, last_utm_campaign)
            ELSE last_utm_campaign
        END AS campaign_name,
        COUNT(*) AS {{ metric }}
    FROM {{ source_table }} p
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
{% endmacro %}

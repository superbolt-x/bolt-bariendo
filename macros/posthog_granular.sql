{% macro posthog_granular(source_table, date_col, metric) %}
  {%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
  {% for date_granularity in date_granularity_list %}
    SELECT 
        DATE_TRUNC('{{ date_granularity }}', {{ date_col }}) AS date,
        '{{ date_granularity }}' AS date_granularity,
        CASE
            WHEN last_utm_campaign !~* 'gbp-listing' THEN 'Organic'
            WHEN last_utm_source IN ('facebook','fb') THEN 'Meta'
            WHEN last_utm_source IN ('google','youtube') THEN 'Google'
            ELSE 'Other'
        END AS channel,
        COUNT(*) AS {{ metric }}
    FROM {{ source_table }}
    GROUP BY 1,2,3
    {% if not loop.last %} UNION ALL {% endif %}
  {% endfor %}
{% endmacro %}

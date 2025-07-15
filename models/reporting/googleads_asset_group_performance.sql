{{ config (
    alias = target.database + '_googleads_asset_group_performance'
)}}

{%- set granularities = ['day', 'week', 'month', 'quarter', 'year'] -%}
    
with google_data as (    
    SELECT 
    campaign_name,
    campaign_id,
    campaign_status,
    campaign_type_default,
    asset_group_name,
    asset_group_id,
    asset_group_status,
    date,
    date_granularity,
    spend,
    impressions,
    clicks,
    conversions as purchases,
    conversions_value as revenue,
    0 as consultation_payment
    FROM {{ ref('googleads_performance_by_asset_group') }}
    )

{% for granularity in granularities %}
, {{ granularity }}_agg_convtype as (
    select 
        campaign_name,
        campaign_id,
        null as campaign_status,
        null as campaign_type_default,
        asset_group_name,
        asset_group_id,
        null as asset_group_status,
        date_trunc('{{ granularity }}', date) as date,
        '{{ granularity }}' as date_granularity,
        sum(0) as spend,
        sum(0) as impressions,
        sum(0) as clicks,
        sum(0) as purchases,
        sum(0) as revenue,
        sum(CASE WHEN conversion_action_name = 'Consultation Payment' THEN conversions END) as consultation_payment
    from {{ source('gsheet_raw','asset_group_convtype_insights') }}
    group by 
        campaign_name,
        campaign_id,
        asset_group_name,
        asset_group_id,
        date_trunc('{{ granularity }}', date)
)
{% endfor %}

select * from day_agg_convtype
{% for granularity in granularities[1:] %}
union all
select * from {{ granularity }}_agg_convtype
{% endfor %}
union all
select * from google_data

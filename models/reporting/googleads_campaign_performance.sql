{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}
    
SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share,
CASE WHEN date BETWEEN '2024-01-01' AND '2024-03-31' THEN appbariendocomwebregistersuccess ELSE signup END as signups,
CASE WHEN date BETWEEN '2024-01-01' AND '2024-03-31' THEN reservationpayment ELSE consultationpayment END as consultation_payment
FROM {{ref('googleads_performance_by_campaign')}}

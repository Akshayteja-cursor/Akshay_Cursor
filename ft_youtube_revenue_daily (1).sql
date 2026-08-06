

select
    date::date,
    'News and Business' as business_group,
    'FOX NEWS AND BUSINESS' as business_unit,
    rpm as revenue_per_mile,
    estimated_monetized_playbacks,
    playback_based_cpm,
    cpm,
    ad_impressions as advertiser_impressions,
    estimated_adsense_revenue,
    estimated_doubleclick_revenue,
    youtube_ad_revenue,
    estimated_partner_ad_revenue,
    youtube_premium_revenue,
    revenue_per_transaction,
    transactions,
    transaction_revenue,
    views,
    watch_time as watch_time_hours,
    average_view_duration::decimal(22,7),
    estimated_partner_revenue estimated_partner_revenue,
    CURRENT_TIMESTAMP() as created_timestamp,
    CURRENT_TIMESTAMP() as updated_timestamp,
    0 as is_delete_flag,
    -1 as load_id
from
    fox_bi_qa.bronze_ad_sales.stg_youtube_news_revenue_daily

UNION

select
    date::date,
    'Entertainment' as business_group,
    'FOX ENTERTAINMENT'  as business_unit,
    rpm,
    estimated_monetized_playbacks,
    playback_based_cpm,
    cpm,
    ad_impressions,
    estimated_adsense_revenue,
    estimated_doubleclick_revenue,
    youtube_ad_revenue,
    estimated_partner_ad_revenue,
    youtube_premium_revenue,
    revenue_per_transaction,
    transactions,
    transaction_revenue,
    views,
    watch_time as watch_time_hours,
    average_view_duration::decimal(22,7),
    estimated_partner_revenue,
    current_timestamp() as created_timestamp,
    current_timestamp() as updated_timestamp,
    0 as is_delete_flag,
    -1 as load_id
from
    fox_bi_qa.bronze_ad_sales.stg_youtube_entertainment_revenue_daily

UNION

select
    date::date,
    'Sports' as business_group,
    'FOX SPORTS' as business_unit,
    rpm ,
    estimated_monetized_playbacks ,
    playback_based_cpm ,
    cpm ,
    ad_impressions ,
    estimated_adsense_revenue ,
    estimated_doubleclick_revenue ,
    youtube_ad_revenue ,
    estimated_ad_revenue as estimated_partner_ad_revenue ,
    youtube_premium_revenue ,
    revenue_per_transaction ,
    transactions ,
    transaction_revenue ,
    views,
    watch_time_hours,
    average_view_duration::decimal(22,7),
    estimated_revenue as estimated_partner_revenue ,
    CURRENT_TIMESTAMP() as created_timestamp,
    CURRENT_TIMESTAMP() as updated_timestamp,
    0 as is_delete_flag,
    -1 as load_id
from
    fox_bi_qa.bronze_ad_sales.stg_youtube_sports_revenue_daily

UNION

select
    date::date,
    'OutKick' as business_group,
    'FOX NEWS' as business_unit,
    rpm ,
    estimated_monetized_playbacks ,
    playback_based_cpm ,
    cpm ,
    ad_impressions ,
    estimated_adsense_revenue ,
    estimated_doubleclick_revenue ,
    youtube_ad_revenue ,
    estimated_ad_revenue as estimated_partner_ad_revenue ,
    youtube_premium_revenue ,
    revenue_per_transaction ,
    transactions ,
    transaction_revenue ,
    views,
    watch_time_hours,
    average_view_duration::decimal(22,7),
    estimated_revenue as estimated_partner_revenue ,
    CURRENT_TIMESTAMP() as created_timestamp,
    CURRENT_TIMESTAMP() as updated_timestamp,
    0 as is_delete_flag,
    -1 as load_id
from
   fox_bi_qa.bronze_ad_sales.stg_youtube_outkick_revenue_daily

UNION

select
    date::date,
    'TMZ' as business_group,
    'FOX NEWS' as business_unit,
    rpm ,
    estimated_monetized_playbacks ,
    playback_based_cpm ,
    cpm ,
    ad_impressions ,
    estimated_adsense_revenue ,
    estimated_doubleclick_revenue ,
    youtube_ad_revenue ,
    estimated_ad_revenue as estimated_partner_ad_revenue ,
    youtube_premium_revenue ,
    revenue_per_transaction ,
    transactions ,
    transaction_revenue ,
    views,
    watch_time_hours,
    average_view_duration::decimal(22,7),
    estimated_revenue as estimated_partner_revenue ,
    CURRENT_TIMESTAMP() as created_timestamp,
    CURRENT_TIMESTAMP() as updated_timestamp,
    0 as is_delete_flag,
    -1 as load_id
from
     fox_bi_qa.bronze_ad_sales.stg_youtube_tmz_revenue_daily
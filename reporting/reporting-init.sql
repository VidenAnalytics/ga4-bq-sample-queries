-- Initial code: Taneli Salonen, https://tanelytics.com/ga4-bigquery-session-traffic_source/

-- Initialisation script

declare end_date string default (select format_date('%Y%m%d',date_sub(current_date(), interval 33 day)));
declare end_date string default (select format_date('%Y%m%d',date_sub(current_date(), interval 3 day)));


-- 1. Create session_traffic_prep table

create or replace table `your_project.reporting.src_event_traffic` partition by date as (

select
  cast(event_date as date format 'YYYYMMDD') as date,
  -- Unique session id. user_pseudo_id or ga_session_id fields could missing
  concat(coalesce(user_pseudo_id, ""), coalesce(cast((select value.int_value from unnest(event_params) where key = "ga_session_id") as string), "")) as session_id,
  user_pseudo_id,
  (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_start,
    -- Wrap all traffic source dimensions into a struct for the next step
  (
    select
      as struct (select value.string_value from unnest(event_params) where key = 'source') as source,
      (select value.string_value from unnest(event_params) where key = 'medium') as medium,
      (select value.string_value from unnest(event_params) where key = 'campaign') as campaign,
      (select value.string_value from unnest(event_params) where key = 'gclid') as gclid
  ) as traffic_source,
  event_timestamp,
  (select value.string_value from unnest(event_params) where key='page_location') as page_location
from
    `your_project.your_dataset.events_*`
where
  (_table_suffix between start_date and end_date)
  and
  event_name not in ('session_start', 'first_visit'));


-- 2. Create session_traffic table

create or replace table `your_project.reporting.stg_session_traffic` partition by date as (

with attribution_prep as (
select
  min(date) as date,
  session_id,
  user_pseudo_id,
  session_start,
  -- The traffic source of the first event in the session with session_start and first_visit excluded
  array_agg(
    if(
      coalesce(traffic_source.source, traffic_source.medium, traffic_source.campaign, traffic_source.gclid) is not null,
      (
        select
          as struct
            case when traffic_source.gclid is not null or page_location like "http%gclid=%" then "google"
            when page_location like "http%fbclid=%" then "facebook"
            when page_location like "http%ttclid=%" then "tiktok"
            when page_location like "http%msclkid=%" then "bing"
            else traffic_source.source end  as source,

            case when traffic_source.gclid is not null or page_location like "http%gclid=%" then "cpc"
            when page_location like "http%fbclid=%" then "paidsocial"
            when page_location like "http%ttclid=%" then "paidsocial"
            when page_location like "http%msclkid=%" then "cpc"
            else traffic_source.medium end  as medium,

            traffic_source.campaign,
            traffic_source.gclid
      ),
      null
    )
    order by
      event_timestamp asc
    limit
      1
  ) [safe_offset(0)] as session_first_traffic_source,
  -- The last not null traffic source of the session
  array_agg(
    if(
      coalesce(traffic_source.source,traffic_source.medium,traffic_source.campaign,traffic_source.gclid) is not null,
      (
        select
          as struct
            case when traffic_source.gclid is not null or page_location like "http%gclid=%" then "google"
            when page_location like "http%fbclid=%" then "facebook"
            when page_location like "http%ttclid=%" then "tiktok"
            when page_location like "http%msclkid=%" then "bing"
            else traffic_source.source end  as source,

            case when traffic_source.gclid is not null or page_location like "http%gclid=%" then "cpc"
            when page_location like "http%fbclid=%" then "paidsocial"
            when page_location like "http%ttclid=%" then "paidsocial"
            when page_location like "http%msclkid=%" then "cpc"
            else traffic_source.medium end  as medium,

            traffic_source.campaign,
            traffic_source.gclid
      ),
      null
    ) ignore nulls
    order by
      event_timestamp desc
    limit
      1
  ) [safe_offset(0)] as session_last_traffic_source
from
  `your_project.reporting.src_event_traffic`
where
  session_id is not null
group by
  session_id,
  user_pseudo_id,
  session_start),

last_non_direct as (
select
  date,
  session_id,
  user_pseudo_id,
  session_start,
  ifnull(
    session_first_traffic_source,
    last_value(session_last_traffic_source ignore nulls) over(
      partition by user_pseudo_id
      order by
        session_start range between 2592000 preceding
        and current row -- 30 day lookback
    )
  ) as session_traffic_source_last_non_direct,
from
  attribution_prep)

select
  session_id,
  user_pseudo_id,
  date,
  session_start,
  ifnull(session_traffic_source_last_non_direct.source,'(direct)') as source,
  ifnull(session_traffic_source_last_non_direct.medium,'(none)') as medium,
  ifnull(session_traffic_source_last_non_direct.campaign,'(not set)') as campaign
from
  last_non_direct);


-- 3. Create events table

create or replace table `your_project.reporting.events` partition by event_date as (

with event_params as (
select
  concat(coalesce(user_pseudo_id, ""),
          coalesce(
              cast((select value.int_value from unnest(event_params) where key = "ga_session_id") as string), "")
              ) as session_id,
  parse_date("%y%m%d",event_date) as event_date,
  timestamp_micros(event_timestamp) as event_timestamp,
  event_name,
  (select value.int_value from unnest(event_params) where key = "ga_session_number") as ga_session_number,
  (select value.int_value from unnest(event_params) where key = "ga_session_id") as ga_session_id,
  (select value.string_value from unnest(event_params) where key = "page_location") as page_location,
  (select value.int_value from unnest(event_params) where key = "entrances") as entrances,
  coalesce((select value.string_value from unnest(event_params) where key = "session_engaged"),
    cast((select value.int_value from unnest(event_params) where key = "session_engaged") as string)) as session_engaged,
  user_id,
  user_pseudo_id,
  device.category as device_category,
  device.language as device_language,
  geo.country as geo_country,
  traffic_source.source as user_source,
  traffic_source.medium as user_medium,
  traffic_source.name as user_campaign,
  ecommerce.total_item_quantity as ecommerce_total_item_quantity,
  ecommerce.purchase_revenue as ecommerce_purchase_revenue,
  ecommerce.refund_value as ecommerce_refund_value,
  ecommerce.shipping_value as ecommerce_shipping_value,
  ecommerce.tax_value as ecommerce_tax_value,
  ecommerce.unique_items as ecommerce_unique_items,
  ecommerce.transaction_id as ecommerce_transaction_id,
  (select value.string_value from unnest(event_params) where key = "event_category") as event_category,
  (select value.string_value from unnest(event_params) where key = "event_action") as event_action,
  (select value.string_value from unnest(event_params) where key = "event_label") as event_label
from
  `your_project.your_dataset.events_*`
where
  _table_suffix between start_date and end_date)

select
  event_params.*,
  session_source.source as session_source,
  session_source.medium as session_medium,
  session_source.campaign as session_campaign,
  session_source.session_start as session_start
from
  event_params
left join
  (select * from `your_project.reporting.stg_session_traffic`) as session_source on event_params.session_id = session_source.session_id);


-- 4. Create items table

create or replace table `your_project.reporting.items` partition by date as (

select
  cast(event_date as date format 'YYYYMMDD') as date,
  items.item_name as item_name,
  items.item_variant as item_variant,
  items.quantity as item_quantity,
  items.item_revenue as item_revenue
from
   `your_project.your_dataset.events_*`, unnest(items) as items
where
  (_table_suffix between start_date and end_date)
  and
  event_name = 'purchase');

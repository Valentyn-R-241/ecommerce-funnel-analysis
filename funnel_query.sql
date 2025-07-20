-- Extract session-level data with traffic and device info
WITH session_data AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    event_date AS session_date,

    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,
    CONCAT(traffic_source.medium, ' / ', traffic_source.name) AS medium_campaign,

    device.category AS device_category,
    device.operating_system AS operating_system,
    device.language AS device_language,
    geo.country AS country,

    -- Get landing page URL from event parameters
    (SELECT value.string_value 
     FROM UNNEST(event_params)
     WHERE key = 'page_location') AS landing_page

  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20210101' AND '20210430'
    AND event_name = 'session_start'
),

-- Get all funnel-related events with session ID
funnel_events AS (
  SELECT
    user_pseudo_id,
    event_name,
    event_timestamp,
    event_date,

    -- Extract ga_session_id from event parameters
    (SELECT value.int_value 
     FROM UNNEST(event_params)
     WHERE key = 'ga_session_id') AS ga_session_id

  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20210101' AND '20210430'
    AND event_name IN (
      'session_start',
      'view_item',
      'add_to_cart',
      'begin_checkout',
      'add_shipping_info',
      'add_payment_info',
      'purchase'
    )
)

-- Join session data with funnel events using user + session ID
SELECT
  CONCAT(s.user_pseudo_id, CAST(f.ga_session_id AS STRING)) AS user_session_id,
  s.user_pseudo_id,
  s.session_date,
  s.source,
  s.medium,
  s.campaign,
  s.medium_campaign,
  s.device_category,
  s.operating_system,
  s.device_language,
  s.country,
  s.landing_page,

  f.event_name,
  f.event_timestamp

FROM session_data s
LEFT JOIN funnel_events f
  ON s.user_pseudo_id = f.user_pseudo_id
  AND s.session_date = f.event_date

ORDER BY s.user_pseudo_id, f.event_timestamp
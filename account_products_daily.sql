CREATE OR REPLACE TABLE hubspot.account_products_daily as 
WITH
  dates AS (
  SELECT
    DISTINCT date
  FROM
    hubspot.object_counts_daily),
  accounts AS (
  SELECT
    DISTINCT account_id
  FROM
    hubspot.object_counts_daily ),
  accounts_and_dates AS (
  SELECT
    d.date,
    a.account_id,
  FROM
    accounts a
  CROSS JOIN
    dates d ),
  product_usage AS (
  SELECT
    DATE(event_time) AS date,
    account_id,
    product,
    COUNT(DISTINCT event_id) AS usage,
    COUNT(DISTINCT user_id) AS active_users
  FROM
    hubspot.usage_events
  GROUP BY
    1,
    2,
    3 ),
  last28 AS (
  SELECT
    date,
    account_id,
    product,
    usage,
    active_users,
    SUM(usage) OVER(PARTITION BY account_id, product ORDER BY UNIX_DATE(date) RANGE BETWEEN 27 PRECEDING
      AND CURRENT ROW) AS usage_last28d
  FROM
    product_usage ),
  prior_period AS (
  SELECT
    *,
    LAG(usage_last28d,28) OVER(PARTITION BY account_id, product ORDER BY date) AS usage_prior28d
  FROM
    last28 ),
  usage_changes AS (
  SELECT
    *,
    usage_last28d - usage_prior28d AS usage_change_last28d,
    (usage_last28d - usage_prior28d) / usage_prior28d AS usage_perc_change_last28d,
    SUM(usage) OVER (PARTITION BY account_id, product ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS usage_cumulative
  FROM
    prior_period),
  object_changes AS (
  SELECT
    date,
    account_id,
    total_deals,
    LAG(total_deals,28) OVER(PARTITION BY account_id ORDER BY date) AS total_deals_prior28d
  FROM
    hubspot.object_counts_daily ),
  combined AS (
  SELECT
    p.date,
    p.account_id,
    o.total_deals,
    o.total_deals_prior28d,
    o.total_deals - o.total_deals_prior28d AS total_deals_change_last28d,
    (o.total_deals - o.total_deals_prior28d) / o.total_deals_prior28d AS total_deals_perc_change_last28d,
    s.tier AS subscription_tier,
    p.usage,
    p.usage_last28d,
    p.usage_prior28d,
    p.usage_change_last28d,
    p.usage_perc_change_last28d,
    CASE
      WHEN p.usage_cumulative > 0 THEN 1
      ELSE 0
  END
    AS is_activated,
    p.usage_cumulative,
    CASE
      WHEN usage_last28d >= 50 THEN 1
      ELSE 0
  END
    AS is_established,
    CASE
      WHEN usage > 0 THEN 1
      ELSE 0
  END
    AS is_active,
    CASE
      WHEN usage_last28d > 0 THEN 1
      ELSE 0
  END
    AS is_active_last28d,
    CASE
      WHEN usage_perc_change_last28d <= -0.2 THEN 'High Contraction'
      WHEN usage_perc_change_last28d < -0.1 THEN 'Moderate Contraction'
      WHEN usage_perc_change_last28d < 0.1 THEN 'Flat'
      WHEN usage_perc_change_last28d < 0.2 THEN 'Moderate Growth'
      WHEN usage_perc_change_last28d >= 0.2 THEN 'High Growth'
  END
    AS usage_change_type
  FROM
    usage_changes p
  LEFT JOIN
    object_changes o
  USING
    (account_id,
      date)
  LEFT JOIN
    hubspot.active_subscriptions_daily s
  USING
    (account_id,
      date,
      product) )
SELECT
  *,
  CASE
    WHEN is_activated = 0 THEN 'Unactivated'
    WHEN is_established = 1 THEN 'Established'
    WHEN SUM(is_established) OVER(PARTITION BY account_id) > 0 AND usage_last28d > 0 AND is_established = 0 THEN 'Unestablished'
    WHEN is_activated = 1
  AND usage_last28d = 0 THEN 'Abandoned'
    WHEN is_activated = 1 AND usage_last28d > 0 AND is_established = 0 AND SUM(is_established) OVER(PARTITION BY account_id) = 0 THEN 'Activated'
END
  AS adoption_stage
FROM
  combined

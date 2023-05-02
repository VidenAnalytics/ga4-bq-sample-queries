WITH
  first_visits AS (
    SELECT
      user_pseudo_id,
      event_timestamp,
      DATE(TIMESTAMP_MICROS(event_timestamp)) AS acquisition_date
    FROM
      `your_project_id.your_dataset_id.events_*`
    WHERE
      event_name = 'session_start'
      AND
      (_TABLE_SUFFIX BETWEEN '20230305' AND '20230422')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY user_pseudo_id ORDER BY event_timestamp) = 1
  ),
  retention_data AS (
    SELECT
      a.user_pseudo_id,
      a.acquisition_date,
      DATE(TIMESTAMP_MICROS(b.event_timestamp)) AS retention_date,
      TIMESTAMP_DIFF(TIMESTAMP_MICROS(b.event_timestamp), TIMESTAMP_MICROS(a.event_timestamp), DAY) AS days_since_first_visit
    FROM
      first_visits a
    JOIN
      `your_project_id.your_dataset_id.events_*` b
    ON
      a.user_pseudo_id = b.user_pseudo_id
    WHERE
      b.event_name = 'session_start'
    AND
    (_TABLE_SUFFIX BETWEEN '20230305' AND '20230422')
  ),
  retention_cohorts AS (
    SELECT
      acquisition_date,
      days_since_first_visit,
      COUNT(DISTINCT user_pseudo_id) AS user_count
    FROM
      retention_data
    GROUP BY
      acquisition_date,
      days_since_first_visit
  )
SELECT
  acquisition_date,
  ARRAY_AGG(STRUCT(days_since_first_visit, user_count)) AS retention_array
FROM
  retention_cohorts
GROUP BY
  acquisition_date
ORDER BY
  acquisition_date;

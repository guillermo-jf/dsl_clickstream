CREATE VIEW `dsl_project.vw_total_page_views_minute` AS
SELECT
  -- Truncate the visit_start_time to the minute for aggregation
  TIMESTAMP_TRUNC(visit_start_time, MINUTE) AS visit_minute,
  -- Count the number of page_view events within that minute
  COUNTIF(event.event_type = 'page_view') AS total_page_views
FROM
  -- Specify the base table
  `dsl_project.web_visits`,
  -- Unnest the events array to access individual event details
  UNNEST(events) AS event
WHERE
  -- Ensure that the timestamp used for grouping is not null
  visit_start_time IS NOT NULL
GROUP BY
  -- Group the counts by the truncated minute
  visit_minute
ORDER BY
  -- Display the results in chronological order
  visit_minute DESC;

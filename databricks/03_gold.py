# Databricks notebook source
# databricks/03_gold.py
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.delay_by_route_hour AS
SELECT mode, route_short_name, route_long_name,
       to_date(event_local_ts)              AS service_day,
       hour(event_local_ts)                 AS hour_of_day,
       date_format(event_local_ts, 'E')     AS weekday,
       count(*)                             AS observations,
       round(avg(delay_s) / 60, 2)          AS avg_delay_min,
       round(percentile_approx(delay_s, 0.9) / 60, 2) AS p90_delay_min,
       round(avg(CASE WHEN delay_s > 300 THEN 1.0 ELSE 0.0 END), 3) AS share_late_5min
FROM workspace.silver.stop_delays
GROUP BY ALL
""")

spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.delay_by_stop AS
SELECT stop_id, stop_name, stop_lat, stop_lon, mode,
       count(*)                    AS observations,
       round(avg(delay_s) / 60, 2) AS avg_delay_min
FROM workspace.silver.stop_delays
GROUP BY ALL
HAVING count(*) >= 50
""")

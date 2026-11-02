# Databricks notebook source
# databricks/02_silver.py
from pyspark.sql import Window
from pyspark.sql import functions as F

STATIC = "/Volumes/workspace/landing/raw/gtfs_static"
SCHEMA = """
  trip STRUCT<trip_id: STRING, route_id: STRING, start_date: STRING>,
  stop_time_update ARRAY<STRUCT<
    stop_sequence: INT, stop_id: STRING,
    arrival: STRUCT<delay: INT, time: STRING>,
    departure: STRUCT<delay: INT, time: STRING>>>
"""

# Tabelle di riferimento
routes = (spark.read.option("header", True).csv(f"{STATIC}/routes.txt")
    .select("route_id", "route_short_name", "route_long_name",
            F.col("route_type").cast("int").alias("route_type"))
    .withColumn("mode", F.expr("""CASE route_type WHEN 0 THEN 'Tram' WHEN 2 THEN 'Treno'
                                  WHEN 3 THEN 'Bus' WHEN 4 THEN 'Traghetto' ELSE 'Altro' END""")))
routes.write.mode("overwrite").saveAsTable("workspace.silver.routes")

stops = (spark.read.option("header", True).csv(f"{STATIC}/stops.txt")
    .select("stop_id", "stop_name",
            F.col("stop_lat").cast("double").alias("stop_lat"),
            F.col("stop_lon").cast("double").alias("stop_lon")))
stops.write.mode("overwrite").saveAsTable("workspace.silver.stops")

# Una riga per corsa, fermata e osservazione
events = (spark.table("workspace.bronze.trip_updates")
    .withColumn("p", F.from_json("payload", SCHEMA))
    .select(F.col("p.trip.trip_id").alias("trip_id"),
            F.col("p.trip.route_id").alias("route_id"),
            F.col("p.trip.start_date").alias("start_date"),
            F.timestamp_seconds("feed_timestamp").alias("feed_ts"),
            F.explode("p.stop_time_update").alias("stu"))
    .select("trip_id", "route_id", "start_date", "feed_ts",
            F.col("stu.stop_sequence").alias("stop_sequence"),
            F.col("stu.stop_id").alias("stop_id"),
            F.coalesce("stu.arrival.delay", "stu.departure.delay").alias("delay_s"),
            F.timestamp_seconds(F.coalesce("stu.arrival.time", "stu.departure.time")
                                .cast("bigint")).alias("event_ts"))
    .where("delay_s IS NOT NULL AND event_ts IS NOT NULL"))

# Deduplica: ultima osservazione per corsa e fermata
latest = Window.partitionBy("start_date", "trip_id", "stop_sequence").orderBy(F.col("feed_ts").desc())
stop_delays = (events
    .withColumn("rn", F.row_number().over(latest)).where("rn = 1").drop("rn")
    .join(spark.table("workspace.silver.routes"), "route_id", "left")
    .join(spark.table("workspace.silver.stops"), "stop_id", "left")
    .withColumn("event_local_ts", F.from_utc_timestamp("event_ts", "Australia/Brisbane")))

(stop_delays.write.mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("workspace.silver.stop_delays"))

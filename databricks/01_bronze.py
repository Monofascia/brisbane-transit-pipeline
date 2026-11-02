# Databricks notebook source
# databricks/01_bronze.py
from pyspark.sql import functions as F

SOURCE = "/Volumes/workspace/landing/raw/trip_updates/"
CHECKPOINT = "/Volumes/workspace/landing/checkpoints/bronze_trip_updates"

(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/schema")
    .load(SOURCE)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable("workspace.bronze.trip_updates")
    .awaitTermination())
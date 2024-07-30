# Databricks notebook source
table = dbutils.widgets.get("table")

df = spark.read.json(f"/Volumes/raw/pokemon/pokemo_raw/{table}/")

(df.distinct()
   .coalesce(1)
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f"bronze.pokemon.{table}"))

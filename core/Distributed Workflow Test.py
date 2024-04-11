# Databricks notebook source
dbutils.widgets.dropdown("start_percentage", "0.4", ["0.0", "0.1", "0.2", "0.3", "0.4"])
dbutils.widgets.dropdown("emd_percentage", "0.4", ["0.0", "0.1", "0.2", "0.3", "0.4"])
startPercentage = dbutils.widgets.get("start_percentage")
endPercentage = dbutils.widgets.get("end_percentage")
print(f"start: {startPercentage}")
print(f"end: {endPercentage}")
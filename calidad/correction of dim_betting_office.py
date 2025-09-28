from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

# Set schema name as a parameter
SCHEMA_NAME = "cabragana"

# Databricks notebook source
# MAGIC %sql
# MAGIC select * from {SCHEMA_NAME}.gold.dim_betting_office

# COMMAND ----------

bookmakers = {
    "B365": {
        "name": "Bet365",
        "url": "https://www.bet365.com/",
        "country": "United Kingdom"
    },
    "BS": {
        "name": "Betsson",
        "url": "https://www.betsson.com/",
        "country": "Sweden"
    },
    "BW": {
        "name": "Bwin",
        "url": "https://www.bwin.com/",
        "country": "Austria"
    },
    "GB": {
        "name": "Gamebookers",
        "url": "https://www.gamebookers.com/",
        "country": "Antigua and Barbuda"
    },
    "IW": {
        "name": "Interwetten",
        "url": "https://www.interwetten.com/",
        "country": "Austria"
    },
    "LB": {
        "name": "Ladbrokes",
        "url": "https://www.ladbrokes.com/",
        "country": "United Kingdom"
    },
    "PS": {
        "name": "Pinnacle Sports",
        "url": "https://www.pinnacle.com/",
        "country": "Curaçao"
    },
    "SJ": {
        "name": "Stan James",
        "url": "https://www.stanjames.com/",
        "country": "United Kingdom"
    },
    "VC": {
        "name": "Victor Chandler (BetVictor)",
        "url": "https://www.betvictor.com/",
        "country": "Gibraltar"
    },
    "WH": {
        "name": "William Hill",
        "url": "https://www.williamhill.com/",
        "country": "United Kingdom"
    }
}

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType
)

data = [
    (3424915662, "WH",  "William Hill",                 "https://www.williamhill.com/",      "United Kingdom"),
    (2506957540, "IW",  "Interwetten",                  "https://www.interwetten.com/",     "Austria"),
    (3923460405, "B365","Bet365",                       "https://www.bet365.com/",         "United Kingdom"),
    (1179038950, "SJ",  "Stan James",                   "https://www.stanjames.com/",      "United Kingdom"),
    (1122891783, "VC",  "Victor Chandler (BetVictor)",  "https://www.betvictor.com/",      "Gibraltar"),
    (1989802799, "BW",  "Bwin",                         "https://www.bwin.com/",           "Austria"),
    (1714678657, "GB",  "Gamebookers",                  "https://www.gamebookers.com/",    "Antigua and Barbuda"),
    (2244424266, "LB",  "Ladbrokes",                    "https://www.ladbrokes.com/",      "United Kingdom"),
    (151015397,  "PS",  "Pinnacle Sports",              "https://www.pinnacle.com/",       "Curaçao"),
    (1911832374, "BS",  "Betsson",                      "https://www.betsson.com/",        "Sweden")
]

schema = StructType([
    StructField("id_betting_office", LongType(),  False),
    StructField("abbr",              StringType(),False),
    StructField("name",              StringType(),False),
    StructField("url",               StringType(),False),
    StructField("country",           StringType(),False)
])

dim_bookmaker = spark.createDataFrame(data, schema)

dim_bookmaker.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.gold.dim_betting_office")

# COMMAND ----------
event_data = [
    (10001, 'opening', 'Odds at the start of the match'),
    (10002, 'closing', 'Odds at the end of the match')
]

event_schema = StructType([
    StructField("id_event", LongType(), False),
    StructField("event_abb", StringType(), False),
    StructField("event_description", StringType(), False)
])

dim_event_df = spark.createDataFrame(event_data, event_schema)
dim_event_df.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.gold.dim_event")

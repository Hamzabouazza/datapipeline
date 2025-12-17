"""
Device Data Generator

Generates synthetic device data using Databricks data generator
and writes it to PostgreSQL database.
"""

from pyspark.sql.types import LongType, IntegerType, StringType
import dbldatagen as dg
from core.DataCore import DataCore
from config import Config


config = Config()


data_core = DataCore(app_name="DeviceDataGenLocal", master=config.spark_master)

if config.jar_path:
    data_core.with_jdbc_jar(config.jar_path)

data_core.with_shuffle_partitions(config.shuffle_partitions)

spark = data_core.spark

device_population = 1000
data_rows = 2000
partitions_requested = 4


country_codes = [
    "CN",
    "US",
    "FR",
    "CA",
    "IN",
    "JM",
    "IE",
    "PK",
    "GB",
    "IL",
    "AU",
    "SG",
    "ES",
    "GE",
    "MX",
    "ET",
    "SA",
    "LB",
    "NL",
]
country_weights = [
    1300,
    365,
    67,
    38,
    1300,
    3,
    7,
    212,
    67,
    9,
    25,
    6,
    47,
    83,
    126,
    109,
    58,
    8,
    17,
]

manufacturers = [
    "Delta corp",
    "Xyzzy Inc.",
    "Lakehouse Ltd",
    "Acme Corp",
    "Embanks Devices",
]

lines = ["delta", "xyzzy", "lakehouse", "gadget", "droid"]

# Generate test data
testDataSpec = (
    dg.DataGenerator(
        spark,
        name="device_data_set",
        rows=data_rows,
        partitions=partitions_requested,
        randomSeedMethod="hash_fieldname",
    )
    .withIdOutput()
    .withColumn(
        "internal_device_id",
        LongType(),
        minValue=0x1000000000000,
        uniqueValues=device_population,
        omit=True,
        baseColumnType="hash",
    )
    .withColumn(
        "device_id", StringType(), format="0x%013x", baseColumn="internal_device_id"
    )
    .withColumn(
        "country",
        StringType(),
        values=country_codes,
        weights=country_weights,
        baseColumn="internal_device_id",
    )
    .withColumn(
        "manufacturer",
        StringType(),
        values=manufacturers,
        baseColumn="internal_device_id",
    )
    .withColumn(
        "line",
        StringType(),
        values=lines,
        baseColumn="manufacturer",
        baseColumnType="hash",
        omit=True,
    )
    .withColumn(
        "model_ser",
        IntegerType(),
        minValue=1,
        maxValue=11,
        baseColumn="device_id",
        baseColumnType="hash",
        omit=True,
    )
    .withColumn(
        "model_line",
        StringType(),
        expr="concat(line, '#', model_ser)",
        baseColumn=["line", "model_ser"],
    )
    .withColumn(
        "event_type",
        StringType(),
        values=[
            "activation",
            "deactivation",
            "plan change",
            "telecoms activity",
            "internet activity",
            "device error",
        ],
        random=True,
    )
    .withColumn(
        "event_ts",
        "timestamp",
        begin="2020-01-01 01:00:00",
        end="2020-12-31 23:59:00",
        interval="1 minute",
        random=True,
    )
)

dfTestData = testDataSpec.build()

dfTestData.show(20)

# Write to PostgreSQL
dfTestData.write.format("jdbc").option("url", config.jdbc_url).option(
    "dbtable", "device_data"
).option("user", config.postgres_user).option(
    "password", config.postgres_password
).option(
    "driver", "org.postgresql.Driver"
).mode(
    "overwrite"
).save()

# Clean up
data_core.stop()

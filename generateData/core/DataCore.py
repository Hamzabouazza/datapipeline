"""
DataCore - Simple Spark Session Manager

A lightweight, practical Spark session manager without over-engineering.
"""

from pyspark.sql import SparkSession
from typing import Optional, Dict


class DataCore:
    """Simple Spark session manager."""

    def __init__(self, app_name: str = "DataPipeline", master: str = "local[*]"):
        """
        Initialize DataCore.

        Args:
            app_name: Spark application name
            master: Spark master URL
        """
        self.app_name = app_name
        self.master = master
        self._spark: Optional[SparkSession] = None
        self._configs: Dict[str, str] = {}

    def add_config(self, key: str, value: str):
        """Add a Spark configuration."""
        self._configs[key] = value
        return self

    def with_jdbc_jar(self, jar_path: str):
        """Configure JDBC JAR file."""
        self._configs["spark.jars"] = jar_path
        self._configs["spark.driver.extraClassPath"] = jar_path
        return self

    def with_shuffle_partitions(self, partitions: int):
        """Set shuffle partitions."""
        self._configs["spark.sql.shuffle.partitions"] = str(partitions)
        return self

    @property
    def spark(self) -> SparkSession:
        """Get or create Spark session."""
        if self._spark is None:
            builder = SparkSession.builder.appName(self.app_name).master(self.master)

            for key, value in self._configs.items():
                builder = builder.config(key, value)

            self._spark = builder.getOrCreate()
            self._spark.sparkContext.setLogLevel("WARN")

        return self._spark

    def stop(self):
        """Stop the Spark session."""
        if self._spark:
            self._spark.stop()
            self._spark = None

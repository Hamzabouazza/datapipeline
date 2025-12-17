"""
Configuration management for data pipeline.
"""

import os
from dotenv import load_dotenv


class Config:
    """Simple configuration class for environment variables."""

    def __init__(self):
        """Load environment variables."""
        load_dotenv()

        # Database
        self.postgres_host = os.getenv("POSTGRES_HOST")
        self.postgres_port = os.getenv("POSTGRES_PORT")
        self.postgres_db = os.getenv("POSTGRES_DB")
        self.postgres_user = os.getenv("POSTGRES_USER")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD")

        # Spark
        self.jar_path = os.getenv("JAR_PATH")
        self.spark_master = os.getenv("SPARK_MASTER", "local[*]")
        self.shuffle_partitions = int(os.getenv("SHUFFLE_PARTITIONS", "4"))

    @property
    def jdbc_url(self):
        """Build JDBC URL."""
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

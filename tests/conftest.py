import pytest
from pyspark.sql import SparkSession
from pathlib import Path

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return (SparkSession.builder
        .master("local[1]")
        .appName("pytest-spark")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate())

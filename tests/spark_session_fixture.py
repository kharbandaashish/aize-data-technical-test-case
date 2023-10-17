from typing import Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    spark = SparkSession.builder.master("local[*]").appName("Tests").getOrCreate()
    yield spark
    spark.stop()

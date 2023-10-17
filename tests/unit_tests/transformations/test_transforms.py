from typing import Callable

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from aizedatatechnicaltestcase.transformations.transforms import (
    cast_columns,
    get_distinct_records,
    get_geohash,
    get_udf,
)


def test_cast_columns(spark_session: SparkSession) -> None:
    df = spark_session.createDataFrame((("1", "a"), ("2", "b")), ["x", "y"])
    columns_to_cast = {"x": "double"}
    actual_df = cast_columns(df, columns_to_cast)

    assert actual_df.schema == StructType([StructField('x', DoubleType(), True), StructField('y', StringType(), True)])
    assert actual_df.collect() == [Row(x=1.0, y="a"), Row(x=2.0, y="b")]


def test_get_distinct_records(spark_session: SparkSession) -> None:
    df = spark_session.createDataFrame((("1", "a"), ("2", "b"), ("2", "b")), ["x", "y"])
    count = get_distinct_records(df)
    assert count == 2


def test_get_geohash() -> None:
    actual_df = get_geohash(1.0, 1.0, 12)
    assert actual_df == "s00twy01mtw0"


def test_get_udf() -> None:
    udf_get_geohash = get_udf(get_geohash, StringType())
    assert isinstance(udf_get_geohash, Callable)  # type: ignore

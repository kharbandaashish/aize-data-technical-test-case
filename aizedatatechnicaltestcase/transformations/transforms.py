from typing import Callable, Dict, Optional, Union

import pygeohash as pgh  # type: ignore
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StringType


def get_geohash(latitude: float, longitude: float, precision: Optional[int] = 12) -> str:
    """
    Function to get the geohash for a given latitude and longitude.
    :param latitude: Latitude value.
    :param longitude: Longitude value.
    :param precision: Precision of the geohash, default is 12.
    :return: Geohash string.
    """
    return pgh.encode(latitude=latitude, longitude=longitude, precision=precision)


def get_udf(func: Callable, return_type: Union[StringType, IntegerType, DoubleType]) -> Callable:
    """
    Function to get UDF for getting the
    :param func: Function to get the UDF for.
    :param return_type: Return type of the UDF.
    :return: UDF for getting the geohash.
    """
    return f.udf(func, return_type)


def get_distinct_records(df: DataFrame) -> int:
    """
    Function to get the number of distinct records in a dataframe.
    :param df: Dataframe to get the number of distinct records from.
    :return: Count of distinct records.
    """
    return df.distinct().count()


def cast_columns(df: DataFrame, columns_to_cast: Dict[str, str]) -> DataFrame:
    """
    Function to cast columns of a dataframe to a given datatype.
    :param df: Dataframe to cast the columns of.
    :param columns_to_cast: Dictionary of columns to cast and the datatype to cast them to.
    :return: Dataframe with the columns casted.
    """
    for k, v in columns_to_cast.items():
        df = df.withColumn(k, df[k].cast(v))
    return df

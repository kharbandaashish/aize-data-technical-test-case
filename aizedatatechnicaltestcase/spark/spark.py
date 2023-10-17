from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """
    Function to get a spark session.
    :param app_name: Name of the spark application.
    :return: An object of SparkSession.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

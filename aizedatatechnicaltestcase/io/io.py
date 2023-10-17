from pyspark.sql import DataFrame, SparkSession

from aizedatatechnicaltestcase.common.constants import CSV_FORMAT, DELIMITER, HEADER, PIPE


def read_csv(spark: SparkSession, file_path: str, header_flag: bool = True, delimiter: str = PIPE) -> DataFrame:
    """
    Function to read a csv file into a :class:`DataFrame`.

    :param spark: A spark session object.
    :param file_path:  Path to the csv file.
    :param header_flag: Flag to indicate if the csv file has a header, default is True.
    :param delimiter: Delimiter used in the csv file, default is "|".
    :return: Spark dataframe.
    """
    return spark.read.format(CSV_FORMAT).option(HEADER, header_flag).option(DELIMITER, delimiter).load(file_path)


def write_csv(df: DataFrame, file_path: str, header_flag: bool = True, delimiter: str = PIPE) -> None:
    """
    Function to write a :class:`DataFrame` to a csv file.

    :param df: DataFrame to write.
    :param file_path: Path to the csv file.
    :param header_flag: Flag to indicate if the csv file has a header, default is True.
    :param delimiter: Delimiter used in the csv file, default is "|".
    :return: None
    """
    df.toPandas().to_csv(file_path, sep=delimiter, header=header_flag, index=False)  # type: ignore

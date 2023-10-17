from unittest import mock

from pyspark.sql import DataFrame, SparkSession

from aizedatatechnicaltestcase.io.io import read_csv, write_csv


def test_read_csv() -> None:
    mock_spark_session = mock.Mock(spec=SparkSession)
    df = read_csv(mock_spark_session, "tests/data/test.csv", header_flag=True, delimiter="|")
    assert len(mock_spark_session.mock_calls) == 4
    assert isinstance(df, mock.Mock)


def test_write_csv() -> None:
    mock_spark_session = mock.Mock(spec=DataFrame)
    write_csv(mock_spark_session, "tests/data/test.csv", header_flag=True, delimiter="|")
    assert len(mock_spark_session.mock_calls) == 2

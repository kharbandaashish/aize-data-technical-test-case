import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from aizedatatechnicaltestcase.run import run
from config.config import INPUT_FILE_NAME, INPUT_FILE_PATH, OUTPUT_FILE_NAME, OUTPUT_FILE_PATH


def test_run(spark_session: SparkSession) -> None:
    run()

    input_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), INPUT_FILE_PATH, INPUT_FILE_NAME
    )
    output_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))),
        OUTPUT_FILE_PATH,
        OUTPUT_FILE_NAME,
    )

    input_df = spark_session.read.format("csv").option("header", "true").option("delimiter", ";").load(input_file)
    output_df = spark_session.read.format("csv").option("header", "true").option("delimiter", ";").load(output_file)

    # Check if output is generated
    assert os.path.exists(output_file)

    # Check if output is has same number of rows as input
    assert output_df.count() == input_df.count()

    # Check if output is has same number of distinct rows as input to confirm the value in unque_prefix column is unique
    # for all coordinates
    assert output_df.distinct().count() == input_df.distinct().count()

    # Check if the schema of output is as expected
    assert output_df.schema == StructType(
        [
            StructField('Longitude', StringType(), True),
            StructField('Latitude', StringType(), True),
            StructField('geohash', StringType(), True),
            StructField('unique_prefix', StringType(), True),
        ]
    )

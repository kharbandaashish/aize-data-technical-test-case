import os

import pyspark.sql.functions as f
from pyspark.sql.types import StringType

from aizedatatechnicaltestcase.common.constants import (
    FLOAT_TYPE,
    GEOHASH_COLUMN_NAME,
    LATITUDE_COLUMN_NAME,
    LONGITUDE_COLUMN_NAME,
    SEMICOLON,
    UNIQUE_PREFIX_COLUMN_NAME,
)
from aizedatatechnicaltestcase.io.io import read_csv, write_csv
from aizedatatechnicaltestcase.spark.spark import get_spark_session
from aizedatatechnicaltestcase.transformations.transforms import (
    cast_columns,
    get_distinct_records,
    get_geohash,
    get_udf,
)
from aizedatatechnicaltestcase.utils.utils import get_logger
from config.config import INPUT_FILE_NAME, INPUT_FILE_PATH, LOGS_DIR, LOGS_FILE_NAME, OUTPUT_FILE_NAME, OUTPUT_FILE_PATH


def run() -> None:
    app_name = os.path.basename(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

    logs_file_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), LOGS_DIR)
    logs_file = os.path.join(logs_file_dir, LOGS_FILE_NAME)

    if not os.path.isdir(logs_file_dir):
        os.makedirs(logs_file_dir)

    logger = get_logger(logs_file, True)

    logger.info("Application - '{}' started".format(app_name))

    logger.info("Creating spark session with app-name - '{}'".format(app_name))

    spark = get_spark_session(app_name)

    input_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))), INPUT_FILE_PATH, INPUT_FILE_NAME
    )

    logger.info("Reading input file - '{}'".format(input_file))

    df = read_csv(spark, input_file, header_flag=True, delimiter=SEMICOLON)

    columns_to_cast = {LATITUDE_COLUMN_NAME: FLOAT_TYPE, LONGITUDE_COLUMN_NAME: FLOAT_TYPE}

    df = cast_columns(df, columns_to_cast)

    distinct_coordinates = get_distinct_records(df)

    get_geohash_udf = get_udf(get_geohash, StringType())

    logger.info("Generating geohash for coordinates for precision 12")

    df = df.withColumn(GEOHASH_COLUMN_NAME, get_geohash_udf(df[LATITUDE_COLUMN_NAME], df[LONGITUDE_COLUMN_NAME]))

    logger.info("Generating unique prefix for geohash")
    for i in range(1, 13):
        logger.info("Generating unique prefix for geohash for precision - {}".format(i))
        df = df.withColumn(
            UNIQUE_PREFIX_COLUMN_NAME, get_geohash_udf(df[LATITUDE_COLUMN_NAME], df[LONGITUDE_COLUMN_NAME], f.lit(i))
        )
        distinct_geohash = df.select(UNIQUE_PREFIX_COLUMN_NAME).distinct().count()
        logger.info(
            "Precision - {}, distinct_coordinates - {}, distinct_geohash - {}".format(
                i, distinct_coordinates, distinct_geohash
            )
        )

        if distinct_coordinates == distinct_geohash:
            logger.info(
                "distinct_coordinates - {} == distinct_geohash - {}".format(distinct_coordinates, distinct_geohash)
            )
            break

    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))), OUTPUT_FILE_PATH, OUTPUT_FILE_NAME
    )

    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, OUTPUT_FILE_NAME)

    logger.info("Writing output file - '{}'".format(output_file))

    write_csv(df, output_file, header_flag=True, delimiter=SEMICOLON)

    logger.info("Application - '{}' completed".format(app_name))

from dataclasses import dataclass
from logging import Logger
from typing import Dict
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, LongType
import logging

S3_BUCKET_PATH = "s3://data-engineer-assignment-alonw/"
INPUT_PATH = os.path.join(S3_BUCKET_PATH, "input/")
OUTPUT_PATH = os.path.join(S3_BUCKET_PATH, "objectives/")


INPUT_SCHEMA = StructType([
    StructField("Date", DateType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", LongType(), True),
    StructField("ticker", StringType(), True)
])

def start_spark_session(local_run: bool=False) -> SparkSession:
    builder = SparkSession.builder.appName("Objectives").config("spark.executor.instances", "1").config("spark.executor.memory", "512m").config("spark.driver.memory", "512m").config("spark.sql.shuffle.partitions", "4")
    builder = builder.master("local[1]") if local_run else builder
    return builder.getOrCreate()


def load_df_from_csv(spark:SparkSession, path:str, schema: StructType, log: logging.Logger) -> DataFrame:
    log.info(f"Reading input path from {path}")
    try:
        df = spark.read.csv(path, header=True, schema=schema)
        log.info(f"Successfully loaded DataFrame from path: {path}")
    except Exception as e:
        log.error(f"Failed to load DataFrame from {path}. Error: {e}")
        raise
    return df

def write_df_to_parquet(df: DataFrame, output_path: str, log: Logger):
    log.info(f"Start writing parquet files to: {output_path}")
    try:
        df.write.parquet(output_path, mode="overwrite")
        log.info(f"Parquet files successfully written to: {output_path}")
    except Exception as e:
        log.error(f"Failed to write parquet files to {output_path}: {e}")
        raise



def get_output_path_to_df_mapping(args, df_obj_1: DataFrame, df_obj_2: DataFrame,
                                  df_obj_3: DataFrame, df_obj_4: DataFrame) -> Dict[str, DataFrame]:
    return {
        args.output_path_obj_1: df_obj_1,
        args.output_path_obj_2: df_obj_2,
        args.output_path_obj_3: df_obj_3,
        args.output_path_obj_4: df_obj_4
    }

def write_multiple_dfs_to_parquet(output_path_to_df: Dict[str, DataFrame], log: logging.Logger):
    for output_path, df in output_path_to_df.items():
            write_df_to_parquet(df, output_path, log)


@dataclass
class ETLArgs:
        input_path: str
        output_path_obj_1: str
        output_path_obj_2: str
        output_path_obj_3: str
        output_path_obj_4: str


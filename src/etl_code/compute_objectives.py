from logging import Logger
from typing import Tuple, Dict

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from .etl_utils import load_df_from_csv, INPUT_SCHEMA, write_multiple_dfs_to_parquet, get_output_path_to_df_mapping, \
    ETLArgs
from .objectives_transformations import compute_returns, compute_avg_daily_returns, compute_highest_worth, compute_most_volatile, \
    compute_top_30_days_return_dates


def execute(input_df: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Execute the transformations required to get the Objectives as Spark DataFrames

    First - compute daily returns, mark it for caching and trigger an action to force the caching.
    Then - daily returns is used as the input df for all objective calculations.


    :param input_df: DataFrame - raw input based on given CSV
    :return: Tuple of 4 Spark DataFrames, one for each Objective
    """
    df_with_daily_returns = compute_returns(input_df).cache()
    ## force caching df_with_daily_returns to prevent its recomputation in downstream transformations
    df_with_daily_returns.count()


    df_daily_average = compute_avg_daily_returns(df_with_daily_returns)
    df_highest_worth = compute_highest_worth(df_with_daily_returns, top_k=1)
    df_most_volatile = compute_most_volatile(df_with_daily_returns, top_k=1)
    df_top_30_days_return_dates = compute_top_30_days_return_dates(df_with_daily_returns, top_k=3)
    return df_daily_average, df_highest_worth, df_most_volatile, df_top_30_days_return_dates, df_with_daily_returns


def main(spark_session: SparkSession , args: ETLArgs, log: Logger):
    """
    Main ETL function
    - load the input CSV from S3 to Spark DataFrame
    - Call Execute to apply transformation on the input to get the Objectives as Spark DataFrames
    - Write each DataFrame to S3 according to its mapped S3 output path


    :param spark_session: The active SparkSession
    :param: args: DataClass that holds input path and multiple output paths
    :paaram: log: Logger
    """
    input_df = load_df_from_csv(spark_session, args.input_path, schema=INPUT_SCHEMA, log=log)
    df_daily_average, df_highest_worth, df_most_volatile, df_top_30_days_return_dates, df_with_daily_returns = execute(input_df)
    obj_output_path_to_df: Dict[str, DataFrame]
    obj_output_path_to_df = get_output_path_to_df_mapping(args, df_daily_average, df_highest_worth, df_most_volatile,df_top_30_days_return_dates)
    write_multiple_dfs_to_parquet(obj_output_path_to_df, log)
    df_with_daily_returns.unpersist()

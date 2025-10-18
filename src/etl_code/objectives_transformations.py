from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as f, Window

def compute_returns(input_df: DataFrame, offset:int=1) -> DataFrame:
    closing_window = Window.partitionBy("ticker").orderBy("Date")
    df_with_previous_closing_df = input_df.withColumn("previous_close", f.lag("close", offset).over(closing_window))
    df_with_returns = df_with_previous_closing_df.withColumn("return", 100.0*(f.col("close")/f.col("previous_close") - 1)).drop("previous_close")
    return df_with_returns

def compute_avg_daily_returns(df_with_daily_returns: DataFrame) -> DataFrame:
    df_daily_average = df_with_daily_returns.groupBy("date").agg(f.avg("return").alias("average_return"))
    return df_daily_average

def compute_highest_worth(input_df: DataFrame, top_k: int) -> DataFrame:
    input_with_worth = input_df.withColumn("worth", f.col("close")*f.col("volume"))
    ticker_avg_worth = input_with_worth.groupBy("ticker").agg(f.avg("worth").alias("value"))
    return ticker_avg_worth.orderBy(f.col("value").desc()).limit(top_k)

def compute_most_volatile(df_with_daily_returns: DataFrame, top_k: int) -> DataFrame:
    daily_std_df = df_with_daily_returns.withColumn("year", f.year("Date")).groupBy("ticker","year").agg(f.stddev(f.col("return")).alias("daily_standard_deviation"),(f.count("*").alias("trading_days")))
    ann_std_df = daily_std_df.withColumn("standard_deviation", f.col("daily_standard_deviation") * f.sqrt(f.col("trading_days")))
    return ann_std_df.orderBy(f.col("standard_deviation").desc()).limit(top_k).select("ticker", "standard_deviation")

def compute_top_30_days_return_dates(input_df: DataFrame, top_k: int) -> DataFrame:
    df_with_30_days_return = compute_returns(input_df, 30)
    return df_with_30_days_return.orderBy(f.col("return").desc()).limit(top_k).select("ticker", "date")

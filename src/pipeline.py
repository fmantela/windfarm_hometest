from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from .config import PipelineConfig


INPUT_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("turbine_id", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("power_output", DoubleType(), True),
])


def read_raw_data(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Read all matching daily turbine CSV files using the configured input path and schema."""
    path = f"{config.input_path.rstrip('/')}/{config.input_pattern}"
    return (
        spark.read
        .option("header", True)
        .schema(INPUT_SCHEMA)
        .csv(path)
    )


def prepare_data(df: DataFrame) -> DataFrame:
    """Parse timestamps, convert invalid physical measurements to nulls, and remove duplicate readings."""
    parsed = (
        df.withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn(
            "wind_speed",
            F.when(F.col("wind_speed") >= 0, F.col("wind_speed"))
        )
        .withColumn(
            "wind_direction",
            F.when(
                (F.col("wind_direction") >= 0) & (F.col("wind_direction") <= 360),
                F.col("wind_direction")
            )
        )
        .withColumn(
            "power_output",
            F.when(F.col("power_output") >= 0, F.col("power_output"))
        )
        .dropDuplicates(["timestamp", "turbine_id"])
    )
    return parsed.filter(F.col("timestamp").isNotNull() & F.col("turbine_id").isNotNull())


def impute_missing_values(df: DataFrame) -> DataFrame:
    """Fill missing numeric measurements with the turbine median, falling back to the global median."""
    turbine_window = Window.partitionBy("turbine_id")
    global_stats = df.agg(
        F.expr("percentile_approx(wind_speed, 0.5)").alias("global_wind_speed_median"),
        F.expr("percentile_approx(wind_direction, 0.5)").alias("global_wind_direction_median"),
        F.expr("percentile_approx(power_output, 0.5)").alias("global_power_output_median"),
    ).collect()[0]

    # Calculate per-turbine medians with Spark window functions so the imputation remains distributed.
    result = (
        df.withColumn("wind_speed_median", F.expr("percentile_approx(wind_speed, 0.5)").over(turbine_window))
        .withColumn("wind_direction_median", F.expr("percentile_approx(wind_direction, 0.5)").over(turbine_window))
        .withColumn("power_output_median", F.expr("percentile_approx(power_output, 0.5)").over(turbine_window))
        .withColumn(
            "wind_speed",
            F.coalesce(F.col("wind_speed"), F.col("wind_speed_median"), F.lit(global_stats["global_wind_speed_median"]))
        )
        .withColumn(
            "wind_direction",
            F.coalesce(F.col("wind_direction"), F.col("wind_direction_median"), F.lit(global_stats["global_wind_direction_median"]))
        )
        .withColumn(
            "power_output",
            F.coalesce(F.col("power_output"), F.col("power_output_median"), F.lit(global_stats["global_power_output_median"]))
        )
        .drop("wind_speed_median", "wind_direction_median", "power_output_median")
    )
    return result


def replace_power_outliers(df: DataFrame, iqr_multiplier: float) -> DataFrame:
    """Identify per-turbine power outliers with the IQR rule and replace them with the turbine median."""
    stats = df.groupBy("turbine_id").agg(
        F.expr("percentile_approx(power_output, 0.25)").alias("q1"),
        F.expr("percentile_approx(power_output, 0.5)").alias("median_power"),
        F.expr("percentile_approx(power_output, 0.75)").alias("q3"),
    )
    result = df.join(stats, "turbine_id", "left")
    iqr = F.col("q3") - F.col("q1")
    is_outlier = (F.col("power_output") < F.col("q1") - iqr_multiplier * iqr) | (
        F.col("power_output") > F.col("q3") + iqr_multiplier * iqr
    )
    return (
        result.withColumn("power_output_raw", F.col("power_output"))
        .withColumn("power_output", F.when(is_outlier, F.col("median_power")).otherwise(F.col("power_output")))
        .withColumn("power_outlier_replaced", is_outlier)
        .drop("q1", "q3", "median_power")
    )


def clean_data(df: DataFrame, iqr_multiplier: float) -> DataFrame:
    """Run the complete cleaning sequence: validation, deduplication, missing-value imputation, and outlier handling."""
    return replace_power_outliers(impute_missing_values(prepare_data(df)), iqr_multiplier)


def calculate_daily_summary(df: DataFrame) -> DataFrame:
    """Calculate daily minimum, maximum, average, and observation count for each turbine."""
    return (
        df.withColumn("date", F.to_date("timestamp"))
        .groupBy("date", "turbine_id")
        .agg(
            F.min("power_output").alias("min_power_output"),
            F.max("power_output").alias("max_power_output"),
            F.avg("power_output").alias("avg_power_output"),
            F.count("power_output").alias("observation_count"),
        )
    )


def identify_anomalies(summary_df: DataFrame, threshold: float) -> DataFrame:
    """Flag turbine-day averages that are more than the configured standard deviations from the fleet mean."""
    fleet = summary_df.groupBy("date").agg(
        F.avg("avg_power_output").alias("fleet_mean_power"),
        F.stddev_pop("avg_power_output").alias("fleet_stddev_power"),
    )
    return (
        summary_df.join(fleet, "date")
        .withColumn(
            "deviation_from_mean",
            F.col("avg_power_output") - F.col("fleet_mean_power")
        )
        .withColumn(
            "z_score",
            F.when(F.col("fleet_stddev_power") > 0,
                   F.col("deviation_from_mean") / F.col("fleet_stddev_power"))
        )
        .withColumn(
            "is_anomaly",
            F.when(
                F.col("fleet_stddev_power") > 0,
                F.abs(F.col("deviation_from_mean")) > threshold * F.col("fleet_stddev_power")
            ).otherwise(F.lit(False))
        )
    )


def write_output(df: DataFrame, path: str, output_format: str, mode: str = "overwrite") -> None:
    """Persist a DataFrame to the configured storage format, supporting Delta or Parquet."""
    writer = df.write.mode(mode)
    if output_format == "delta":
        writer.format("delta").save(path)
    elif output_format == "parquet":
        writer.parquet(path)
    else:
        raise ValueError("output_format must be 'delta' or 'parquet'")

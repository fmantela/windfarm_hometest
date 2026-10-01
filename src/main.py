import argparse

from pyspark.sql import SparkSession

from .config import PipelineConfig
from .pipeline import (
    calculate_daily_summary,
    clean_data,
    identify_anomalies,
    read_raw_data,
    write_output,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Colibri Home Test PySpark pipeline")
    parser.add_argument("--input-path", required=True, help="Directory containing daily turbine CSV files")
    parser.add_argument("--output-path", required=True, help="Directory for processed outputs")
    parser.add_argument("--input-pattern", default="data_group_*.csv")
    parser.add_argument("--output-format", choices=["delta", "parquet"], default="delta")
    parser.add_argument("--anomaly-threshold", type=float, default=2.0)
    parser.add_argument("--iqr-multiplier", type=float, default=1.5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = PipelineConfig(
        input_path=args.input_path,
        output_path=args.output_path,
        input_pattern=args.input_pattern,
        output_format=args.output_format,
        anomaly_stddev_threshold=args.anomaly_threshold,
        iqr_multiplier=args.iqr_multiplier,
    )

    spark = SparkSession.builder.appName(config.app_name).getOrCreate()
    try:
        raw = read_raw_data(spark, config)
        cleaned = clean_data(raw, config.iqr_multiplier).cache()
        summary = calculate_daily_summary(cleaned).cache()
        anomalies = identify_anomalies(summary, config.anomaly_stddev_threshold)

        base = config.output_path.rstrip("/")
        write_output(cleaned, f"{base}/cleaned_data", config.output_format)
        write_output(summary, f"{base}/turbine_daily_summary", config.output_format)
        write_output(anomalies, f"{base}/anomalies", config.output_format)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

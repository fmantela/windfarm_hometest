from datetime import datetime

from pyspark.sql import SparkSession

from src.pipeline import calculate_daily_summary, identify_anomalies, impute_missing_values


spark = SparkSession.builder.master("local[2]").appName("ColibriHomeTestTests").getOrCreate()


def test_missing_values_are_imputed():
    df = spark.createDataFrame([
        (datetime(2022, 3, 1, 0), 1, 10.0, 100, 5.0),
        (datetime(2022, 3, 1, 1), 1, None, None, None),
        (datetime(2022, 3, 1, 2), 1, 12.0, 120, 7.0),
    ], ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"])

    result = impute_missing_values(df)
    assert result.filter("wind_speed IS NULL OR wind_direction IS NULL OR power_output IS NULL").count() == 0


def test_daily_summary():
    df = spark.createDataFrame([
        (datetime(2022, 3, 1, 0), 1, 5.0),
        (datetime(2022, 3, 1, 1), 1, 7.0),
        (datetime(2022, 3, 1, 2), 1, 9.0),
    ], ["timestamp", "turbine_id", "power_output"])

    row = calculate_daily_summary(df).first()
    assert row.min_power_output == 5.0
    assert row.max_power_output == 9.0
    assert row.avg_power_output == 7.0
    assert row.observation_count == 3


def test_anomaly_detection():
    df = spark.createDataFrame([
        (datetime(2022, 3, 1), 1, 10.0),
        (datetime(2022, 3, 1), 2, 10.0),
        (datetime(2022, 3, 1), 3, 10.0),
        (datetime(2022, 3, 1), 4, 10.0),
        (datetime(2022, 3, 1), 5, 100.0),
    ], ["date", "turbine_id", "avg_power_output"])

    result = identify_anomalies(df, 2.0)
    anomalous = result.filter("is_anomaly").select("turbine_id").collect()
    assert [r.turbine_id for r in anomalous] == [5]


if __name__ == "__main__":
    spark.stop()

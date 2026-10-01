# Colibri Home Test — PySpark Proof of Concept

## Overview

This project implements the requested renewable-energy data pipeline in Python and PySpark.
The pipeline reads daily CSV files for groups of five turbines, cleans the measurements, calculates per-turbine daily statistics, identifies anomalies, and persists the results.

The assessment states that CSVs are appended daily, that each turbine always belongs to the same group file, and that sensor malfunctions can cause missing entries. It asks for scalable/testable Python + PySpark code and preferably Delta-table storage.

## Configuration

There is deliberately **no YAML configuration file**. Input/output locations and processing options are supplied through Python `argparse` CLI arguments.

Example:

```bash
spark-submit src/main.py \
  --input-path ./data \
  --output-path ./output
```

Available options:

```text
--input-path          Required input directory
--output-path         Required output directory
--input-pattern       Default: data_group_*.csv
--output-format       delta or parquet; default: delta
--anomaly-threshold   Default: 2.0 standard deviations
--iqr-multiplier      Default: 1.5
```

This keeps configuration separate from pipeline logic without introducing an additional configuration dependency.

## Input

Place files such as these under the input directory:

```text
data_group_1.csv
data_group_2.csv
data_group_3.csv
```

The supplied PoC data currently includes groups 2 and 3. The wildcard means group 1 and future group files are picked up without code changes.

Expected columns:

```text
timestamp,turbine_id,wind_speed,wind_direction,power_output
```

## Processing

1. Read all matching CSVs with an explicit Spark schema.
2. Parse timestamps and reject records without a valid timestamp/turbine ID.
3. Normalise invalid physical values to null:
   - wind speed < 0
   - wind direction outside 0–360 degrees
   - negative power output
4. Remove duplicate `(timestamp, turbine_id)` readings.
5. Impute missing numeric values using each turbine's median, with a global median fallback.
6. Detect power-output outliers using a configurable IQR rule and replace them with the turbine median.
7. Calculate daily per-turbine minimum, maximum, average and observation count.
8. Calculate the fleet daily mean and population standard deviation of turbine daily averages.
9. Flag a turbine/day as anomalous when its average is more than 2 standard deviations from the fleet mean.
10. Write cleaned data, daily summaries and anomalies to Delta tables by default.

## Outputs

```text
output/
├── cleaned_data/
├── turbine_daily_summary/
└── anomalies/
```

Delta is the default. For an environment without Delta support:

```bash
spark-submit src/main.py \
  --input-path ./data \
  --output-path ./output \
  --output-format parquet
```

## Testing

Tests cover:

- missing-value imputation
- daily min/max/average aggregation
- two-standard-deviation anomaly detection

Run with:

```bash
pytest -q
```

## Assumptions

- A reading is uniquely identified by `timestamp + turbine_id`.
- A daily summary means calendar-day aggregation of the available readings.
- The assessment's anomaly definition is applied to turbine daily average power output relative to the fleet for that day.
- A 2-standard-deviation threshold is used by default.
- IQR is used as a practical PoC rule for identifying power outliers; the multiplier is configurable.
- Missing sensor values are imputed rather than dropping otherwise useful turbine observations.
- The sample is hourly, but the implementation does not hard-code an hourly frequency.

## Productionisation discussion

For production, the same processing functions could run as a scheduled Spark job against object storage. Delta would provide transactional writes and support incremental processing. A production implementation would additionally add data-quality metrics, structured logging, monitoring/alerting, schema evolution controls, partitioning strategy, idempotent/incremental processing, and orchestration.

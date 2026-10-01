from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    input_path: str
    output_path: str
    input_pattern: str = "data_group_*.csv"
    output_format: str = "delta"
    anomaly_stddev_threshold: float = 2.0
    iqr_multiplier: float = 1.5
    app_name: str = "ColibriHomeTestPipeline"

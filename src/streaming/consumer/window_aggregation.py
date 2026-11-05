"""Tumbling window aggregation for P2-B-B3-02."""

from pyspark.sql import DataFrame, functions as F

WINDOW_SIZES = {
    "1m": "1 minute",
    "5m": "5 minutes",
}


def build_window_counts(
    parsed_df: DataFrame,
    window_size: str,
    watermark_delay: str,
) -> DataFrame:
    """Count valid events by event type and tumbling window."""

    if window_size not in WINDOW_SIZES:
        raise ValueError(
            "window_size must be '1m' or '5m'"
        )

    if (
        not isinstance(watermark_delay, str)
        or not watermark_delay.strip()
    ):
        raise ValueError(
            "watermark_delay must be specified"
        )

    required = {"event_time", "event_type", "is_valid"}
    missing = required - set(parsed_df.columns)

    if missing:
        raise ValueError(
            "Missing columns: " + ", ".join(sorted(missing))
        )

    valid_events = parsed_df.filter(
        (F.col("is_valid") == F.lit(True))
        & F.col("event_time").isNotNull()
        & F.col("event_type").isNotNull()
    )

    return (
        valid_events
        .withWatermark("event_time", watermark_delay)
        .groupBy(
            F.window(
                "event_time",
                WINDOW_SIZES[window_size]
            ),
            F.col("event_type")
        )
        .agg(F.count("*").alias("event_count"))
    )

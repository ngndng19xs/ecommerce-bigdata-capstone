"""CLI launcher for P2-B-B3-02 window aggregation."""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.common.spark_session import get_spark, load_config
from src.streaming.consumer.event_consumer import parse_kafka_stream
from src.streaming.consumer.window_aggregation import build_window_counts


def main():
    parser = argparse.ArgumentParser(
        description="Spark Kafka tumbling windows with watermark"
    )

    parser.add_argument("--config", default="configs/config.example.yaml")
    parser.add_argument("--topic", default="ecommerce_events")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--window-size", choices=["1m", "5m"], required=True)
    parser.add_argument("--watermark-delay", required=True)
    parser.add_argument("--sink", choices=["console", "json"], default="console")
    parser.add_argument("--output")
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--starting-offsets",
                        choices=["earliest", "latest"], default="latest")
    parser.add_argument("--max-offsets-per-trigger", type=int)
    parser.add_argument("--trigger-interval", default="2 seconds")
    parser.add_argument("--driver-memory", default="1g")
    parser.add_argument("--until-caught-up", action="store_true")

    args = parser.parse_args()

    if args.sink == "json" and not args.output:
        parser.error("--output is required when --sink=json")

    if not args.watermark_delay.strip():
        parser.error("--watermark-delay cannot be empty")

    if (args.max_offsets_per_trigger is not None
            and args.max_offsets_per_trigger < 1):
        parser.error("--max-offsets-per-trigger must be positive")

    cfg = load_config(args.config)
    cfg.setdefault("ml", {})
    cfg["ml"]["driver_memory"] = args.driver_memory
    cfg["ml"]["shuffle_partitions"] = 2

    spark = get_spark("P2_B3_02_Windows", cfg=cfg)
    query = None

    try:
        reader = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
        )

        if args.max_offsets_per_trigger is not None:
            reader = reader.option(
                "maxOffsetsPerTrigger", args.max_offsets_per_trigger
            )

        raw = reader.load()
        parsed = parse_kafka_stream(raw)

        result = build_window_counts(
            parsed,
            window_size=args.window_size,
            watermark_delay=args.watermark_delay,
        )

        mode = "append" if args.sink == "json" else "update"

        writer = (
            result.writeStream
            .format(args.sink)
            .outputMode(mode)
            .option("checkpointLocation", args.checkpoint)
        )

        if args.sink == "json":
            writer = writer.option("path", args.output)
        else:
            writer = writer.option("truncate", "false")

        print("TOPIC:", args.topic, flush=True)
        print("WINDOW_SIZE:", args.window_size, flush=True)
        print("WATERMARK_DELAY:", args.watermark_delay, flush=True)
        print("SINK:", args.sink, flush=True)
        print("CHECKPOINT:", args.checkpoint, flush=True)

        query = writer.trigger(
            processingTime=args.trigger_interval
        ).start()

        if args.until_caught_up:
            query.processAllAvailable()

            for p in query.recentProgress:
                dropped = sum(
                    int(s.get("numRowsDroppedByWatermark", 0))
                    for s in p.get("stateOperators", [])
                )
                print(
                    "BATCH:", p.get("batchId"),
                    "INPUT:", p.get("numInputRows"),
                    "DROPPED:", dropped,
                    flush=True
                )

            print("RESULT: LAUNCHER_CATCHUP_FINISHED")
        else:
            query.awaitTermination()

    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

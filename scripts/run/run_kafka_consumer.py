"""CLI entrypoint for Kafka Spark Structured Streaming Consumer (Task S2 / Version kafka-v1.0)."""

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.streaming.consumer.event_consumer import (  # noqa: E402
    PRODUCTION_TOPIC,
    create_kafka_stream,
    order_events_microbatch,
    parse_kafka_stream,
)
from src.common.spark_session import get_spark, load_config  # noqa: E402


def main() -> int:
    """Entrypoint to launch Spark Structured Streaming Consumer from CLI."""
    parser = argparse.ArgumentParser(description="Run Kafka Event Consumer (Task S2)")
    parser.add_argument("--config", default="configs/config.example.yaml", help="Config file path")
    parser.add_argument("--topic", default=PRODUCTION_TOPIC, help="Kafka topic name")
    parser.add_argument("--bootstrap-servers", default=None, help="Kafka bootstrap servers")
    args = parser.parse_args()

    cfg = load_config(args.config)
    bootstrap = args.bootstrap_servers or ",".join(
        cfg.get("kafka", {}).get("bootstrap_servers", ["localhost:9092"])
    )
    topic = args.topic or cfg.get("kafka", {}).get("topics", {}).get(
        "clickstream_raw", PRODUCTION_TOPIC
    )

    spark = get_spark("KafkaEventConsumer", cfg=cfg)
    raw_stream = create_kafka_stream(spark, bootstrap_servers=bootstrap, topic=topic)
    parsed_stream = parse_kafka_stream(raw_stream)
    ordered_stream = order_events_microbatch(parsed_stream)

    query = (
        ordered_stream.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

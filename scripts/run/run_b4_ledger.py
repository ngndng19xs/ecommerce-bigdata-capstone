"""Run the B4 local sample ledger with its own checkpoint."""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# pyrefly: ignore[missing-import]
from pyspark.sql import SparkSession
from src.streaming.consumer.event_consumer import parse_kafka_stream
from src.streaming.consumer.ledger_sink import commit_local_ledger_batch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=100)
    parser.add_argument("--until-caught-up", action="store_true")
    args = parser.parse_args()

    if args.max_offsets_per_trigger < 1:
        parser.error("--max-offsets-per-trigger must be positive")

    ledger = Path(args.ledger).expanduser().resolve()
    checkpoint = Path(args.checkpoint).expanduser().resolve()

    if (ledger == checkpoint
            or ledger in checkpoint.parents
            or checkpoint in ledger.parents):
        parser.error("Ledger and checkpoint must be separate directories")

    spark = (
        SparkSession.builder
        .appName("P3_B4_LocalLedger")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    query = None

    try:
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
            .load()
        )
        parsed = parse_kafka_stream(raw)

        def save_batch(batch_df, batch_id):
            inserted = commit_local_ledger_batch(
                batch_df, batch_id, str(ledger)
            )
            print(
                "B4_BATCH={} INSERTED_ROWS={}".format(batch_id, inserted),
                flush=True,
            )

        query = (
            parsed.writeStream
            .foreachBatch(save_batch)
            .option("checkpointLocation", checkpoint.as_uri())
            .trigger(processingTime="5 seconds")
            .start()
        )

        print("TOPIC:", args.topic, flush=True)
        print("LEDGER:", ledger, flush=True)
        print("CHECKPOINT:", checkpoint, flush=True)

        if args.until_caught_up:
            query.processAllAvailable()
            print("B4_CATCHUP_FINISHED", flush=True)
        else:
            query.awaitTermination()
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

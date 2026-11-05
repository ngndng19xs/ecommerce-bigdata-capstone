"""Publish complete local funnel snapshots for B4 sample runs."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F
from src.streaming.consumer.session_funnel import build_session_funnel


def publish_funnel_snapshot(spark, ledger_dir, snapshot_dir, batch_id):
    """Recompute from a fixed list of committed ledger directories."""
    ledger_root = Path(ledger_dir).resolve()
    output_root = Path(snapshot_dir).resolve()

    if (ledger_root == output_root
            or ledger_root in output_root.parents
            or output_root in ledger_root.parents):
        raise ValueError("Ledger and snapshots must use separate directories")

    commits_dir = ledger_root / "commits"
    commits = (
        sorted(p for p in commits_dir.iterdir() if p.is_dir())
        if commits_dir.exists() else []
    )
    if not commits:
        return None

    for commit in commits:
        if not (commit / "_SUCCESS").is_file():
            raise RuntimeError("Incomplete ledger commit: {}".format(commit))

    ledger = spark.read.parquet(*[p.as_uri() for p in commits])
    keys = ["kafka_topic", "kafka_partition", "kafka_offset"]

    total = ledger.count()
    unique = ledger.select(*keys).distinct().count()
    if total != unique:
        raise ValueError("Duplicate Kafka identities in committed ledger")

    valid = ledger.filter(F.col("is_valid") == True).count()
    invalid = ledger.filter(F.col("is_valid") == False).count()
    unknown = ledger.filter(F.col("is_valid").isNull()).count()
    valid_null = ledger.filter(
        (F.col("is_valid") == True) & F.col("user_session").isNull()
    ).count()

    versions = [
        r["schema_version"]
        for r in ledger.select("schema_version").distinct().collect()
    ]
    topics = [
        r["kafka_topic"]
        for r in ledger.select("kafka_topic").distinct().collect()
    ]

    funnel = build_session_funnel(ledger)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    name = "{}-batch-{}-{}".format(stamp, batch_id, uuid4().hex)

    staged = output_root / "_staging" / name
    published = output_root / "commits" / name
    staged.mkdir(parents=True, exist_ok=False)
    published.parent.mkdir(parents=True, exist_ok=True)

    funnel.write.mode("errorifexists").parquet(
        (staged / "sessions").as_uri()
    )

    saved = spark.read.parquet((staged / "sessions").as_uri())
    counts = saved.agg(
        F.count("*").alias("funnel_sessions"),
        F.sum(F.col("has_view").cast("long")).alias("view_sessions"),
        F.sum(F.col("has_cart").cast("long")).alias("cart_sessions"),
        F.sum(F.col("has_purchase").cast("long")).alias("purchase_sessions"),
    ).first().asDict()

    metadata = {
        "scope": "B4 local sample",
        "created_at_utc": stamp,
        "batch_id": int(batch_id),
        "source_commits": [p.name for p in commits],
        "source_topics": topics,
        "total_messages": total,
        "unique_offsets": unique,
        "duplicate_offsets": total - unique,
        "valid_messages": valid,
        "invalid_messages": invalid,
        "unknown_validity_messages": unknown,
        "valid_null_session_messages": valid_null,
        "observed_schema_versions": versions,
        "timezone": "UTC",
    }
    metadata.update({k: int(v or 0) for k, v in counts.items()})

    with (staged / "metadata.json").open("x", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    if not (staged / "sessions" / "_SUCCESS").is_file():
        raise RuntimeError("Snapshot Parquet has no success marker")

    os.rename(str(staged), str(published))
    return str(published)

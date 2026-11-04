"""Prepare Kafka micro-batches for the B4 event ledger."""

from pyspark.sql import functions as F

LEDGER_KEY = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
]


def prepare_ledger_batch(batch_df):
    """Validate message identity and remove exact duplicate rows.

    Call this function inside foreachBatch, on a batch DataFrame.
    Keep invalid payloads and null sessions for subsequent accounting.
    """
    required = LEDGER_KEY + ["kafka_key", "kafka_value"]

    missing = [name for name in required if name not in batch_df.columns]
    if missing:
        raise ValueError("Missing ledger columns: {}".format(missing))

    invalid_key = (
        F.col("kafka_topic").isNull()
        | (F.length(F.trim(F.col("kafka_topic"))) == 0)
        | F.col("kafka_partition").isNull()
        | (F.col("kafka_partition") < 0)
        | F.col("kafka_offset").isNull()
        | (F.col("kafka_offset") < 0)
    )

    if batch_df.filter(invalid_key).limit(1).count():
        raise ValueError("Null or invalid Kafka ledger identity")

    # One Kafka identity must always represent the same key and value.
    variants = batch_df.select(
        *(LEDGER_KEY + ["kafka_key", "kafka_value"])
    ).distinct()

    conflicts = (
        variants.groupBy(*LEDGER_KEY)
        .count()
        .filter(F.col("count") > 1)
    )

    if conflicts.limit(1).count():
        raise ValueError("Conflicting payloads for the same Kafka offset")

    return batch_df.dropDuplicates(LEDGER_KEY)


def select_new_ledger_rows(batch_df, committed_df):
    """Return unseen Kafka identities; reject conflicting old messages.

    committed_df must contain only successfully committed ledger rows.
    Durable writes and concurrency control are handled by the sink.
    """
    incoming = prepare_ledger_batch(batch_df)
    committed = prepare_ledger_batch(committed_df)

    identity_payload = LEDGER_KEY + ["kafka_key", "kafka_value"]

    combined = incoming.select(*identity_payload).unionByName(
        committed.select(*identity_payload)
    )

    # Also detect conflicts between incoming and committed messages.
    prepare_ledger_batch(combined)

    return incoming.join(
        committed.select(*LEDGER_KEY),
        on=LEDGER_KEY,
        how="left_anti",
    )

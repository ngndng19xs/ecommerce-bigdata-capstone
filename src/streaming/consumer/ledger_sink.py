"""Local Parquet ledger sink for B4 sample runs on VM B.

Use a dedicated local directory outside the shared repo.
All writers must use this sink and its lock.
"""

import fcntl
import os
from pathlib import Path
from uuid import uuid4

from pyspark import StorageLevel

from src.streaming.consumer.event_ledger import (
    prepare_ledger_batch,
    select_new_ledger_rows,
)


def _local_root(ledger_dir):
    root = Path(ledger_dir).expanduser()

    if not root.is_absolute():
        raise ValueError("Ledger directory must be an absolute local path")

    root = root.resolve()

    if str(root).startswith(("/media/", "/mnt/")):
        raise ValueError("Use VM local disk, not a shared-folder path")

    return root


def read_local_ledger(spark, ledger_dir, schema):
    """Read published commits only; ignore incomplete staging writes."""
    root = _local_root(ledger_dir)
    commits_dir = root / "commits"

    paths = (
        sorted(p for p in commits_dir.iterdir() if p.is_dir())
        if commits_dir.exists()
        else []
    )

    if not paths:
        return spark.createDataFrame([], schema)

    for path in paths:
        if not (path / "_SUCCESS").is_file():
            raise RuntimeError("Incomplete published commit: {}".format(path))

    return spark.read.schema(schema).parquet(
        *[path.as_uri() for path in paths]
    )


def commit_local_ledger_batch(batch_df, batch_id, ledger_dir):
    """Commit unseen identities and return the number of inserted rows.

    A retry rechecks stored identities instead of trusting batch_id alone.
    Staging and commits must remain on the same local filesystem.
    """
    if not isinstance(batch_id, int) or batch_id < 0:
        raise ValueError("batch_id must be a non-negative integer")

    root = _local_root(ledger_dir)
    root.mkdir(parents=True, exist_ok=True)
    staging_dir = root / "_staging"
    commits_dir = root / "commits"
    staging_dir.mkdir(exist_ok=True)
    commits_dir.mkdir(exist_ok=True)

    spark = batch_df.sql_ctx.sparkSession

    with (root / ".writer.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)

        incoming = prepare_ledger_batch(batch_df)
        committed = read_local_ledger(spark, root, incoming.schema)

        new_rows = select_new_ledger_rows(incoming, committed)
        new_rows.persist(StorageLevel.DISK_ONLY)

        try:
            inserted = new_rows.count()

            if inserted == 0:
                return 0

            name = "batch-{}-{}".format(batch_id, uuid4().hex)
            staged = staging_dir / name
            published = commits_dir / name

            new_rows.write.mode("errorifexists").parquet(staged.as_uri())

            if not (staged / "_SUCCESS").is_file():
                raise RuntimeError("Parquet write has no success marker")

            # Publish the complete directory while holding the writer lock.
            os.rename(str(staged), str(published))
            return inserted
        finally:
            new_rows.unpersist()

"""Time-based dataset splitting module for clickstream ML pipelines.

Splits sessions chronologically based on session_start_date into train, validation,
and test sets. This temporal split prevents lookahead bias and closely simulates
real-world deployment where models trained on past data are evaluated on future dates.
"""

from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, functions as F
from src.common.logger import get_logger

logger = get_logger("split")


def split_by_time(
    df: DataFrame,
    ratios: Optional[Dict[str, float]] = None,
) -> Tuple[DataFrame, DataFrame, DataFrame, Dict[str, object]]:
    """Split session dataset into train, validation, and test sets by distinct session_start_date.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing at least 'session_start_date' and 'label' columns.
    ratios : Optional[Dict[str, float]]
        Dictionary specifying split ratios for 'train', 'val', and 'test'.
        Default is {'train': 0.70, 'val': 0.15, 'test': 0.15}.

    Returns
    -------
    Tuple[DataFrame, DataFrame, DataFrame, Dict[str, object]]
        train_df, val_df, test_df, and split_metadata dict.

    Raises
    ------
    ValueError
        If ratios do not sum to ~1.0, or if any partition is empty,
        or if train partition contains zero positive labels.
    """
    if ratios is None:
        ratios = {"train": 0.70, "val": 0.15, "test": 0.15}

    train_ratio = float(ratios.get("train", 0.70))
    val_ratio = float(ratios.get("val", 0.15))
    test_ratio = float(ratios.get("test", 0.15))

    total_ratio = train_ratio + val_ratio + test_ratio
    if abs(total_ratio - 1.0) > 1e-4:
        raise ValueError(
            f"Split ratios must sum to 1.0, but got {total_ratio:.4f} ({ratios})"
        )

    # Collect ordered distinct session_start_date
    distinct_dates_rows = (
        df.select("session_start_date")
        .distinct()
        .sort("session_start_date")
        .collect()
    )
    distinct_dates = [row[0] for row in distinct_dates_rows]
    total_dates = len(distinct_dates)

    if total_dates < 3:
        raise ValueError(
            f"Dataset has only {total_dates} distinct dates, requiring at least 3 for train/val/test split."
        )

    # Calculate index cutoffs guaranteeing at least 1 date per partition
    n_train = max(1, int(total_dates * train_ratio))
    n_val = max(1, round(total_dates * val_ratio))
    if n_train + n_val >= total_dates:
        n_train = max(1, total_dates - 2)
        n_val = 1

    train_dates = distinct_dates[:n_train]
    val_dates = distinct_dates[n_train : n_train + n_val]
    test_dates = distinct_dates[n_train + n_val :]

    if not train_dates or not val_dates or not test_dates:
        raise ValueError(
            f"Failed to produce non-empty date splits: train={len(train_dates)}, val={len(val_dates)}, test={len(test_dates)}"
        )

    train_df = df.filter(F.col("session_start_date").isin(train_dates))
    val_df = df.filter(F.col("session_start_date").isin(val_dates))
    test_df = df.filter(F.col("session_start_date").isin(test_dates))

    # Validate non-empty partitions and label existence
    train_count = train_df.count()
    val_count = val_df.count()
    test_count = test_df.count()

    if train_count == 0:
        raise ValueError("Train dataset is empty.")
    if val_count == 0:
        raise ValueError("Validation dataset is empty.")
    if test_count == 0:
        raise ValueError("Test dataset is empty.")

    train_pos = train_df.filter(F.col("label") == 1).count()
    if train_pos == 0:
        raise ValueError("Train dataset does not contain any positive labels (label=1).")

    val_pos = val_df.filter(F.col("label") == 1).count()
    test_pos = test_df.filter(F.col("label") == 1).count()

    split_metadata = {
        "train": {
            "dates": [str(d) for d in train_dates],
            "start_date": str(train_dates[0]),
            "end_date": str(train_dates[-1]),
            "total_sessions": train_count,
            "pos_label_count": train_pos,
            "pos_rate": train_pos / train_count if train_count else 0.0,
        },
        "val": {
            "dates": [str(d) for d in val_dates],
            "start_date": str(val_dates[0]),
            "end_date": str(val_dates[-1]),
            "total_sessions": val_count,
            "pos_label_count": val_pos,
            "pos_rate": val_pos / val_count if val_count else 0.0,
        },
        "test": {
            "dates": [str(d) for d in test_dates],
            "start_date": str(test_dates[0]),
            "end_date": str(test_dates[-1]),
            "total_sessions": test_count,
            "pos_label_count": test_pos,
            "pos_rate": test_pos / test_count if test_count else 0.0,
        },
    }

    logger.info("=== Time-based Split Summary ===")
    logger.info(
        "Train: %d sessions (%d pos, %.2f%%) | Dates: %s to %s (%d days)",
        train_count,
        train_pos,
        train_pos / train_count * 100.0,
        train_dates[0],
        train_dates[-1],
        len(train_dates),
    )
    logger.info(
        "Val:   %d sessions (%d pos, %.2f%%) | Dates: %s to %s (%d days)",
        val_count,
        val_pos,
        val_pos / val_count * 100.0,
        val_dates[0],
        val_dates[-1],
        len(val_dates),
    )
    logger.info(
        "Test:  %d sessions (%d pos, %.2f%%) | Dates: %s to %s (%d days)",
        test_count,
        test_pos,
        test_pos / test_count * 100.0,
        test_dates[0],
        test_dates[-1],
        len(test_dates),
    )
    logger.info("================================")

    return train_df, val_df, test_df, split_metadata

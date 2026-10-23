"""Module for session-based clickstream sampling.

This script samples clickstream events by user_session using CRC32 hashing
so that all events belonging to selected sessions are retained intact.
It supports streaming directly from .zip archives or plain .csv files without
extracting large files to disk.
"""

import argparse
import logging
import os
import sys
import zipfile
import zlib
from collections import Counter
from typing import BinaryIO, Dict, Iterator, Optional, Set, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("sample_by_session")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sample clickstream events by user_session using CRC32 hashing."
    )
    parser.add_argument(
        "--src",
        type=str,
        required=True,
        help="Path to input source file (.csv or .zip).",
    )
    parser.add_argument(
        "--dst",
        type=str,
        default="data/raw/sample_nov_1pct.csv",
        help="Path to output CSV file (default: data/raw/sample_nov_1pct.csv).",
    )
    parser.add_argument(
        "--pct",
        type=float,
        default=1.0,
        help="Sampling percentage (default: 1.0, meaning 1%).",
    )
    parser.add_argument(
        "--member",
        type=str,
        default=None,
        help="Target member filename inside .zip archive. If not set and zip has 1 file, picks it automatically.",
    )
    return parser.parse_args()


def get_stream(src_path: str, member_name: Optional[str] = None) -> Tuple[BinaryIO, Optional[zipfile.ZipFile]]:
    """Open streaming binary reader from either a .zip or .csv file.

    Parameters
    ----------
    src_path : str
        Path to the source file (.zip or .csv).
    member_name : Optional[str]
        Member filename inside the archive if src_path is a .zip file.

    Returns
    -------
    Tuple[BinaryIO, Optional[zipfile.ZipFile]]
        A binary stream and the zipfile object (if opened) to ensure proper cleanup.
    """
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"Source file not found: {src_path}")

    lower_src = src_path.lower()
    if lower_src.endswith(".zip"):
        zip_ref = zipfile.ZipFile(src_path, mode="r")
        all_members = [
            info.filename
            for info in zip_ref.infolist()
            if not info.is_dir()
        ]

        if member_name:
            if member_name not in all_members:
                zip_ref.close()
                raise ValueError(
                    f"Specified member '{member_name}' not found in zip archive: {all_members}"
                )
            target = member_name
        else:
            if len(all_members) == 1:
                target = all_members[0]
            elif len(all_members) == 0:
                zip_ref.close()
                raise ValueError(f"Zip archive '{src_path}' contains no files.")
            else:
                zip_ref.close()
                raise ValueError(
                    f"Zip archive contains multiple files ({all_members}). "
                    "Please specify target file via --member."
                )

        logger.info("Streaming from zip archive member: %s", target)
        stream = zip_ref.open(target, mode="r")
        return stream, zip_ref

    if lower_src.endswith(".csv"):
        logger.info("Streaming from plain CSV: %s", src_path)
        stream = open(src_path, mode="rb")
        return stream, None

    raise ValueError(f"Unsupported file format for source '{src_path}'. Expected .zip or .csv.")


def extract_row_metadata(line_bytes: bytes) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    """Extract user_session, event_date_str, and event_type from a raw CSV line.

    Columns in REES46 format:
    0: event_time (e.g. 2019-11-01 00:00:00 UTC)
    1: event_type (view, cart, purchase, remove_from_cart)
    ...
    8 (last): user_session

    Parameters
    ----------
    line_bytes : bytes
        Raw CSV line in bytes.

    Returns
    -------
    Tuple[Optional[bytes], Optional[str], Optional[str]]
        (user_session_bytes, event_date_str, event_type_str)
    """
    stripped = line_bytes.rstrip(b"\r\n")
    if not stripped:
        return None, None, None

    last_comma_idx = stripped.rfind(b",")
    if last_comma_idx == -1:
        return None, None, None

    session_bytes = stripped[last_comma_idx + 1 :]
    if not session_bytes:
        return None, None, None

    first_comma_idx = stripped.find(b",")
    event_date_str: Optional[str] = None
    event_type_str: Optional[str] = None

    if first_comma_idx != -1:
        time_bytes = stripped[:first_comma_idx].strip()
        if len(time_bytes) >= 10:
            try:
                event_date_str = time_bytes[:10].decode("utf-8", errors="ignore")
            except Exception:
                event_date_str = None

        second_comma_idx = stripped.find(b",", first_comma_idx + 1)
        if second_comma_idx != -1:
            try:
                event_type_str = (
                    stripped[first_comma_idx + 1 : second_comma_idx]
                    .strip()
                    .decode("utf-8", errors="ignore")
                    .lower()
                )
            except Exception:
                event_type_str = None

    return session_bytes, event_date_str, event_type_str


def sample_clickstream(
    src_path: str,
    dst_path: str,
    pct: float,
    member_name: Optional[str] = None,
    log_interval: int = 5_000_000,
) -> Dict[str, object]:
    """Sample clickstream data by user_session using CRC32 hash modulo.

    Parameters
    ----------
    src_path : str
        Source file path (.zip or .csv).
    dst_path : str
        Destination file path.
    pct : float
        Percentage of sessions to retain (e.g., 1.0 for 1%).
    member_name : Optional[str]
        Member name inside zip archive if applicable.
    log_interval : int
        Number of lines between progress logs.

    Returns
    -------
    Dict[str, object]
        Summary metrics including rows read, rows written, distinct days, and event type counts.
    """
    logger.info("Starting session sampling: src=%s, dst=%s, pct=%s%%", src_path, dst_path, pct)
    os.makedirs(os.path.dirname(os.path.abspath(dst_path)), exist_ok=True)

    stream, zip_ref = get_stream(src_path, member_name)

    total_read = 0
    total_written = 0
    distinct_dates: Set[str] = set()
    event_counts: Counter[str] = Counter()

    try:
        header = stream.readline()
        if not header:
            raise ValueError(f"Input file '{src_path}' is empty.")

        with open(dst_path, "wb") as out_f:
            out_f.write(header)

            for line in stream:
                total_read += 1

                if total_read % log_interval == 0:
                    logger.info(
                        "Progress: %d rows read, %d rows sampled (%.2f%%)",
                        total_read,
                        total_written,
                        (total_written / total_read) * 100.0 if total_read else 0.0,
                    )

                session_bytes, date_str, event_type = extract_row_metadata(line)
                if not session_bytes:
                    continue

                # Hash session identifier to select sessions deterministically
                if (zlib.crc32(session_bytes) % 100) < pct:
                    out_f.write(line)
                    total_written += 1
                    if date_str:
                        distinct_dates.add(date_str)
                    if event_type:
                        event_counts[event_type] += 1

    finally:
        stream.close()
        if zip_ref is not None:
            zip_ref.close()

    summary: Dict[str, object] = {
        "rows_read": total_read,
        "rows_written": total_written,
        "distinct_days_count": len(distinct_dates),
        "distinct_days": sorted(list(distinct_dates)),
        "event_counts": dict(event_counts),
    }

    logger.info(
        "Sampling complete: Read %d rows, Wrote %d rows (%.3f%%)",
        total_read,
        total_written,
        (total_written / total_read * 100.0) if total_read else 0.0,
    )
    logger.info("Distinct days count: %d", len(distinct_dates))
    logger.info("Event type distribution: %s", dict(event_counts))

    return summary


def main() -> None:
    """Main CLI entrypoint."""
    args = parse_args()
    sample_clickstream(
        src_path=args.src,
        dst_path=args.dst,
        pct=args.pct,
        member_name=args.member,
    )


if __name__ == "__main__":
    main()

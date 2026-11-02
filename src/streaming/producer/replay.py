"""
P2-B-B2-01: Phát lại luồng sự kiện từ dữ liệu CSV mẫu H02 mà không cần tải toàn bộ dữ liệu vào RAM.

Mặc định chạy ở chế độ thử nghiệm (dry-run), không gửi dữ liệu vào Kafka.
Dữ liệu JSON hiện tại chỉ là bản NHÁP, chưa phải hợp đồng dữ liệu S2 được phê duyệt.

Yêu cầu Python 3.6 trở lên; chế độ dry-run không cần cài đặt thư viện bên thứ ba.
"""

import argparse
import csv
import json
import logging
import math
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from itertools import islice
from pathlib import Path

LOG = logging.getLogger("b2_producer")
COLUMNS = [
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session",
]
EVENT_TYPES = {"view", "cart", "remove_from_cart", "purchase"}
DRAFT_VERSION = "draft-b2-01-not-approved-s2"


def utc_date(text):
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Use UTC date YYYY-MM-DD")


def positive_rate(text):
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError("--rate must be a number")
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError(
            "--rate must be a finite number > 0"
        )
    return value


def csv_files(input_path):
    path = Path(input_path).expanduser()

    if path.is_file() and path.suffix.lower() == ".csv":
        return [path]

    if path.is_dir():
        files = sorted(path.glob("part-*.csv"))
        if files:
            return files
        raise ValueError("No part-*.csv files in " + str(path))

    raise ValueError(
        "CSV input file/folder not found: " + str(path)
    )


def batches(iterator, chunk_size):
    """Keep at most chunk_size CSV records in memory."""
    while True:
        batch = list(islice(iterator, chunk_size))
        if not batch:
            break
        yield batch


def draft_event(row):
    """Return UTC date and DRAFT JSON; preserve ID/price as strings."""
    try:
        dt = datetime.strptime(
            row["event_time"].strip(),
            "%Y-%m-%d %H:%M:%S UTC"
        ).replace(tzinfo=timezone.utc)

        event_type = row["event_type"].strip()
        if event_type not in EVENT_TYPES:
            raise ValueError("Unknown event_type: " + event_type)

        for field in ("product_id", "category_id", "user_id"):
            raw = row[field].strip()
            if raw and not raw.isdigit():
                raise ValueError("Invalid integer field: " + field)

        raw_price = row["price"].strip()
        if raw_price:
            price = Decimal(raw_price)
            if not price.is_finite():
                raise ValueError("Non-finite price")

    except (
        KeyError, ValueError, InvalidOperation, AttributeError
    ) as exc:
        raise ValueError("Invalid CSV event: " + str(exc))

    event = {
        name: (row[name].strip() or None)
        for name in COLUMNS
    }
    event["event_time"] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    event["schema_version"] = DRAFT_VERSION

    return dt.date(), event


def arguments():
    p = argparse.ArgumentParser(
        description="P2-B-B2-01 H02 producer (DRY-RUN default)"
    )

    p.add_argument(
        "--input", required=True,
        help="One CSV, or folder of part-*.csv"
    )
    p.add_argument(
        "--start-date", type=utc_date,
        help="Inclusive UTC YYYY-MM-DD"
    )
    p.add_argument(
        "--end-date", type=utc_date,
        help="Inclusive UTC YYYY-MM-DD"
    )
    p.add_argument(
        "--limit", type=int, default=10,
        help="Max accepted events (0=all, dry-run only)"
    )
    p.add_argument(
        "--rate", type=positive_rate, default=10.0,
        help="Maximum target accepted events per second"
    )
    p.add_argument(
        "--chunk-size", type=int, default=1000,
        help="CSV rows buffered per chunk; 1..100000"
    )
    p.add_argument(
        "--preview", type=int, default=2,
        help="Number of draft JSON examples in dry-run"
    )
    p.add_argument(
        "--send-test", action="store_true",
        help="Opt-in sending to a separate Kafka TEST topic only"
    )
    p.add_argument(
        "--topic", default="b2_test_rees46",
        help="Topic must start with b2_test_ when sending"
    )
    p.add_argument(
        "--bootstrap-servers", default="127.0.0.1:9092",
        help="Kafka address host:port[,host:port]"
    )
    return p


def replay(args):
    if not 1 <= args.chunk_size <= 100000:
        raise ValueError("--chunk-size must be 1..100000")

    if args.limit < 0 or args.preview < 0:
        raise ValueError("--limit and --preview must be >= 0")

    if (
        args.start_date and args.end_date
        and args.start_date > args.end_date
    ):
        raise ValueError("--start-date must be <= --end-date")

    if args.send_test and (
        args.limit == 0
        or not args.topic.startswith("b2_test_")
    ):
        raise ValueError(
            "--send-test needs --limit > 0 and a b2_test_ topic"
        )

    files = csv_files(args.input)

    stats = {
        "scanned": 0,
        "selected": 0,
        "sent_ack": 0,
        "send_failed": 0,
        "malformed": 0,
        "filtered_date": 0,
    }

    producer = None
    started = time.monotonic()
    next_send_at = started

    LOG.info(
        "START mode=%s files=%d limit=%d rate=%s "
        "chunk=%d schema=%s",
        "KAFKA-TEST" if args.send_test else "DRY-RUN",
        len(files), args.limit, args.rate,
        args.chunk_size, DRAFT_VERSION
    )

    try:
        if args.send_test:
            try:
                from kafka import KafkaProducer
            except ImportError:
                raise RuntimeError(
                    "Missing kafka-python in VM Python environment"
                )

            try:
                producer = KafkaProducer(
                    bootstrap_servers=[
                        s.strip()
                        for s in args.bootstrap_servers.split(",")
                    ],
                    acks="all",
                    key_serializer=lambda x: (
                        x.encode("utf-8")
                        if x is not None else None
                    ),
                    value_serializer=lambda x: json.dumps(
                        x,
                        ensure_ascii=False,
                        separators=(",", ":")
                    ).encode("utf-8"),
                )
            except Exception as exc:
                raise RuntimeError(
                    "Kafka producer initialization failed: "
                    + str(exc)
                )

        reached_limit = False

        for path in files:
            LOG.info("READ file=%s", path.name)

            with path.open(
                "r", encoding="utf-8-sig", newline=""
            ) as handle:
                reader = csv.DictReader(handle)

                if reader.fieldnames != COLUMNS:
                    raise ValueError(
                        "Invalid H02 CSV header in " + str(path)
                    )

                for chunk in batches(reader, args.chunk_size):
                    for row in chunk:
                        stats["scanned"] += 1

                        if (
                            None in row
                            or any(
                                row.get(c) is None for c in COLUMNS
                            )
                        ):
                            stats["malformed"] += 1
                            continue

                        try:
                            event_date, event = draft_event(row)
                        except ValueError as exc:
                            stats["malformed"] += 1

                            if stats["malformed"] <= 3:
                                LOG.warning(
                                    "BAD_ROW file=%s record=%d reason=%s",
                                    path.name,
                                    stats["scanned"],
                                    exc
                                )
                            continue

                        if (
                            (
                                args.start_date
                                and event_date < args.start_date
                            )
                            or (
                                args.end_date
                                and event_date > args.end_date
                            )
                        ):
                            stats["filtered_date"] += 1
                            continue

                        # Limit pacing to accepted events.
                        delay = next_send_at - time.monotonic()
                        if delay > 0:
                            time.sleep(delay)

                        next_send_at = (
                            time.monotonic() + 1.0 / args.rate
                        )
                        stats["selected"] += 1

                        if args.send_test:
                            try:
                                metadata = producer.send(
                                    args.topic,
                                    key=event["user_session"],
                                    value=event
                                ).get(timeout=20)
                            except Exception as exc:
                                stats["send_failed"] += 1
                                raise RuntimeError(
                                    "Kafka send/ACK failed: "
                                    + str(exc)
                                )

                            stats["sent_ack"] += 1

                            if (
                                stats["sent_ack"] <= 3
                                or stats["sent_ack"] % 1000 == 0
                            ):
                                LOG.info(
                                    "ACK topic=%s partition=%s offset=%s",
                                    metadata.topic,
                                    metadata.partition,
                                    metadata.offset
                                )

                        elif stats["selected"] <= args.preview:
                            LOG.info(
                                "DRAFT_JSON=%s",
                                json.dumps(
                                    event, ensure_ascii=False
                                )
                            )

                        if (
                            args.limit > 0
                            and stats["selected"] >= args.limit
                        ):
                            reached_limit = True
                            break

                    if reached_limit:
                        break

            if reached_limit:
                break

    finally:
        if producer is not None:
            try:
                producer.close(timeout=30)
            except Exception as exc:
                LOG.error("Kafka close failed: %s", exc)

        LOG.info(
            "SUMMARY scanned=%d selected=%d sent_ack=%d "
            "send_failed=%d malformed=%d filtered_date=%d "
            "elapsed=%.2fs",
            stats["scanned"],
            stats["selected"],
            stats["sent_ack"],
            stats["send_failed"],
            stats["malformed"],
            stats["filtered_date"],
            time.monotonic() - started
        )

    return stats


def main(argv=None):
    args = arguments().parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    try:
        replay(args)
    except (ValueError, RuntimeError, OSError) as exc:
        LOG.error("B2-01 FAILED: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

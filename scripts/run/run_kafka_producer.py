"""Điểm khởi chạy chương trình từ dòng lệnh (CLI) cho Kafka Event Producer chính thức (Task S2)."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.streaming.producer.event_producer import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())

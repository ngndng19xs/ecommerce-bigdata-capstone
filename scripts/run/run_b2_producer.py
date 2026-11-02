
"""Điểm khởi chạy chương trình từ dòng lệnh (CLI) cho nhiệm vụ P2-B-B2-01."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.streaming.producer.replay import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())

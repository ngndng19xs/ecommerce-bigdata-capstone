# P3-B-B4-03 — Restart, Replay and Out-of-order

## 1. Mục tiêu
Kiểm thử hệ thống Spark Structured Streaming:
- Restart consumer với cùng checkpoint.
- Xử lý Cart đến muộn theo event_time.
- Replay Kafka offsets bằng checkpoint mới.
- Chống ghi trùng ledger theo (topic, partition, offset).
- Bảo đảm kết quả funnel không thay đổi sau replay.

## 2. Môi trường
- Apache Spark 3.1.2
- Apache Kafka 2.8.0
- Python 3.8.11
- Spark local[1]
- Timezone UTC

## 3. Dữ liệu kiểm thử
- Nguồn: REES46, sử dụng Kafka events từ B4-02.
- Source topic: ecommerce_events_b4_oct22_v1
- Test topic: ecommerce_events_b4_restart_v1
- Số phiên: 1
- Số sự kiện: 3
- Kafka offsets: 0, 1, 2

Session:
001226df-03c7-41e6-a9c4-7d0233348bd5

Thời gian sự kiện:
- View: 2019-10-01 14:12:34 UTC
- Cart: 2019-10-01 14:12:48 UTC
- Purchase: 2019-10-01 14:13:11 UTC

## 4. Kết quả nghiệm thu

| Run | Kịch bản | Kết quả |
| --- | --- | --- |
| Run 1 | View + Purchase | INSERTED_ROWS=2 |
| Run 2 | Restart CP1 | Không tạo thêm commit |
| Run 3 | Cart đến muộn | INSERTED_ROWS=1 |
| Run 4 | Replay CP2 | INSERTED_ROWS=0 ở cả 2 batch |

Funnel trước Cart: View=1, Cart=0, Purchase=0.

Funnel sau Cart: View=1, Cart=1, Purchase=1.

## 5. Bằng chứng kiểm thử
- Tổng 3 Kafka offsets duy nhất.
- Ledger có 2 committed batches.
- Replay không tạo thêm ledger commit.
- SHA-256 ledger trước và sau replay không đổi.
- CP2 final offset = 3.
- Snapshot Run 3 và Run 4 có kết quả giống nhau.
- Integration verification PASS.
- Regression test: 15 passed.

## 6. Script nghiệm thu
- scripts/test/b4_03_send.py
- tests/integration/verify_b4_03_run1.py
- tests/integration/verify_b4_03_run3.py
- tests/integration/verify_b4_03_replay.py

## 7. Giới hạn
Kiểm thử B4-03 sử dụng 3 sự kiện thuộc một phiên REES46.
Chưa kiểm thử trên toàn bộ dataset hoặc đánh giá tải lớn.

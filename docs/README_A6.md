# A6 – Session Analytics

## 1. Mục tiêu

Phân tích hành vi người dùng theo `user_session` trên dữ liệu eCommerce tháng 10–11/2019. Công việc gồm:

- **A6-01:** Tổng hợp thông tin từng session.
- **A6-02:** So sánh session có mua hàng và không mua hàng, đồng thời thống kê phân phối số event/session.

## 2. Input và Output

**Input:**
- HDFS: `/data/curated/events`
- Dữ liệu Parquet đã làm sạch, phân vùng theo `event_date`.
- Phạm vi: October–November 2019; thời gian UTC.

**Output:**

| Task | HDFS output |
|---|---|
| A6-01 | `/data/analytics/session_summary` |
| A6-02 | `/data/analytics/session_summary_analysis` |

A6-02 tạo hai kết quả:
- `group_summary`: thống kê tổng hợp theo nhóm session.
- `event_distribution`: phân phối số session theo khoảng số event.

## 3. Cách chạy

Chạy trên máy ảo CentOS có Spark và Hadoop:

```bash
env -u PYSPARK_DRIVER_PYTHON spark-submit \
  --master local[2] \
  --driver-memory 4g \
  ~/capstone/analytics/session/session_analytics.py \
  > ~/capstone/analytics/session/session_analytics.log 2>&1
```

Sau khi A6-01 hoàn thành, chạy A6-02:

```bash
env -u PYSPARK_DRIVER_PYTHON spark-submit \
  --master local[2] \
  --driver-memory 4g \
  ~/capstone/analytics/session/session_comparison.py \
  > ~/capstone/analytics/session/session_comparison.log 2>&1
```

Kiểm tra kết quả:

```bash
grep "SUCCESS" ~/capstone/analytics/session/session_analytics.log
grep "SUCCESS" ~/capstone/analytics/session/session_comparison.log
```

## 4. Kết quả chính

- Tổng số session: **23,016,650**.
- Session có mua hàng: **1,402,758**.
- Session không mua hàng: **21,613,892**.
- A6-01 và A6-02 chạy thành công, kết quả đã được ghi lên HDFS.

Chi tiết phân phối session được trình bày trong `H10_session_distribution.md`.

## 5. Lưu ý

Các thống kê toàn session phục vụ phân tích mô tả và lựa chọn ngưỡng N. Không sử dụng thông tin phát sinh sau thời điểm dự đoán hoặc nhãn của tập test làm đặc trưng đầu vào mô hình.
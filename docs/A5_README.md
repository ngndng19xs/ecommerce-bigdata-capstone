# Batch Conversion Funnel – Task A5

## 1. Mục tiêu

Tính toán Conversion Funnel theo ngày từ dữ liệu sự kiện eCommerce đã được ETL và lưu trên HDFS.

## 2. Đầu vào và đầu ra

- Input: `/data/curated/events`
- Output: `/data/analytics/conversion_funnel`
- Output format: Parquet
- Partition column: `funnel_date`

## 3. Yêu cầu môi trường

- Hadoop HDFS đang hoạt động.
- PySpark/Spark đã cài đặt.
- Python 3.
- Tài khoản thực thi có quyền đọc input và ghi output trên HDFS.

## 4. Chạy chương trình chính

Từ thư mục chứa `batch_conversion_funnel.py`:

```bash
env -u PYSPARK_DRIVER_PYTHON spark-submit batch_conversion_funnel.py > funnel_run.log 2>&1
```

## 5. Kiểm tra đầu ra

```bash
hdfs dfs -ls /data/analytics/conversion_funnel
hdfs dfs -count -h /data/analytics/conversion_funnel
```

Đọc lại dữ liệu bằng:

```bash
env -u PYSPARK_DRIVER_PYTHON spark-submit check_funnel_output.py
```

## 6. Xác minh độc lập

```bash
env -u PYSPARK_DRIVER_PYTHON spark-submit verify_funnel_independent.py > verify_funnel.log 2>&1
```

Truy vấn SQL tương ứng được lưu tại:

`sql/verify_funnel_independent.sql`

## 7. Kiểm tra chất lượng kết quả

```bash
env -u PYSPARK_DRIVER_PYTHON spark-submit validate_funnel_metrics.py > validate_funnel_metrics.log 2>&1
```

Kết quả mong đợi:

```text
Total days: 31
Duplicate dates: 0
Invalid daily records: 0
VALIDATION RESULT: PASS
```

## 8. Quy tắc tính toán

- Đơn vị tính: `user_session`.
- Thứ tự sự kiện: `event_time`, `event_type_rank`, `event_sort_key`.
- Cart phải xảy ra sau View.
- Purchase phải xảy ra sau Cart.
- Ngày Funnel lấy theo View đầu tiên theo UTC.
- Mỗi phiên chỉ tính một lần ở mỗi giai đoạn.


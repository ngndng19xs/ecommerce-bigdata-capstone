# Tiêu Chí Nghiệm Thu Đồ Án (Acceptance Checklist)

Bảng kiểm này được dùng để đánh giá tiến độ thực hiện và chất lượng đầu ra của từng hạng mục trong đồ án **E-commerce Big Data Capstone** trước khi nghiệm thu và bảo vệ.

---

## 1. Chuẩn Hóa Cấu Trúc & Quản Lý Dự Án
- [x] **Cấu trúc thư mục chuẩn**: Đã phân tách rõ ràng các tầng `src/`, `sql/`, `scripts/`, `docs/`, `results/`, `tests/`.
- [x] **File `.gitignore`**: Đảm bảo không commit file dữ liệu thô (`data/raw/`, `data/processed/`), checkpoint Spark, log Kafka và secret keys.
- [x] **File `requirements.txt`**: Ghi nhận đầy đủ và chính xác các thư viện cần thiết đã xác nhận.
- [ ] **Tài liệu hướng dẫn triển khai**: README.md mô tả rõ ràng các bước thiết lập môi trường và chạy thử.

---

## 2. Ingestion & Kiểm Soát Chất Lượng Dữ Liệu (Data Quality)
- [ ] **Nạp dữ liệu thô (`src/ingestion/`)**: Script nạp dữ liệu hoạt động ổn định, có cơ chế ghi nhận log tiến trình.
- [ ] **Kiểm tra Schema**: Kiểm tra kiểu dữ liệu của các trường clickstream (`event_time`, `event_type`, `product_id`, `category_id`, `price`, `user_id`, `user_session`).
- [ ] **Data Quality Suite (`tests/data_quality/`)**:
  - [ ] Kiểm tra tỷ lệ giá trị null / thiếu trên các trường khóa chính.
  - [ ] Kiểm tra logic nghiệp vụ: `price >= 0`, `event_type IN ('view', 'cart', 'purchase', 'remove_from_cart')`.
  - [ ] Kiểm tra tính hợp lệ của timestamp.

---

## 3. Batch Processing & Data Lake / Warehouse
- [ ] **Batch ETL Pipeline (`src/batch/etl/`)**:
  - [ ] Làm sạch và khử trùng lặp (deduplication) sự kiện.
  - [ ] Ép kiểu chuẩn (`TimestampType`, `DecimalType` / `DoubleType`, `StringType`).
  - [ ] Ghi dữ liệu đã xử lý ra định dạng Parquet có nén (Snappy).
  - [ ] Phân vùng (Partitioning) tối ưu theo thời gian (`event_year`, `event_month`).
- [ ] **Hiệu Năng & Benchmark (`src/batch/analytics/` & `results/benchmark/`)**:
  - [ ] Đo đạc thời gian xử lý và dung lượng lưu trữ giữa CSV và Parquet.
  - [ ] Báo cáo số liệu benchmark cụ thể lưu trong `results/benchmark/`.
- [ ] **Hive DDL & Analytics (`sql/hive/`, `sql/analytics/`)**:
  - [ ] Tạo cơ sở dữ liệu `ecommerce_db` và các bảng Fact/Dimension.
  - [ ] Thực thi các câu truy vấn phân tích phễu chuyển đổi (Conversion Funnel).
  - [ ] Phân tích hành vi theo thương hiệu (Brand) và nhóm hàng hóa (Category).

---

## 4. Streaming Pipeline (Speed Layer)
- [ ] **Kafka Producer (`src/streaming/producer/`)**:
  - [ ] Phát lại luồng sự kiện clickstream theo đúng hoặc gia tốc thời gian thực.
  - [ ] Xử lý ngắt kết nối và gửi lại (retry) khi gặp lỗi kết nối Kafka.
- [ ] **Spark Structured Streaming (`src/streaming/consumer/`)**:
  - [ ] Đọc dữ liệu liên tục từ Kafka topic với schema cố định.
  - [ ] Sử dụng Watermarking để xử lý sự kiện đến trễ (late-arriving data).
  - [ ] Tính toán cửa sổ trượt (windowed aggregations) theo dõi số lượt view/cart theo thời gian.
  - [ ] Lưu trạng thái checkpoint tin cậy (`checkpoint_dir`).

---

## 5. Machine Learning Pipeline
- [ ] **Dữ Liệu Phiên & Gán Nhãn (`src/ml/dataset/`)**:
  - [ ] Gom nhóm sự kiện theo `user_session` và xác định nhãn mục tiêu `is_purchased` (0 hoặc 1).
- [ ] **Đặc Trưng Học Máy (`src/ml/features/`)**:
  - [ ] Trích xuất các đặc trưng trước khi diễn ra quyết định mua hàng (tránh data leakage).
  - [ ] Chuẩn hóa đặc trưng (VectorAssembler, StandardScaler, StringIndexer).
- [ ] **Huấn Luyện & Đánh Giá (`src/ml/training/`, `src/ml/evaluation/`)**:
  - [ ] Huấn luyện với Spark MLlib (ít nhất 2 mô hình, ví dụ Logistic Regression vs Random Forest / GBT).
  - [ ] Đánh giá độ chính xác qua ROC-AUC, PR-AUC, F1-Score trên tập Test độc lập.
  - [ ] Lưu bảng metrics và đồ thị ROC curve vào `results/metrics/` và `results/figures/`.

---

## 6. Báo Cáo & Demo Phục Vụ Bảo Vệ (Defense Readiness)
- [ ] **Biểu Đồ Trực Quan (`src/visualization/charts/`, `results/figures/`)**: Đầy đủ biểu đồ phễu chuyển đổi, phân bố phiên, kết quả ML.
- [ ] **Kịch Bản Demo End-to-End (`scripts/demo/`)**: Có sẵn kịch bản chạy thử nghiệm minh họa từ lúc nạp dữ liệu đến ra kết quả trong buổi báo cáo.
- [ ] **Báo Cáo Hoàn Chỉnh (`reports/`)**: Báo cáo tổng kết đồ án, slides thuyết trình định dạng PDF/Markdown.

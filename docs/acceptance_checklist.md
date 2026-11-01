# Tiêu Chí Nghiệm Thu Đồ Án Capstone

Tài liệu này liệt kê các tiêu chí nghiệm thu cho từng phần của dự án Big Data Capstone.

Trạng thái: ⬜ Chưa kiểm tra | ✅ Đạt | ❌ Không đạt

---

## S4 – Git Repository & Quy Ước Mã Nguồn

| # | Tiêu chí | Trạng thái | Ghi chú |
|---|----------|------------|---------|
| S4.1 | Repository có Git remote trên GitHub | ⬜ | |
| S4.2 | Hai thành viên có thể truy cập Repo | ⬜ | |
| S4.3 | Cấu trúc thư mục được thống nhất | ⬜ | |
| S4.4 | Có cấu hình mẫu (`config.example.yaml`) dùng chung | ⬜ | |
| S4.5 | Có cấu hình riêng (`config.yaml`) được Git ignore | ⬜ | |
| S4.6 | Không commit CSV/Parquet lớn | ⬜ | |
| S4.7 | Không commit secret (mật khẩu, token, key) | ⬜ | |
| S4.8 | Có README hướng dẫn cài đặt và vận hành | ⬜ | |
| S4.9 | Có quy tắc branch / commit / review | ⬜ | |
| S4.10 | Đường dẫn HDFS thống nhất với cấu trúc A | ⬜ | |
| S4.11 | Các cấu hình chưa xác nhận được ghi rõ (Kafka topic, Hive table) | ⬜ | |
| S4.12 | Người A review thay đổi S4 | ⬜ | |
| S4.13 | PR được merge thành công vào main | ⬜ | |

---

## 1. Hạ Tầng và Môi Trường (S1)

| # | Tiêu chí | Trạng thái | Ghi chú |
|---|----------|------------|---------|
| 1.1 | VM A và VM B đã được cài đặt và cấu hình | ⬜ | |
| 1.2 | HDFS hoạt động trên cả hai VM | ⬜ | |
| 1.3 | Apache Spark đã cài đặt và chạy được | ⬜ | |
| 1.4 | Apache Kafka đã cài đặt và chạy được | ⬜ | |
| 1.5 | Apache Hive đã cài đặt và truy vấn được | ⬜ | |
| 1.6 | Python và PySpark tương thích | ⬜ | |

---

## 2. Khảo Sát Dữ Liệu (S2 / A1-A2)

| # | Tiêu chí | Trạng thái | Ghi chú |
|---|----------|------------|---------|
| 2.1 | Dữ liệu REES46 đã tải về đầy đủ | ⬜ | |
| 2.2 | Schema 9 trường đã được xác nhận | ⬜ | |
| 2.3 | Thống kê mô tả cơ bản đã hoàn thành | ⬜ | |
| 2.4 | Các vấn đề chất lượng dữ liệu đã được ghi nhận | ⬜ | |
| 2.5 | Hợp đồng dữ liệu (data contract) đã được lập | ⬜ | |

---

## 3. Nạp và Xử Lý Dữ Liệu (A3-A5)

| # | Tiêu chí | Trạng thái | Ghi chú |
|---|----------|------------|---------|
| 3.1 | Dữ liệu thô đã nạp vào HDFS `/data/raw/` | ⬜ | |
| 3.2 | ETL pipeline chạy thành công | ⬜ | |
| 3.3 | Parquet curated tại `/data/curated/events_cleaned` | ⬜ | |
| 3.4 | Dữ liệu lỗi tại `/data/curated/_quarantine/` | ⬜ | |
| 3.5 | Partition theo `event_date` (UTC) | ⬜ | |
| 3.6 | Không có data leakage từ future events | ⬜ | |

---

## 4. Hive và Analytics (A4-A6)

| # | Tiêu chí | Trạng thái | Ghi chú |
|---|----------|------------|---------|
| 4.1 | Hive database và tables đã tạo | ⬜ | |
| 4.2 | Conversion funnel query chạy đúng | ⬜ | |
| 4.3 | Session summary đã tính toán | ⬜ | |
| 4.4 | Product performance đã phân tích | ⬜ | |
| 4.5 | Benchmark batch processing đã đo | ⬜ | |

---

## 5. Streaming (B1-B4)

| # | Tiêu chí | Trạng thái | Ghi chú |
|---|----------|------------|---------|
| 5.1 | Kafka topic đã tạo và hoạt động | ⬜ | |
| 5.2 | Producer phát lại sự kiện thành công | ⬜ | |
| 5.3 | Spark Structured Streaming consumer chạy được | ⬜ | |
| 5.4 | Aggregation theo window hoạt động | ⬜ | |
| 5.5 | Kết quả streaming ghi vào HDFS | ⬜ | |

---

## 6. Machine Learning (B5-B8)

| # | Tiêu chí | Trạng thái | Ghi chú |
|---|----------|------------|---------|
| 6.1 | Session dataset đã tạo | ⬜ | |
| 6.2 | Feature engineering hoàn thành | ⬜ | |
| 6.3 | Train/test split theo thời gian | ⬜ | |
| 6.4 | Model đã huấn luyện | ⬜ | |
| 6.5 | Metrics (AUC, F1, Precision, Recall) đã tính | ⬜ | |
| 6.6 | Không có data leakage | ⬜ | |

---

## 7. Tích Hợp và Demo (B9 / S6)

| # | Tiêu chí | Trạng thái | Ghi chú |
|---|----------|------------|---------|
| 7.1 | Demo end-to-end chạy được | ⬜ | |
| 7.2 | Kịch bản demo có script rõ ràng | ⬜ | |
| 7.3 | Kết quả trực quan hóa đã chuẩn bị | ⬜ | |

---

## 8. Chất Lượng và Báo Cáo (S5)

| # | Tiêu chí | Trạng thái | Ghi chú |
|---|----------|------------|---------|
| 8.1 | Unit tests đã viết và pass | ⬜ | |
| 8.2 | Data quality checks đã chạy | ⬜ | |
| 8.3 | Báo cáo cuối kỳ đã hoàn thiện | ⬜ | |
| 8.4 | Slide trình bày đã chuẩn bị | ⬜ | |

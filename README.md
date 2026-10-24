# E-Commerce Clickstream Big Data Capstone

Hệ thống xử lý và phân tích dữ liệu lớn (Big Data Pipeline) cho dữ liệu clickstream thương mại điện tử, kết hợp **Batch Processing (PySpark, Hive)**, **Streaming (Apache Kafka, Spark Structured Streaming)**, và **Machine Learning (Spark MLlib)** để phân tích hành vi người dùng, phễu chuyển đổi và dự đoán ý định mua hàng.

---

## 📁 Cấu Trúc Thư Mục Dự Án

```text
.
├── README.md
├── .gitignore                      # Bỏ qua tệp không cần thiết(các tệp trong đây không đẩy lên github)
├── requirements.txt                # Chỉ ghi dependency đã xác nhận
├── configs/                        # Cấu hình mẫu, không chứa mật khẩu/token
│   └── config.example.yaml
├── data/
│   ├── raw/                        # Dữ liệu tạm cục bộ; không commit
│   └── processed/                  # Dữ liệu tạm cục bộ; không commit
├── docs/
│   ├── architecture.md             # Kiến trúc tổng thể hệ thống (Lambda/Hybrid)
│   ├── naming_and_code_standards.md# Quy chuẩn đặt tên và viết code (PEP 8, SQL, Git)
│   └── acceptance_checklist.md     # Danh sách tiêu chí nghiệm thu đồ án
├── notebooks/                      # Khảo sát/thử nghiệm; không thay thế pipeline chính
├── reports/                        # Báo cáo cuối kỳ, nội dung phục vụ bảo vệ
├── results/
│   ├── benchmark/                  # Số liệu benchmark gọn, có thể commit
│   ├── metrics/                    # Metrics ML/kiểm thử gọn
│   └── figures/                    # Hình/bảng kết quả gọn
├── scripts/
│   ├── ingest/                     # Lệnh gọi pipeline nạp dữ liệu
│   ├── run/                        # Entry point chạy pipeline
│   └── demo/                       # Kịch bản demo end-to-end
├── sql/
│   ├── hive/                       # DDL, database/table/partition
│   └── analytics/                  # Truy vấn funnel/session/brand
├── src/
│   ├── common/                     # Schema, config, logging, helpers dùng chung
│   ├── ingestion/                  # Đọc, kiểm tra và nạp nguồn dữ liệu
│   ├── batch/
│   │   ├── etl/                    # Làm sạch, chuẩn hóa, ghi Parquet
│   │   └── analytics/              # Phân tích batch, benchmark
│   ├── streaming/
│   │   ├── producer/               # Phát lại sự kiện lịch sử vào Kafka
│   │   └── consumer/               # Spark Structured Streaming
│   ├── ml/
│   │   ├── dataset/                # Dữ liệu cấp phiên, nhãn
│   │   ├── features/               # Đặc trưng trước thời điểm dự đoán
│   │   ├── training/               # Huấn luyện Spark MLlib
│   │   └── evaluation/             # Đánh giá, lưu metrics
│   └── visualization/
│       ├── charts/                 # Biểu đồ tĩnh cho phân tích/báo cáo
│       └── dashboards/             # Dashboard tương tác nếu được thống nhất triển khai
└── tests/
    ├── unit/                       # Kiểm thử đơn vị các hàm logic
    ├── integration/                # Kiểm thử tích hợp luồng pipeline
    └── data_quality/               # Kiểm tra chất lượng và tính toàn vẹn dữ liệu
```

---

## 🛠️ Yêu Cầu Môi Trường & Cài Đặt

### 1. Yêu Cầu Tiên Quyết
- **Python**: `>= 3.9` (Khuyến nghị 3.10 hoặc 3.11)
- **Java**: OpenJDK 8 hoặc 11 (bắt buộc đối với Apache Spark)
- **Apache Spark**: `>= 3.4.0`
- **Apache Kafka**: `>= 2.8.0` (cho tầng Streaming)

### 2. Thiết Lập Môi Trường Ảo
```bash
# Tạo môi trường ảo Python
python -m venv .venv

# Kích hoạt trên Windows:
.venv\Scripts\activate

# Kích hoạt trên Linux/macOS:
source .venv/bin/activate

# Cài đặt các gói phụ thuộc đã xác nhận
pip install -r requirements.txt
```

### 3. Cấu Hình
Sao chép cấu hình mẫu và tùy chỉnh các tham số kết nối phù hợp:
```bash
cp configs/config.example.yaml configs/config.yaml
```
> **Lưu ý:** Không commit file `configs/config.yaml` chứa thông tin nhạy cảm. Hệ thống đã cấu hình sẵn trong `.gitignore`.

---

## 🚀 Hướng Dẫn Vận Hành Pipeline

### 1. Nạp và Chuẩn Bị Dữ Liệu (Ingestion)
Đặt dữ liệu thô (ví dụ file CSV từ Kaggle E-Commerce Clickstream) vào thư mục `data/raw/`, sau đó khởi chạy script nạp:
```bash
python -m src.ingestion.load_data
```

### 2. Xử Lý Hàng Loạt (Batch ETL)
Làm sạch, chuẩn hóa kiểu dữ liệu, khử trùng lặp và lưu thành Parquet:
```bash
python -m src.batch.etl.clean_events
```

### 3. Chạy Streaming (Real-time Pipeline)
Khởi chạy Kafka Producer phát lại sự kiện và Spark Structured Streaming consumer:
```bash
# Terminal 1: Phát lại luồng sự kiện vào Kafka
python -m src.streaming.producer.event_replay

# Terminal 2: Spark Structured Streaming xử lý luồng
python -m src.streaming.consumer.spark_stream
```

### 4. Huấn Luyện Mô Hình Học Máy (MLlib)
Tạo đặc trưng và huấn luyện mô hình dự đoán ý định mua hàng:
```bash
python -m src.ml.training.train_classifier
```

### 5. Chạy Kiểm Thử (Tests)
```bash
pytest tests/
```

---

## 🤖 Machine Learning Pipeline (Nhiệm Vụ T012)

Pipeline dự đoán ý định mua hàng (Purchase Intent Prediction) ở cấp độ phiên người dùng với mô hình Spark MLlib Logistic Regression, bảo đảm nghiêm ngặt nguyên tắc **chống rò rỉ dữ liệu (Anti-Data-Leakage)** theo đặc tả bài toán T010.

### 1. Quy Trình Vận Hành End-to-End Theo Thứ Tự

```bash
# Bước 1: Cắt mẫu 1% dữ liệu theo user_session (đọc streaming trực tiếp từ file nén .zip)
python scripts/ingest/sample_by_session.py --src "E:\Data_SamSung\Github\raw_download\2019-Nov.csv.zip" --pct 1

# Bước 2: Tạo ma trận đặc trưng cấp phiên (Head: rn <= 5) và gán nhãn (Tail: rn > 5)
python -m src.ml.dataset.build_session_dataset

# Bước 3: Huấn luyện mô hình Logistic Regression và đánh giá toàn diện trên tập Test
python -m src.ml.training.train_lr

# Bước 4: Chạy bộ kiểm thử đơn vị chống leakage và kiểm tra logic pipeline
pytest tests/unit -q
```

### 2. Các Giả Định Nghiệp Vụ Cốt Lõi (Assumptions)
- **Tiền tố quan sát $K = 5$:**
  - Chỉ sử dụng tối đa $K=5$ sự kiện đầu tiên của mỗi phiên để tính toán các đặc trưng hành vi (số lượt view, cart, số sản phẩm/nhóm hàng/thương hiệu riêng biệt, giá, thời lượng tương tác, giờ, thứ trong tuần, loại sự kiện gần nhất).
  - Đuôi phiên ($rn > 5$) chỉ dùng duy nhất cho việc xác định nhãn: `label = 1` nếu có ít nhất 1 lần `purchase` sau mốc $K$, ngược lại `label = 0`.
- **Loại trừ rò rỉ (Strict Anti-Leakage):**
  - Loại bỏ hoàn toàn các phiên có `purchase` xuất hiện trong $K$ sự kiện đầu ($rn \le 5$).
  - Loại bỏ các phiên có tổng số sự kiện $n\_events \le K$.
  - Loại bỏ các phiên bắt đầu vào ngày lớn nhất của toàn bộ tập dữ liệu (Right-censoring).
  - Tuyệt đối không đưa `user_session`, `user_id`, `source_file`, `n_events`, hay bất kỳ thông tin nào từ đuôi phiên vào ma trận đặc trưng.
- **Phân chia tập dữ liệu theo thời gian (Time-based Split):**
  - Phân chia theo `session_start_date` (UTC) với tỷ lệ 70% Train (ngày 01-20), 15% Validation (ngày 21-24), 15% Test (ngày 25-29).
  - Tỷ lệ này sẽ được rà soát và chốt lại khi có báo cáo phân bố chính thức từ TV1.

### 3. Vị Trí Chuyển Đổi Sang Parquet Curated Của TV1 (T003)
Khi TV1 hoàn tất pipeline làm sạch dữ liệu Parquet (T003):
1. Mở file cấu hình `configs/config.yaml` (hoặc `configs/config.example.yaml`).
2. Tại khối `ml:`, trỏ đường dẫn dữ liệu đầu vào sang thư mục Parquet của TV1:
   ```yaml
   ml:
     # Đổi từ file CSV mẫu sang thư mục Parquet đã làm sạch:
     sample_csv: "data/processed/nov_2019_run_01/events"
   ```
3. **Không cần sửa đổi code:** Module `src/common/schema.py` đã cung cấp hàm `normalize_events(df)` có khả năng tự động nhận diện và chuẩn hóa dữ liệu từ cả 2 nguồn (CSV thô hoặc Parquet của TV1) cho đầu ra thống nhất (`event_time` Timestamp UTC, `event_date` Date UTC, `price` Double).

---

## 📖 Tài Liệu Tham Khảo Thêm
- [Kiến trúc hệ thống chi tiết](docs/architecture.md)
- [Quy chuẩn đặt tên và viết mã nguồn](docs/naming_and_code_standards.md)
- [Tiêu chí nghiệm thu đồ án](docs/acceptance_checklist.md)

# E-Commerce Clickstream Big Data Capstone

Hệ thống xử lý và phân tích dữ liệu lớn (Big Data Pipeline) cho dữ liệu clickstream thương mại điện tử REES46 (~100M+ sự kiện, Oct–Nov 2019), kết hợp **Batch Processing (PySpark, Hive)**, **Streaming (Apache Kafka, Spark Structured Streaming)**, và **Machine Learning (Spark MLlib)** để phân tích hành vi người dùng, phễu chuyển đổi và dự đoán khả năng mua hàng.

---

## 📐 Tổng Quan Kiến Trúc

Dự án theo mô hình **Lambda Architecture rút gọn**, chia thành ba tầng xử lý chính:

| Tầng | Luồng dữ liệu | Công nghệ |
|------|----------------|-----------|
| **Batch** | HDFS Raw CSV → Spark ETL → Parquet Curated → Hive → Analytics | PySpark, Hive |
| **Streaming** | Parquet Curated → Kafka Producer (history replay) → Kafka → Spark Structured Streaming | Kafka, Spark Streaming |
| **ML** | Parquet Curated → Session Features → Train/Test (time-based) → Spark MLlib | Spark MLlib |

> **Lưu ý quan trọng:**
> - Tầng Streaming sử dụng **dữ liệu lịch sử phát lại**, không phải sự kiện trực tiếp từ website.
> - Hệ thống **chưa có Serving Layer** hợp nhất kết quả batch và streaming.

Chi tiết kiến trúc: [docs/architecture.md](docs/architecture.md)

---

## 👥 Phân Công Thành Viên

### Người A – Data & Analytics

| Hạng mục | Thư mục chính |
|----------|---------------|
| Ingestion & ETL | `src/ingestion/`, `src/batch/etl/` |
| Raw / Curated HDFS | Quản lý dữ liệu trên HDFS VM-A |
| Hive DDL & Analytics | `sql/hive/`, `sql/analytics/`, `src/batch/analytics/` |
| Benchmark | `results/benchmark/` |

### Người B – Streaming & ML

| Hạng mục | Thư mục chính |
|----------|---------------|
| Git Repo & Standards (S4) | Repo root, `docs/`, `configs/` |
| Kafka Producer | `src/streaming/producer/` |
| Spark Structured Streaming | `src/streaming/consumer/` |
| ML Dataset & Features | `src/ml/dataset/`, `src/ml/features/` |
| ML Training & Evaluation | `src/ml/training/`, `src/ml/evaluation/` |
| Demo | `scripts/demo/` |

### Thư mục chung (phối hợp, review chéo)

`configs/`, `docs/`, `src/common/`, `tests/`, `README.md`

---

## 📁 Cấu Trúc Thư Mục Dự Án

```text
.
├── README.md
├── .gitignore
├── requirements.txt
├── configs/
│   └── config.example.yaml         # Cấu hình mẫu (KHÔNG chứa mật khẩu/token)
├── data/
│   ├── raw/                         # Dữ liệu tạm local; không commit
│   └── processed/                   # Dữ liệu tạm local; không commit
├── docs/
│   ├── architecture.md
│   ├── naming_and_code_standards.md
│   ├── acceptance_checklist.md
│   ├── data_contract.md             # Hợp đồng dữ liệu (Owner: A)
│   ├── env_check.md                 # Kiểm tra môi trường VM (Owner: B/S1)
│   ├── status.md                    # Bảng tiến độ task
│   └── issues.md                    # Quản lý lỗi (Owner: A/QA)
├── notebooks/                       # EDA / thử nghiệm
├── reports/                         # Báo cáo cuối kỳ, slides bảo vệ
├── results/
│   ├── benchmark/
│   ├── metrics/
│   └── figures/
├── scripts/
│   ├── ingest/
│   ├── run/
│   └── demo/
├── sql/
│   ├── hive/                        # DDL: database, table, partition
│   └── analytics/                   # Truy vấn funnel, session, brand
├── src/
│   ├── common/                      # SparkSession, schema, logger, config loader
│   ├── ingestion/
│   ├── batch/
│   │   ├── etl/
│   │   └── analytics/
│   ├── streaming/
│   │   ├── producer/
│   │   └── consumer/
│   ├── ml/
│   │   ├── dataset/
│   │   ├── features/
│   │   ├── training/
│   │   └── evaluation/
│   └── visualization/
│       ├── charts/
│       └── dashboards/
└── tests/
    ├── unit/
    ├── integration/
    └── data_quality/
```

---

## 🗂️ Phân Biệt Git Repo, Local Storage và HDFS

| Nơi lưu | Mục đích | Ví dụ đường dẫn |
|----------|----------|-----------------|
| **Git Repository** | Mã nguồn, SQL, cấu hình mẫu, tài liệu | `src/`, `sql/`, `configs/config.example.yaml` |
| **Local Storage** | Dữ liệu thử nghiệm nhỏ, kết quả tạm | `data/raw/`, `data/processed/`, `results/` |
| **HDFS** | Dữ liệu lớn và kết quả xử lý phân tán | `/data/raw/`, `/data/curated/`, `/data/ml/` |

> ⚠️ Thư mục `data/` trong Git Repo là **filesystem local**, KHÔNG phải HDFS. Dữ liệu HDFS không được đồng bộ qua GitHub.

---

## 🖥️ Kiến Trúc Hai VM

| | VM-A (Data & Analytics) | VM-B (Streaming & ML) |
|---|---|---|
| **HDFS** | Độc lập – giữ dữ liệu thô và kết quả ETL đầy đủ | Độc lập – nhận Parquet curated từ A |
| **Vai trò** | Ingestion, ETL, Hive, Batch Analytics, Benchmark | Kafka, Streaming, ML Pipeline, Demo |
| **NameNode** | Riêng (không chia sẻ với B) | Riêng (không chia sẻ với A) |

- Hai VM dùng **cùng quy ước đường dẫn HDFS** (ví dụ `/data/curated/events_cleaned`) nhưng **không chia sẻ NameNode**.
- Dữ liệu HDFS **không được đồng bộ thông qua GitHub**.
- Xem thêm chi tiết: [docs/architecture.md](docs/architecture.md)

---

## 🛠️ Yêu Cầu Môi Trường & Cài Đặt

### 1. Yêu Cầu Tiên Quyết

- **Python**: >= 3.9 (khuyến nghị 3.10 hoặc 3.11)
- **Java**: OpenJDK 8 hoặc 11 (bắt buộc cho Apache Spark)
- **Apache Spark**: >= 3.4.0
- **Apache Kafka**: >= 2.8.0 (cho tầng Streaming)
- **Apache Hive**: theo cấu hình VM
- **HDFS**: theo cấu hình VM

> Kiểm tra chi tiết phiên bản trên từng VM: [docs/env_check.md](docs/env_check.md)

### 2. Thiết Lập Môi Trường Ảo

```bash
# Tạo môi trường ảo Python
python -m venv .venv

# Kích hoạt trên Windows (PowerShell):
.venv\Scripts\Activate.ps1

# Kích hoạt trên Linux VM:
source .venv/bin/activate

# Cài đặt các gói phụ thuộc
pip install -r requirements.txt
```

### 3. Cấu Hình

Mỗi VM tạo file cấu hình riêng từ bản mẫu:

**Trên Linux VM:**
```bash
cp configs/config.example.yaml configs/config.yaml
```

**Trên Windows (PowerShell):**
```powershell
Copy-Item configs/config.example.yaml configs/config.yaml
```

Sau đó chỉnh sửa `configs/config.yaml` theo môi trường VM:
- Địa chỉ Kafka `bootstrap_servers` (có thể khác nhau giữa hai VM).
- Tài nguyên Spark `driver_memory`, `executor_memory` (tùy RAM của VM).
- Thông tin kết nối khác nếu cần.
- Đường dẫn HDFS logic giữ theo quy ước chung; HDFS sử dụng `fs.defaultFS` sẵn có của VM.

> ⚠️ `configs/config.yaml` **KHÔNG được commit** lên Git (đã cấu hình trong `.gitignore`).

---

## 🔄 Chuyển Dữ Liệu Từ VM-A Sang VM-B (S3)

Quy trình khái quát:

1. **A hoàn thành ETL** – dữ liệu sạch tại `/data/curated/events_cleaned` trên HDFS-A.
2. **A kiểm tra** – xác nhận số bản ghi, số partition, khoảng ngày.
3. **A xuất Parquet** từ HDFS sang filesystem local VM-A.
4. **Chuyển dữ liệu** sang VM-B qua VirtualBox Shared Folder hoặc phương án được nhóm cho phép.
5. **B kiểm tra checksum** – đối chiếu tính toàn vẹn.
6. **B đưa vào HDFS-B** tại đúng đường dẫn `/data/curated/events_cleaned`.
7. **B đối chiếu** – số dòng, số partition, schema và manifest trước khi sử dụng.

> ⚠️ Cẩn thận khi dùng `hdfs dfs -put`: đảm bảo không tạo sai một cấp thư mục (ví dụ `/data/curated/events_cleaned/events_cleaned`).
> ⚠️ Hướng dẫn chi tiết sẽ được cập nhật khi nhóm thực hiện bàn giao lần đầu.

Xem thêm yêu cầu manifest: [docs/data_contract.md](docs/data_contract.md)

---

## 🚀 Hướng Dẫn Chạy Chương Trình

> **Lưu ý:** Các module dưới đây là **lệnh dự kiến**. Trên nhánh `main` hiện tại, các file Python nghiệp vụ chưa được merge – chúng đang được phát triển trên các nhánh chức năng. Sử dụng sau khi module được triển khai và kiểm thử.

### 1. Nạp Dữ Liệu (Ingestion)

```bash
# Lệnh dự kiến – sử dụng sau khi module được triển khai
python -m src.ingestion.load_data
```

### 2. Batch ETL

```bash
# Lệnh dự kiến – sử dụng sau khi module được triển khai
python -m src.batch.etl.clean_events
```

> Đối với Spark job trên VM thực tế, xem xét sử dụng `spark-submit` với các tham số phù hợp.

### 3. Streaming

```bash
# Terminal 1: Kafka Producer – phát lại sự kiện lịch sử
# Lệnh dự kiến – sử dụng sau khi module được triển khai
python -m src.streaming.producer.event_replay

# Terminal 2: Spark Structured Streaming consumer
# Lệnh dự kiến – sử dụng sau khi module được triển khai
python -m src.streaming.consumer.spark_stream
```

### 4. Machine Learning

```bash
# Lệnh dự kiến – sử dụng sau khi module được triển khai
python -m src.ml.training.train_classifier
```

> ML code đang được phát triển trên nhánh `ml-model` và sẽ được merge vào `main` sau khi review.

### 5. Kiểm Thử

```bash
pytest tests/
```

---

## 🌿 Git Workflow

### Clone và thiết lập

```bash
git clone https://github.com/ngndng19xs/ecommerce-bigdata-capstone.git
cd ecommerce-bigdata-capstone

# Thiết lập Git identity
git config user.name "Tên của bạn"
git config user.email "email@example.com"
```

### Tạo feature branch

```bash
# Cập nhật main mới nhất
git checkout main
git pull origin main

# Tạo nhánh cho task
git checkout -b feature/<TaskID>-<mô-tả>
# Ví dụ: git checkout -b feature/B2-producer
```

### Commit và Push

```bash
# Commit theo mã task
git add <files>
git commit -m "[TaskID] mô tả ngắn"
# Ví dụ: git commit -m "[B2] add Kafka event replay producer"

# Push nhánh lên GitHub
git push origin feature/<TaskID>-<mô-tả>
```

### Tạo Pull Request

1. Truy cập GitHub → tạo Pull Request từ feature branch vào `main`.
2. Ghi rõ: task ID, danh sách file thay đổi, cách chạy, bằng chứng kiểm tra.
3. **Người A review code của B. Người B review code của A.**
4. Reviewer approve → Merge vào `main`.

> ⚠️ **Không push trực tiếp vào `main`.** Main phải được giữ ở trạng thái ổn định.

Quy tắc chi tiết: [docs/naming_and_code_standards.md](docs/naming_and_code_standards.md)

---

## 📊 Trạng Thái Triển Khai

| Hạng mục | Trạng thái | Ghi chú |
|----------|------------|---------|
| Cấu trúc Repository | ✅ Đã khởi tạo | Khung thư mục, `__init__.py`, `.gitkeep` |
| Cấu hình mẫu (`config.example.yaml`) | ✅ Đã cập nhật S4 | Bao gồm HDFS paths, Kafka, Hive, ML |
| Tài liệu (`docs/`) | ✅ Đã cập nhật S4 | architecture, standards, checklist, contracts |
| ETL (Ingestion, Batch) | ⬜ Đang phát triển | Nhánh của Người A |
| Streaming (Kafka, Spark) | ⬜ Đang phát triển | Nhánh của Người B |
| Machine Learning | 🔄 Có code trên nhánh `ml-model` | Chưa merge vào `main` |
| Demo end-to-end | ⬜ Chưa bắt đầu | Phụ thuộc hoàn thành Streaming + ML |
| Kiểm thử | 🔄 Có unit test trên nhánh `ml-model` | Chưa merge vào `main` |

> Bảng tiến độ chi tiết: [docs/status.md](docs/status.md)

---

## 📖 Tài Liệu Tham Khảo

- [Kiến trúc hệ thống chi tiết](docs/architecture.md)
- [Quy chuẩn đặt tên và viết mã nguồn](docs/naming_and_code_standards.md)
- [Tiêu chí nghiệm thu đồ án](docs/acceptance_checklist.md)
- [Hợp đồng dữ liệu](docs/data_contract.md)
- [Kiểm tra môi trường VM](docs/env_check.md)
- [Bảng tiến độ dự án](docs/status.md)
- [Quản lý lỗi](docs/issues.md)

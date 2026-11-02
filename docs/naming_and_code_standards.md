# Quy Chuẩn Đặt Tên và Viết Mã Nguồn

Tài liệu này xác định các quy tắc đặt tên file, biến, hàm, class, quy chuẩn viết code, Git workflow và review cho nhóm 2 người.

---

## 1. Python

### 1.1. Quy tắc đặt tên

| Đối tượng | Quy tắc | Ví dụ |
|---|---|---|
| File / Module | `snake_case` | `clean_events.py` |
| Hàm / Method | `snake_case` | `load_raw_data()` |
| Biến | `snake_case` | `event_count` |
| Hằng số | `UPPER_SNAKE_CASE` | `MAX_RETRY_COUNT` |
| Class | `PascalCase` | `SparkSessionBuilder` |
| Package / Thư mục | `snake_case` | `batch/etl/` |

### 1.2. Tiêu chuẩn viết code

- **Tuân thủ PEP 8** – Sử dụng `black` để format, `flake8` để lint.
- **Docstring**: Mỗi module và hàm quan trọng phải có docstring mô tả mục đích, tham số và giá trị trả về.

  ```python
  def clean_events(df: DataFrame, config: dict) -> DataFrame:
      """Làm sạch và chuẩn hóa DataFrame sự kiện clickstream.

      Args:
          df: DataFrame chứa sự kiện thô từ HDFS.
          config: Dictionary cấu hình từ config.yaml.

      Returns:
          DataFrame đã được làm sạch, sẵn sàng ghi Parquet.
      """
  ```

- **Import**: Sắp xếp theo thứ tự: standard library → third-party → local modules.
- **Không hardcode**: Đường dẫn HDFS, mật khẩu, token phải đọc từ file cấu hình hoặc biến môi trường.
- **Không hardcode mật khẩu hoặc token** vào mã nguồn.
- **Type hints**: Khuyến khích sử dụng type annotation cho hàm.
- **Ưu tiên module Python có thể chạy lại** thay vì chỉ dùng notebook.

### 1.3. PySpark Best Practices

1. **Không sử dụng `df.collect()` hoặc `df.toPandas()` trên tập dữ liệu lớn**: Chỉ dùng khi dữ liệu đã được tổng hợp thành kích thước nhỏ.
2. **Khai báo Explicit Schema**: Hạn chế dùng `inferSchema=True` trên production pipeline.
3. **Partitioning hợp lý**: Không phân mảnh partition quá nhỏ (tránh Small File Problem).
4. **Broadcast Join**: Sử dụng `broadcast(small_df)` khi join bảng Fact lớn với bảng Dimension nhỏ (< 50MB).

### 1.4. Logging thay vì print

```python
import logging
logger = logging.getLogger(__name__)

# Đúng
logger.info("Hoàn thành ETL. Số bản ghi: %d", record_count)

# Không dùng
print("done")
```

### 1.5. Quản lý cấu hình & secrets

- **TUYỆT ĐỐI KHÔNG commit**: mật khẩu, token, API keys, chuỗi kết nối database.
- **Sử dụng file cấu hình**: Đọc tham số từ `configs/config.yaml` và biến môi trường (`python-dotenv`).

---

## 2. SQL

### 2.1. Quy tắc đặt tên

| Đối tượng | Quy tắc | Ví dụ |
|---|---|---|
| File SQL | `snake_case` | `create_fact_events.sql` |
| Database | `snake_case` | `ecommerce_db` |
| Table (Fact) | `fact_` prefix | `fact_events` |
| Table (Dimension) | `dim_` prefix | `dim_sessions` |
| Table (Aggregate) | `agg_` prefix | `agg_funnel_metrics` |
| Column | `snake_case` | `event_time`, `user_id` |

### 2.2. Tiêu chuẩn viết SQL

- **Từ khóa SQL viết HOA**: `SELECT`, `FROM`, `WHERE`, `JOIN`, `GROUP BY`.
- **Phân biệt DDL và DML**:
  - `sql/hive/`: DDL (CREATE DATABASE, CREATE TABLE, ALTER TABLE).
  - `sql/analytics/`: Truy vấn phân tích (funnel, session, brand).
- **Không sử dụng `SELECT *`** trong tác vụ xử lý lớn; liệt kê rõ các cột cần thiết.
- **Tránh hardcode tên bảng** không đúng hợp đồng dữ liệu.
- **Mỗi file SQL phải có header comment** mô tả mục đích:

  ```sql
  -- ============================================================================
  -- File: create_fact_events.sql
  -- Purpose: Create Hive external table for cleaned clickstream events
  -- Database: ecommerce_db
  -- Source: /data/curated/events_cleaned (Parquet, partitioned by event_date)
  -- Owner: Person A
  -- ============================================================================
  ```

---

## 3. Git

### 3.1. Quy tắc đặt tên nhánh

Dành cho task mới:

| Loại nhánh | Mẫu | Ví dụ |
|---|---|---|
| Feature mới | `feature/<TaskID>-<mô-tả>` | `feature/S4-repo-standards` |
| Sửa lỗi | `bugfix/<TaskID>-<mô-tả>` | `bugfix/A3-null-session-fix` |
| Hotfix | `hotfix/<mô-tả>` | `hotfix/spark-config-fix` |

Một số ví dụ nhánh theo task:

- `feature/A3-etl`
- `feature/B2-producer`
- `feature/B4-stream-funnel`
- `feature/B7-ml-training`

> **Lưu ý:** Các nhánh cũ (`streaming`, `ml-model`, `analytics`, v.v.) không bắt buộc đổi tên.

### 3.2. Quy tắc commit message

```text
[<TaskID>] <hành-động> <mô-tả-ngắn>
```

**Ví dụ:**

- `[S4] update repository documentation and standards`
- `[A3] implement curated parquet ETL pipeline`
- `[B2] add Kafka event replay producer`
- `[B6] implement session feature engineering`
- `[B7] add logistic regression training pipeline`

**Quy tắc:**

- Mỗi commit tập trung vào **một mục đích rõ ràng**.
- Không trộn lẫn thay đổi nhiều task trong một commit.
- Viết bằng tiếng Anh, dùng động từ mệnh lệnh (imperative mood).

### 3.3. Quy trình Pull Request

1. **Không push trực tiếp vào `main`.**
2. Tạo feature branch từ `main` mới nhất.
3. Commit theo quy tắc trên.
4. Push feature branch lên GitHub.
5. Tạo Pull Request với mô tả:
   - Task ID và tên task.
   - Danh sách file thay đổi.
   - Cách chạy / kiểm tra.
   - Bằng chứng kiểm tra (screenshot, log output).
6. **Người A review code của B. Người B review code của A.**
7. Reviewer kiểm tra trước khi approve và merge.
8. **Main phải luôn ở trạng thái ổn định** – không merge code chưa kiểm tra.

### 3.4. Quy tắc bảo mật Git

- **Không commit**: dữ liệu lớn (CSV, Parquet), file cấu hình chứa mật khẩu/token, file `.env`.
- **Kiểm tra trước khi commit**: `git diff --cached` để xem những gì sắp commit.
- **Sử dụng `.gitignore`** đã cấu hình sẵn.
- Nếu vô tình commit file nhạy cảm, báo ngay cho nhóm để xử lý.

---

## 4. Quyền Sở Hữu Thư Mục

| Thư mục | Phụ trách chính | Ghi chú |
|---------|-----------------|---------|
| `src/ingestion/` | Người A | Nạp dữ liệu |
| `src/batch/` | Người A | ETL và analytics batch |
| `sql/` | Người A | Hive DDL và truy vấn analytics |
| Code benchmark | Người A | `src/batch/analytics/`, `results/benchmark/` |
| `src/streaming/` | Người B | Kafka producer và Spark consumer |
| `src/ml/` | Người B | Dataset, features, training, eval |
| `scripts/demo/` | Người B | Kịch bản demo end-to-end |
| `src/common/` | Chung | Review chéo khi sửa |
| `configs/` | Chung | Cấu hình mẫu dùng chung |
| `docs/` | Chung | Tài liệu dự án |
| `tests/` | Chung | Mỗi người viết test cho phần mình |
| `README.md` | Chung | Phối hợp cập nhật |

---

## 5. Kiểm Thử và Bàn Giao

### 5.1. Kiểm thử

- Sử dụng `pytest` cho unit test và integration test.
- Không commit dữ liệu lớn làm test fixture; sử dụng dữ liệu mẫu nhỏ.
- Không coi việc "job chạy xong không lỗi" là bằng chứng dữ liệu chính xác.
- Kiểm tra kết quả đầu ra: số dòng, schema, giá trị mẫu.
- Chạy thử trên dữ liệu mẫu khi có code thực tế.

### 5.2. Bàn giao dữ liệu giữa A và B

- Khi bàn giao Parquet từ A sang B, cần kèm:
  - **Manifest**: số dòng, số partition, khoảng ngày, schema version.
  - **Git commit hash** hoặc tag liên quan đến code sinh dữ liệu.
  - **Checksum** (nếu khả thi).
- Người nhận kiểm tra đối chiếu trước khi sử dụng.

### 5.3. Review chéo

- Không merge khi chưa được review.
- Reviewer kiểm tra: logic code, tuân thủ quy chuẩn, không hardcode, có docstring.
- Nếu phát hiện vấn đề, comment trên PR và yêu cầu sửa trước khi approve.

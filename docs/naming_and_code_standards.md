# Quy Chuẩn Đặt Tên & Viết Mã Nguồn (Naming & Coding Standards)

Tài liệu này xác định các quy tắc cốt lõi giúp các thành viên trong nhóm thống nhất phong cách lập trình, duy trì chất lượng mã nguồn cao và hạn chế xung đột khi hợp nhất mã.

---

## 1. Quy Chuẩn Đặt Tên (Naming Conventions)

### 1.1. Thư Mục và Tệp Tin
| Loại đối tượng | Quy tắc | Ví dụ |
|---|---|---|
| Module/File Python | `snake_case.py` | `clean_events.py`, `kafka_producer.py` |
| File cấu hình | `snake_case.yaml` / `.json` | `config.example.yaml`, `spark_conf.yaml` |
| File SQL | `số_thứ_tự_tên_nhiệm_vụ.sql` | `01_create_database.sql`, `02_funnel_analysis.sql` |
| File Notebooks | `tiền_tố_mục_đích.ipynb` | `01_eda_clickstream.ipynb`, `02_feature_test.ipynb` |
| File Shell Script | `snake_case.sh` | `run_batch_etl.sh`, `start_streaming.sh` |

### 1.2. Python Code Standards (PEP 8)
- **Tên Class**: `PascalCase` (ví dụ: `ClickstreamCleaner`, `SessionFeatureExtractor`).
- **Tên Hàm / Phương thức**: `snake_case` (ví dụ: `load_raw_data()`, `compute_conversion_rate()`).
- **Biến thường**: `snake_case` (ví dụ: `event_timestamp`, `user_session_id`).
- **Hằng số**: `UPPER_SNAKE_CASE` (ví dụ: `DEFAULT_DATE_FORMAT`, `MAX_RETRIES`).
- **Biến Private / Protected**: Bắt đầu bằng dấu gạch dưới `_protected_variable`.

### 1.3. SQL & Data Warehouse (Hive)
- **Từ khóa SQL**: Viết IN HOA (`SELECT`, `INSERT INTO`, `WHERE`, `GROUP BY`, `PARTITIONED BY`).
- **Tên Database / Schema**: `snake_case` (ví dụ: `ecommerce_db`).
- **Tiền tố bảng (Data Modeling)**:
  - Bảng sự kiện (Fact table): `fact_` (ví dụ: `fact_clickstream_events`).
  - Bảng chiều (Dimension table): `dim_` (ví dụ: `dim_users`, `dim_products`).
  - Bảng tổng hợp (Aggregate table): `agg_` (ví dụ: `agg_funnel_daily`, `agg_brand_performance`).

### 1.4. Apache Kafka
- **Topic**: Quy ước `domain.entity.datatype` (ví dụ: `ecommerce.clickstream.raw`, `ecommerce.events.deadletter`).
- **Consumer Group**: Tiền tố mô tả dịch vụ (ví dụ: `ecommerce-capstone-spark-streaming`).

---

## 2. Tiêu Chuẩn Viết Mã Nguồn (Code Standards)

### 2.1. Type Hinting & Docstrings
Tất cả các hàm và phương thức dùng chung trong `src/` bắt buộc phải có Type Annotations và Docstring:

```python
from pyspark.sql import DataFrame
from typing import List, Optional

def clean_clickstream_events(
    df: DataFrame, 
    required_columns: List[str],
    drop_null_sessions: bool = True
) -> DataFrame:
    """Làm sạch và chuẩn hóa dữ liệu sự kiện clickstream.

    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame chứa dữ liệu thô.
    required_columns : List[str]
        Danh sách các cột bắt buộc không được thiếu schema.
    drop_null_sessions : bool, optional
        Cờ loại bỏ các bản ghi không có user_session, mặc định là True.

    Returns
    -------
    DataFrame
        DataFrame đã được làm sạch và chuẩn hóa timestamp.
    """
    ...
```

### 2.2. PySpark Best Practices
1. **Không sử dụng `df.collect()` hoặc `df.toPandas()` trên tập dữ liệu lớn**: Chỉ dùng khi dữ liệu đã được tổng hợp (aggregate) thành kích thước nhỏ để vẽ đồ thị hoặc lưu metrics.
2. **Khai báo Explicit Schema**: Hạn chế dùng `inferSchema=True` trên production pipeline để tránh quét lại dữ liệu toàn bộ và gây chậm hiệu năng.
3. **Partitioning hợp lý**: Không phân mảnh partition quá nhỏ (tránh Small File Problem) và không để partition vượt quá 200MB - 1GB.
4. **Broadcast Join**: Sử dụng `broadcast(small_df)` khi kết hợp bảng Fact lớn với bảng Dimension nhỏ (< 50MB).

### 2.3. Quản Lý Cấu Hình & Secrets
- **TUYỆT ĐỐI KHÔNG commit**: Mật khẩu, token, API keys, chuỗi kết nối database vào git.
- **Sử dụng file cấu hình**: Đọc tham số từ file cấu hình `configs/` và nạp biến môi trường thông qua thư viện `python-dotenv`.

### 2.4. Logging Thay Vì Print
Sử dụng module `logging` chuẩn của Python để hỗ trợ phân cấp log và lưu vết:

```python
import logging

logger = logging.getLogger(__name__)

# Đúng
logger.info("Hoàn thành quá trình ETL. Số bản ghi đã xử lý: %d", record_count)
logger.warning("Phát hiện %d dòng thiếu thông tin sản phẩm.", missing_count)

# Không dùng
print("done")
```

---

## 3. Quy Ước Git & Commit (Git Flow)

### 3.1. Định Dạng Commit Message (Conventional Commits)
```
<type>(<scope>): <mô tả ngắn gọn bằng tiếng Anh hoặc tiếng Việt>
```
- `feat`: Tính năng mới (ví dụ: `feat(batch): add parquet deduplication step`).
- `fix`: Sửa lỗi (ví dụ: `fix(streaming): resolve timestamp parsing bug in kafka consumer`).
- `docs`: Cập nhật tài liệu (ví dụ: `docs: update architecture diagram and acceptance checklist`).
- `refactor`: Tái cấu trúc mã nguồn không làm thay đổi tính năng.
- `test`: Bổ sung hoặc chỉnh sửa unit test.
- `chore`: Cập nhật cấu hình, dependency (`requirements.txt`, `.gitignore`).

### 3.2. Quy Tắc Nhánh
- `main`: Nhánh ổn định cao nhất, dùng cho phiên bản báo cáo/bảo vệ.
- `develop`: Nhánh tích hợp các tính năng chính.
- `feature/<name>`: Nhánh làm việc của từng thành viên cho từng tính năng độc lập.

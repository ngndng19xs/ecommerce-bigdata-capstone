# Hợp Đồng Dữ Liệu (Data Contract)

> **Owner:** Người A. **Reviewer:** Người B.
>
> Tài liệu này là bản khung để Người A hoàn thiện trong Task S2.
> Các trường đánh dấu "TBD" hoặc "Chưa xác nhận" cần được cập nhật sau khi có kết quả khảo sát A2.

---

## 1. Nguồn Dữ Liệu

| Hạng mục | Giá trị |
|----------|---------|
| Dataset | REES46 eCommerce Behavior Data |
| Khoảng thời gian | October – November 2019 |
| Quy mô | ~100M+ sự kiện |
| Timezone | UTC |
| Nguồn | Kaggle |

---

## 2. Schema Gốc (9 trường)

| # | Tên trường | Kiểu dữ liệu | Mô tả |
|---|-----------|---------------|-------|
| 1 | `event_time` | Timestamp (UTC) | Thời điểm xảy ra sự kiện |
| 2 | `event_type` | String | Loại sự kiện: `view`, `cart`, `remove_from_cart`, `purchase` |
| 3 | `product_id` | Long | ID sản phẩm |
| 4 | `category_id` | Long | ID danh mục |
| 5 | `category_code` | String (nullable) | Mã danh mục phân cấp |
| 6 | `brand` | String (nullable) | Tên thương hiệu |
| 7 | `price` | Double | Giá sản phẩm |
| 8 | `user_id` | Long | ID người dùng |
| 9 | `user_session` | String (nullable) | ID phiên truy cập |

---

## 3. Quy Ước Thời Gian

- `event_time` theo **UTC**.
- `event_date` được tính từ UTC (`event_time` cast to date).
- Phân vùng Parquet theo `event_date`.

---

## 4. Quy Ước Xử Lý Dữ Liệu

### 4.1. Raw → Curated

- **Raw** giữ nguyên tại `/data/raw/ecommerce_events` để phục vụ đối chiếu.
- **Dữ liệu sạch** tại `/data/curated/events_cleaned`.
- **Bản ghi không hợp lệ** tại `/data/curated/_quarantine/invalid_events`.

### 4.2. Xử lý trường đặc biệt

| Trường | Quy tắc | Trạng thái |
|--------|---------|------------|
| `brand` null/rỗng | Chuẩn hóa theo quy tắc S2 | TBD – chờ xác nhận A2 |
| `user_session` null | Không dùng trong funnel và ML | TBD – chờ xác nhận A2 |
| `price` <= 0 hoặc anomaly | Quy tắc xử lý theo kết quả khảo sát | TBD – chờ xác nhận A2 |
| `category_code` null | Giữ nguyên hoặc gán "unknown" | TBD – chờ xác nhận A2 |

---

## 5. Thứ Tự Sự Kiện

| Trường | Giá trị |
|--------|---------|
| Sắp xếp chính | `event_time` |
| Sắp xếp phụ | `event_type_rank` → `event_sort_key` |

**Event type rank:**

| event_type | rank |
|-----------|------|
| `view` | 1 |
| `cart` | 2 |
| `remove_from_cart` | 3 |
| `purchase` | 4 |

> Không thay thế quy tắc thứ tự này bằng Kafka arrival time.

---

## 6. Kafka

| Hạng mục | Giá trị | Trạng thái |
|----------|---------|------------|
| Topic chính thức | TBD – cần xác nhận S2/B1 | ⬜ |
| Schema JSON | TBD | ⬜ |
| `schema_version` | TBD | ⬜ |
| Message key | TBD | ⬜ |
| Serialize timestamp | TBD | ⬜ |

> Hiện `config.example.yaml` có topic `ecommerce.clickstream.raw` và `ecommerce.clickstream.alerts`. Cần xác nhận xem có khác biệt với tài liệu S2 hay không.

---

## 7. Hive

| Hạng mục | Giá trị | Trạng thái |
|----------|---------|------------|
| Database | `ecommerce_db` | ⬜ Chờ A xác nhận |
| `fact_events` | External table → `/data/curated/events_cleaned` | ⬜ Chờ A xác nhận |
| `dim_sessions` | TBD | ⬜ |
| `agg_funnel_metrics` | TBD | ⬜ |

---

## 8. S3 Manifest (Bàn Giao Dữ Liệu A→B)

Mỗi lần bàn giao Parquet từ VM-A sang VM-B cần kèm manifest gồm:

| Trường | Mô tả |
|--------|-------|
| Source HDFS path | Đường dẫn nguồn trên HDFS-A |
| Destination HDFS path | Đường dẫn đích trên HDFS-B |
| Phạm vi dữ liệu | Loại dữ liệu (curated, analytics, etc.) |
| Số dòng | Tổng số bản ghi |
| Số partition | Số thư mục partition |
| Khoảng ngày UTC | `min(event_date)` – `max(event_date)` |
| Schema version | Phiên bản schema |
| Checksum | MD5/SHA256 nếu khả thi |
| Git commit hash/tag | Commit hoặc tag của code sinh dữ liệu |

---

## 9. Machine Learning

| Hạng mục | Giá trị | Trạng thái |
|----------|---------|------------|
| Prediction point | Sau N sự kiện đầu tiên của phiên | N = TBD |
| Đặc trưng | Chỉ dùng sự kiện trước prediction point | |
| Split strategy | Time-based | |
| Train end (UTC) | TBD | ⬜ |
| Validation end (UTC) | TBD | ⬜ |
| Test | Phần còn lại sau validation end | |

> N và ranh giới thời gian chưa chốt. Cập nhật sau khi hoàn thành A2 và thảo luận nhóm.


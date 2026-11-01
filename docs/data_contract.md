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

| **#** | **Tên trường**  | **Kiểu dữ liệu**  | **Nullable** | **Mô tả**                                                    |
| ----- | --------------- | ----------------- | ------------ | ------------------------------------------------------------ |
| 1     | `event_time`    | Timestamp (UTC)   | Không*       | Thời điểm xảy ra sự kiện                                     |
| 2     | `event_type`    | String            | Không*       | Loại sự kiện: `view`, `cart`, `remove_from_cart`, `purchase` |
| 3     | `product_id`    | Long              | Không*       | ID sản phẩm                                                  |
| 4     | `category_id`   | Long              | Không*       | ID danh mục                                                  |
| 5     | `category_code` | String            | Có           | Mã danh mục phân cấp                                         |
| 6     | `brand`         | String            | Có           | Tên thương hiệu                                              |
| 7     | `price`         | Double            | Không*       | Giá sản phẩm                                                 |
| 8     | `user_id`       | Long              | Không*       | ID người dùng                                                |
| 9     | `user_session`  | String            | Có           | ID phiên truy cập                                            |

**Quy ước Schema:**

- Schema trên là schema logic thống nhất của dữ liệu sau khi chuyển đổi kiểu dữ liệu.
- Dữ liệu Raw được giữ nguyên theo định dạng nguồn CSV.
- Khi đọc dữ liệu Raw, các trường có thể được tiếp nhận dưới dạng String; việc chuyển đổi sang kiểu dữ liệu chuẩn được thực hiện trong quá trình xử lý.
- `category_code`, `brand` và `user_session` được cho phép nhận giá trị NULL.
- Các trường còn lại được xác định là không NULL theo schema dự kiến. Việc kiểm tra và xử lý các giá trị không hợp lệ được thực hiện trong bước Data Quality và Cleaning.

---

## 3. Quy Ước Thời Gian

- `event_time` được thống nhất theo **UTC (Coordinated Universal Time)**.
- Kiểu dữ liệu chuẩn của `event_time` là `Timestamp`.
- Không tự động chuyển đổi `event_time` sang múi giờ địa phương trong quá trình xử lý.
- `event_date` được tạo bằng cách lấy phần ngày từ `event_time` theo UTC (`event_time` cast to date).
- Dữ liệu Parquet được phân vùng theo `event_date`.
- Định dạng phân vùng: `event_date=YYYY-MM-DD`.
- Apache Spark Session phải sử dụng timezone UTC để đảm bảo tính nhất quán khi chuyển đổi Timestamp và tạo `event_date`.

Cấu hình Spark:

```python
.config("spark.sql.session.timeZone", "UTC")
```

---

## 4. Quy Ước Xử Lý Dữ Liệu

### 4.1. Raw → Curated

- **Raw:** Giữ nguyên dữ liệu nguồn tại HDFS-VMA /data/raw/ecommerce_events để phục vụ đối chiếu và tái xử lý.
- **Curated:** Dữ liệu đã được xử lý và làm sạch sẽ được lưu tại HDFS-VMA /data/curated/events_cleaned.
- **Quarantine:** Bản ghi không hợp lệ được lưu tại HDFS-A /data/curated/_quarantine/invalid_events.
- HDFS trên VM-A là hệ thống lưu trữ nguồn do Người A phụ trách.
- VM-B sử dụng HDFS riêng làm hệ thống lưu trữ đích. Việc bàn giao dữ liệu từ VM-A sang VM-B được thực hiện thông qua cơ chế truyền file đã thống nhất giữa hai thành viên.

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


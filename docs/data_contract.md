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

## 2. Schema Dữ liệu

### 2.1. Schema Raw – 9 trường

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

### 2.2. Schema sau chuẩn hóa và xử lý ETL – 13 trường

Schema Version: curated-v1.0

| #  | Tên trường        | Kiểu dữ liệu | Spark Nullable | Mô tả                                                                                   |
| -- | ----------------- | ------------ | -------- | --------------------------------------------------------------------------------------- |
| 1  | `event_time`      | Timestamp    | Có       | Thời điểm xảy ra sự kiện theo UTC                                                       |
| 2  | `event_type`      | String       | Có       | Loại sự kiện: `view`, `cart`, `remove_from_cart`, `purchase`                            |
| 3  | `product_id`      | String       | Có       | ID sản phẩm                                                                             |
| 4  | `category_id`     | String       | Có       | ID danh mục                                                                             |
| 5  | `category_code`   | String       | Có       | Mã danh mục phân cấp                                                                    |
| 6  | `brand`           | String       | Có       | Tên thương hiệu                                                                         |
| 7  | `price`           | Double       | Có       | Giá sản phẩm                                                                            |
| 8  | `user_id`         | String       | Có       | ID người dùng                                                                           |
| 9  | `user_session`    | String       | Có       | ID phiên truy cập                                                                       |
| 10 | `reject_reason`   | String       | Có       | Lý do bản ghi bị từ chối (Reject), phục vụ kiểm tra dữ liệu không hợp lệ                |
| 11 | `event_type_rank` | Integer      | Có       | Thứ tự ưu tiên của loại sự kiện, phục vụ sắp xếp khi trùng timestamp                    |
| 12 | `event_sort_key`  | String       | Có       | Khóa SHA-256 dùng để phân định thứ tự khi `event_time` và `event_type_rank` trùng nhau  |
| 13 | `event_date`      | Date         | Có       | Ngày xảy ra sự kiện được trích xuất từ `event_time` theo UTC, dùng để phân vùng dữ liệu |

Quy ước Schema:
- Schema gồm 13 trường, trong đó 9 trường đầu được kế thừa từ dữ liệu Raw và 4 trường cuối được bổ sung trong quá trình ETL.
- Các trường `product_id`, `category_id`, `user_id` được chuẩn hóa thành String trong DataFrame xử lý.
- `event_time` sử dụng Timestamp theo UTC.
- `event_date` được tạo từ `event_time` và sử dụng làm khóa phân vùng Parquet.
- `reject_reason` được sử dụng để ghi nhận nguyên nhân Reject.
- `event_type_rank` và `event_sort_key` phục vụ thống nhất thứ tự sự kiện giữa Batch và Streaming.
- Các trường được khai báo `Nullable = Có` theo Spark Schema thực tế. Quy tắc kiểm tra NULL và phân loại Valid/Rejected được quy định riêng tại mục 4.2.

Quy ước Nullable:
- Cột Nullable phản ánh thuộc tính nullable được khai báo trong Spark Schema.
- Việc một trường có Nullable = Có không đồng nghĩa với việc dữ liệu NULL được chấp nhận trong Curated.
- Điều kiện chấp nhận NULL, thay thế giá trị NULL hoặc Reject bản ghi được quy định tại mục 4.2 – Xử lý trường đặc biệt.
- Các trường được tạo trong ETL phải tuân thủ điều kiện hợp lệ theo logic xử lý tương ứng.

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

**Quy định triển khai:**
- Raw phải được bảo toàn để phục vụ đối chiếu và tái xử lý.
- ETL phân loại dữ liệu thành hai nhóm: Curated (hợp lệ) và Quarantine (không hợp lệ).
- Không xóa âm thầm các bản ghi lỗi khỏi dữ liệu nguồn.
- Các bản ghi bị Reject phải có lý do cụ thể để phục vụ kiểm tra chất lượng dữ liệu.
- VM-A là hệ thống lưu trữ nguồn, do Người A phụ trách.
- VM-B sử dụng HDFS riêng làm hệ thống lưu trữ đích. Dữ liệu Curated được bàn giao từ VM-A sang VM-B thông qua Shared Folder.
- Đường dẫn giống nhau trên hai VM không đồng nghĩa dữ liệu được chia sẻ tự động.

### 4.2. Xử lý trường đặc biệt

| **Trường hợp** | **Quy tắc xử lý** | **Kết quả** |
|---|---|---|
| `brand` NULL/rỗng | Gán giá trị `Unknown` | Giữ trong Curated |
| `category_code` NULL | Giữ nguyên giá trị NULL | Giữ trong Curated |
| `user_session` NULL | Giữ bản ghi nhưng không sử dụng trong Funnel và ML | Giữ trong Curated |
| `event_type` không hợp lệ | Reject bản ghi | Quarantine |
| `event_time` không parse được | Reject bản ghi | Quarantine |
| `price` NULL | Reject bản ghi | Quarantine |
| `price` âm | Reject bản ghi | Quarantine |
| `price = 0` | Giữ nguyên bản ghi trong Curated | Giữ trong Curated |
| `user_id` NULL | Reject bản ghi | Quarantine |
| CSV sai định dạng | Ghi nhận riêng để kiểm tra, không loại bỏ âm thầm | Theo dõi riêng |
---

**Quyết định đối với `price = 0`:**

Qua khảo sát dữ liệu, nhóm ghi nhận:

- Tháng 10: 73.446 bản ghi có `price = 0`.
- Tháng 11: 196.218 bản ghi có `price = 0`.
- Tổng cộng: 269.664 bản ghi.

Do số lượng bản ghi có `price = 0` tương đối lớn và chưa có đủ bằng chứng xác định đây là dữ liệu không hợp lệ, nhóm quyết định giữ lại các bản ghi này trong Curated, không đưa vào Quarantine chỉ dựa trên điều kiện `price = 0`.

Đối với các giá trị `price` bất thường khác, chỉ áp dụng quy tắc Reject khi thỏa mãn các điều kiện đã thống nhất, không tự ý loại bỏ dữ liệu khi chưa có cơ sở.

## 5. Thứ Tự Sự Kiện

### 5.1. Quy tắc sắp xếp sự kiện

| Trường | Giá trị |
|--------|---------|
| Sắp xếp chính | `event_time` |
| Sắp xếp phụ | `event_type_rank` → `event_sort_key` |

### 5.2. Quy ước event_type_rank

Nhóm thống nhất bảng thứ tự ưu tiên như sau:

| event_type         | event_type_rank |
| ------------------ | --------------- |
| `view`             | 1               |
| `cart`             | 2               |
| `remove_from_cart` | 3               |
| `purchase`         | 4               |

> Không thay thế quy tắc thứ tự này bằng Kafka arrival time.

Trường `event_type_rank` được sử dụng để phân định thứ tự giữa các sự kiện có cùng `event_time`.

Lưu ý: Đây là quy ước xử lý của nhóm khi timestamp trùng nhau, không phải bằng chứng khẳng định thứ tự hành vi thực tế của người dùng.

### 5.3. Quy tắc tạo event_sort_key

Trong trường hợp nhiều sự kiện có cùng `event_time` và `event_type_rank`, nhóm sử dụng thuật toán SHA-256 để tạo `event_sort_key` từ 9 trường dữ liệu nguồn đã chuẩn hóa.

Danh sách 9 trường đầu vào:

```
event_time
event_type
product_id
category_id
category_code
brand
price
user_id
user_session
```

Công thức minh họa bằng PySpark:

```
from pyspark.sql import functions as F

source_columns = [
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session"
]

df = df.withColumn(
    "event_sort_key",
    F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(c).cast("string"),
                    F.lit("<NULL>") # Nên dùng F.lit() thay vì string thường
                )
                for c in source_columns
            ]
        ),
        256
    )
)
```

**Quy định:**
- Thứ tự 9 trường đầu vào phải được giữ cố định.
- Phải thống nhất cách chuẩn hóa dữ liệu trước khi tạo key.
- Giá trị NULL được biểu diễn thống nhất bằng `<NULL>`.
- Không thay đổi công thức tạo key giữa các lần chạy.
- Không sử dụng `monotonically_increasing_id()` vì giá trị có thể thay đổi theo cách Spark phân chia partition và thực thi.

Lưu ý: Đoạn code trên là công thức minh họa. Cách chuẩn hóa từng trường và biểu diễn giá trị trước khi tạo hash cần được hai thành viên thống nhất trong quá trình triển khai.

### 5.4. Quy tắc tính Funnel theo phiên

Nhóm thống nhất tính Funnel theo `user_session`, không tính dựa trên tổng số sự kiện.

Funnel gồm ba giai đoạn:

| Giai đoạn | Điều kiện                                                 |
| --------- | --------------------------------------------------------- |
| View      | Phiên có ít nhất một sự kiện `view`.                      |
| Cart      | Có sự kiện `cart` đầu tiên xuất hiện sau View đầu tiên.   |
| Purchase  | Có sự kiện `purchase` đầu tiên xuất hiện sau Cart hợp lệ. |

Các sự kiện được sắp xếp theo quy tắc tại mục 5.1 trước khi xác định các giai đoạn Funnel.

Ví dụ kiểm thử:

| Phiên | Chuỗi sự kiện          | Kết quả Funnel |
| ----- | ---------------------- | -------------- |
| S1    | View → Cart → Purchase | Đủ 3 giai đoạn |
| S2    | View → Cart            | View, Cart     |
| S3    | Cart → View → Purchase | Chỉ View       |
| S4    | View → Purchase → Cart | View, Cart     |
| S5    | View → Cart → Purchase | Đủ 3 giai đoạn |

Trong trường hợp S4, Purchase xảy ra trước Cart nên không được tính là chuyển đổi Purchase hợp lệ.

Các phiên có `user_session` NULL vẫn được giữ trong Curated nhưng không tham gia tính Funnel.

### 5.5. Công thức tính tỷ lệ chuyển đổi

Tỷ lệ chuyển đổi View → Cart:

$$ CR\_{View\rightarrow Cart} = \frac{\text{Số phiên có View và Cart sau View}} {\text{Số phiên có View}} $$

Tỷ lệ chuyển đổi Cart → Purchase:

$$ CR\_{Cart\rightarrow Purchase} = \frac{\text{Số phiên có đủ View, Cart, Purchase đúng thứ tự}} {\text{Số phiên có View và Cart sau View}} $$

Tỷ lệ chuyển đổi View → Purchase:

$$ CR\_{View\rightarrow Purchase} = \frac{\text{Số phiên có đủ 3 giai đoạn}} {\text{Số phiên có View}} $$

### 5.6. Quy tắc xác định ngày của phiên

Ngày của một phiên được xác định theo ngày UTC của sự kiện View đầu tiên trong phiên đó.

Quy tắc này được sử dụng thống nhất khi tổng hợp Funnel theo ngày hoặc tuần.

### 5.7. Quy định thống nhất giữa Batch và Streaming

Các thành viên phải sử dụng cùng một quy tắc xử lý:
- Cùng bảng `event_type_rank`.
- Cùng công thức tạo `event_sort_key`.
- Cùng cách sắp xếp `(event_time, event_type_rank, event_sort_key)`.
- Cùng cách xác định View đầu tiên, Cart đầu tiên sau View và Purchase đầu tiên sau Cart.
- Cùng quy tắc xác định ngày UTC của phiên.

Không sử dụng Kafka arrival time để thay thế `event_time` trong việc xác định thứ tự sự kiện.

Các trường hợp kiểm thử tối thiểu:

| Test case | Chuỗi sự kiện                 | Mục đích                               |
| --------- | ----------------------------- | -------------------------------------- |
| TC01      | View → Cart → Purchase        | Kiểm tra Funnel bình thường            |
| TC02      | Cart → View → Cart → Purchase | Kiểm tra thứ tự sự kiện đảo            |
| TC03      | View và Cart cùng timestamp   | Kiểm tra `event_type_rank`             |
| TC04      | View → Purchase → Cart        | Kiểm tra Purchase xuất hiện trước Cart |


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

Manifest là file metadata đi kèm mỗi lần bàn giao dữ liệu Parquet từ VM-A sang VM-B.

### 8.1. Cấu trúc Manifest

Mỗi lần bàn giao Parquet từ VM-A sang VM-B cần kèm manifest gồm:

| Trường                  | Mô tả                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------- |
| `dataset_name`          | Tên bộ dữ liệu được bàn giao                                                             |
| `snapshot_id`           | Mã định danh của phiên bản dữ liệu                                                       |
| `source_hdfs_path`      | Đường dẫn nguồn trên HDFS-A                                                              |
| `destination_hdfs_path` | Đường dẫn đích trên HDFS-B                                                               |
| `data_scope`            | Phạm vi dữ liệu được bàn giao (curated, analytics,...)                                   |
| `record_count`          | Tổng số bản ghi                                                                          |
| `partition_count`       | Số thư mục partition                                                                     |
| `min_event_date_utc`    | Ngày nhỏ nhất trong dữ liệu theo UTC                                                     |
| `max_event_date_utc`    | Ngày lớn nhất trong dữ liệu theo UTC                                                     |
| `schema_version`        | Phiên bản schema của bộ dữ liệu                                                          |
| `files`                 | Danh sách các file Parquet, gồm đường dẫn, dung lượng và checksum (SHA256) của từng file |
| `commit_hash`           | Git commit hash hoặc tag của phiên bản code sinh dữ liệu                                 |

### 8.2. Quy tắc tạo Checksum

Nhóm thống nhất sử dụng thuật toán SHA-256 để kiểm tra tính toàn vẹn của dữ liệu.

Đối với thư mục chứa nhiều file Parquet:

- Phải tính SHA-256 cho từng file.
- Lưu đầy đủ tên file, dung lượng và checksum trong Manifest.
- Không chỉ tính checksum cho một file đại diện.
- Sau khi truyền dữ liệu sang VM-B, thực hiện tính lại checksum và so sánh với giá trị trong Manifest.

Nếu checksum của file tại VM-B khác với VM-A, file đó cần được kiểm tra và truyền lại.

### 8.3. Quy trình kiểm tra dữ liệu trước và sau bàn giao

Trên VM-A – Máy gửi:

1. Xác định snapshot dữ liệu cần bàn giao.
2. Kiểm tra tổng số bản ghi bằng `df.count()`.
3. Kiểm tra số partition và danh sách file Parquet.
4. Xác định `min_event_time_utc` và `max_event_time_utc`.
5. Tính SHA-256 cho từng file Parquet.
6. Tạo Manifest và lưu cùng bộ dữ liệu bàn giao.

Trên VM-B – Máy nhận:

1. Nhận thư mục Parquet và Manifest.
2. Đưa dữ liệu vào HDFS-B theo đường dẫn đã thống nhất.
3. Đọc lại dữ liệu bằng Spark.
4. Kiểm tra tổng số bản ghi.
5. Kiểm tra số partition.
6. Tính lại SHA-256 và đối chiếu với Manifest.
7. Xác nhận dữ liệu đã được nhận đầy đủ và chính xác.

Điều kiện nghiệm thu:

| Tiêu chí        | Điều kiện đạt                                      |
| --------------- | -------------------------------------------------- |
| Record count    | Số bản ghi trên VM-A và VM-B khớp nhau             |
| Partition count | Số partition ngày khớp nhau                        |
| SHA-256         | Checksum của từng file khớp nhau                   |
| Schema          | Schema dữ liệu phù hợp với phiên bản đã thống nhất |
| Snapshot        | Đúng phiên bản dữ liệu được bàn giao               |
| Git commit      | Xác định được phiên bản code tạo dữ liệu           |

### 8.4. Phương thức và quy trình bàn giao dữ liệu

Nhóm thống nhất sử dụng Shared Folder của VirtualBox làm phương thức truyền dữ liệu giữa hai VM.

Quy trình bàn giao:

Bước 1. Trên VM-A:

Đưa dữ liệu Curated từ HDFS ra thư mục Shared Folder:

```
hdfs dfs -get \
/data/curated/events_cleaned \
/media/sf_CapstoneData/
```

Bước 2. Tạo và chuyển Manifest:

Lưu Manifest cùng thư mục dữ liệu Parquet trong Shared Folder.

Bước 3. Trên VM-B:

Đưa dữ liệu vào HDFS-B:

```
hdfs dfs -put \
/media/sf_CapstoneData/events_cleaned \
/data/curated/
```

Bước 4. Kiểm tra dữ liệu:

- Đọc lại Parquet trên VM-B.
- Đối chiếu record count và partition count.
- Kiểm tra checksum SHA-256.
- Xác nhận dữ liệu đúng snapshot.

Quy định:

- Không cần chuyển toàn bộ CSV Raw từ VM-A sang VM-B.
- Chỉ bàn giao dữ liệu đã xử lý và các file metadata cần thiết.
- Không ghi đè snapshot cũ khi chưa kiểm tra phiên bản.
- Mỗi lần bàn giao phải có Manifest tương ứng.


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


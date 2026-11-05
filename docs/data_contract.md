# Hợp Đồng Dữ Liệu (Data Contract)

> **Owner:** Người A (Data Engineering & Analytics)<br>
> **Reviewer:** Người B (Streaming & Machine Learning)<br>
> **Trạng thái:** 🟡 **BẢN HỢP NHẤT CHỜ DUYỆT (READY FOR A/B REVIEW – MỐC H04)**<br>
> **Phiên bản:** `v1.0-draft-integrated` (Hợp nhất kết quả Task S2-01 Kafka Contract và Task S2-02 ML Contract)<br>
> **Tài liệu tham chiếu:** [kafka_contract_DRAFT.md](./kafka_contract_DRAFT.md) | [ml_contract_DRAFT.md](./ml_contract_DRAFT.md) | `docs/MANIFEST_OCT.md` (trên nhánh `data-engineering-storage`)

---

## 1. Nguồn Dữ Liệu

| Hạng mục | Giá trị |
|---|---|
| Dataset | REES46 eCommerce Behavior Data |
| Khoảng thời gian | October – November 2019 |
| Quy mô | ~100M+ sự kiện |
| Timezone | UTC |
| Nguồn | Kaggle |

---

## 2. Schema Dữ Liệu

### 2.1. Schema Raw – 9 trường

**Schema Version:** `raw-v1.0`

| # | Tên trường | Kiểu dữ liệu logic | Nullable | Mô tả |
|---|---|---|---|---|
| 1 | `event_time` | Timestamp (UTC) | Không* | Thời điểm xảy ra sự kiện |
| 2 | `event_type` | String | Không* | Loại sự kiện: `view`, `cart`, `remove_from_cart`, `purchase` |
| 3 | `product_id` | Long | Không* | ID sản phẩm |
| 4 | `category_id` | Long | Không* | ID danh mục |
| 5 | `category_code` | String | Có | Mã danh mục phân cấp |
| 6 | `brand` | String | Có | Tên thương hiệu |
| 7 | `price` | Double | Không* | Giá sản phẩm |
| 8 | `user_id` | Long | Không* | ID người dùng |
| 9 | `user_session` | String | Có | ID phiên truy cập |

**Quy ước Schema Raw:**
- Schema trên là schema logic nguồn của dữ liệu CSV thô ban đầu. Ba trường định danh `product_id`, `category_id`, `user_id` trong dữ liệu thô có định dạng số nguyên lớn (Long).
- Dữ liệu Raw được lưu trữ và bảo toàn nguyên trạng theo định dạng CSV nguồn tại HDFS VM-A (`/data/raw/ecommerce_events`).
- Khi đọc dữ liệu Raw vào Spark, các trường có thể được tiếp nhận dưới dạng String trước khi chuyển đổi kiểu dữ liệu chuẩn trong quá trình xử lý ETL.
- `category_code`, `brand` và `user_session` được phép nhận giá trị NULL trong dữ liệu nguồn.
- Các trường đánh dấu `Không*` được kỳ vọng không có giá trị NULL. Việc phát hiện, kiểm tra và xử lý các dòng dữ liệu không hợp lệ được thực hiện trong bước Data Quality và Cleaning tại tầng ETL (Mục 4.2).

### 2.2. Schema Sau Chuẩn Hóa và Xử Lý ETL – 13 trường

**Schema Version:** `curated-v1.0`

| # | Tên trường | Kiểu dữ liệu Curated | Spark Nullable | Mô tả |
|---|---|---|---|---|
| 1 | `event_time` | Timestamp | Có | Thời điểm xảy ra sự kiện theo UTC |
| 2 | `event_type` | String | Có | Loại sự kiện: `view`, `cart`, `remove_from_cart`, `purchase` |
| 3 | `product_id` | String | Có | ID sản phẩm (chuẩn hóa thành String trong Curated) |
| 4 | `category_id` | String | Có | ID danh mục (chuẩn hóa thành String trong Curated) |
| 5 | `category_code` | String | Có | Mã danh mục phân cấp |
| 6 | `brand` | String | Có | Tên thương hiệu (đã chuẩn hóa `"Unknown"` nếu khuyết) |
| 7 | `price` | Double | Có | Giá sản phẩm (Double $\ge 0.0$) |
| 8 | `user_id` | String | Có | ID người dùng (chuẩn hóa thành String trong Curated) |
| 9 | `user_session` | String | Có | ID phiên truy cập |
| 10 | `reject_reason` | String | Có | Lý do bản ghi bị từ chối (Reject); rỗng/NULL trong bản ghi hợp lệ |
| 11 | `event_type_rank` | Integer | Có | Thứ tự ưu tiên của loại sự kiện (1..4) khi trùng timestamp |
| 12 | `event_sort_key` | String | Có | Khóa băm SHA-256 64 ký tự hex dùng làm tie-breaker tất định |
| 13 | `event_date` | Date | Có | Ngày xảy ra sự kiện theo UTC, dùng làm partition key Parquet |

**Quy ước Schema Curated:**
- Schema gồm 13 trường: 9 trường đầu kế thừa từ dữ liệu nguồn Raw và 4 trường sau được bổ sung trong quá trình ETL (`reject_reason`, `event_type_rank`, `event_sort_key`, `event_date`).
- **Chuẩn hóa ba trường định danh:** Các trường `product_id`, `category_id`, `user_id` được chuẩn hóa thành kiểu `String` trong Curated DataFrame và Curated Parquet. Đây là căn cứ bắt buộc để đồng bộ kiểu chuỗi sang Kafka JSON Schema (Mục 6.3).
- **Trường `reject_reason`:** Xuất hiện trong cấu trúc DataFrame đầu ra của ETL và được ghi tại bảng Quarantine (`/data/curated/_quarantine/invalid_events`). Đối với các bản ghi hợp lệ ghi vào Curated Parquet, trường này có giá trị rỗng hoặc NULL.
- **Trường `event_date`:** Được trích xuất từ `event_time` theo UTC (`to_date(event_time)`) và sử dụng làm khóa phân vùng thư mục vật lý (`event_date=YYYY-MM-DD`). Khi Spark đọc dataset từ thư mục gốc `/data/curated/events`, cột `event_date` được tự động khôi phục vào DataFrame.
- **Hai trường thứ tự `event_type_rank` và `event_sort_key`:** Được tính toán tập trung một lần duy nhất tại tầng ETL của Người A và lưu trực tiếp trong Curated Parquet. Các thành phần downstream (Batch Conversion Funnel, Kafka Producer, Spark Structured Streaming, ML) đọc trực tiếp từ dữ liệu, không tự tính lại.
- **Quy ước Nullable trong Spark Schema:** Thuộc tính `Spark Nullable = Có` phản ánh metadata khai báo của DataFrame trong Spark. Quy tắc chấp nhận NULL hay Reject bản ghi đối với từng trường dữ liệu được kiểm soát nghiêm ngặt theo các điều kiện tại Mục 4.2.
- *Điểm cần lưu ý về kiểm tra NULL đối với ID:* Pipeline ETL (`etl_oct.py`) xác nhận `user_id NULL` nằm trong `invalid_condition` và bị Reject. Tuy nhiên, `product_id NULL` và `category_id NULL` hiện không nằm trong `invalid_condition` của ETL. Điều này dẫn đến **rủi ro tiềm ẩn: nếu dữ liệu nguồn có bản ghi mang giá trị NULL ở hai trường này thì chúng có thể lọt vào Curated Parquet**, tạo ra sự không đồng nhất với Kafka JSON Schema (vốn quy định ba trường ID là chuỗi bắt buộc, không được mang giá trị NULL). Điểm này được ghi nhận tại bảng Pending Decisions (Mục 10.1, Quyết định #11) để Người A kiểm tra thực tế dữ liệu và đưa ra quyết định chính thức; hệ thống giữ nguyên các quy tắc làm sạch dữ liệu (Data Cleaning Rules) đã chốt của Người A.

---

## 3. Quy Ước Thời Gian

- `event_time` được thống nhất tuyệt đối theo **UTC (Coordinated Universal Time)**.
- Kiểu dữ liệu chuẩn của `event_time` trong Parquet là `TimestampType`.
- Không chuyển đổi `event_time` sang múi giờ địa phương (ví dụ `+07:00`) trong quá trình lưu trữ, xử lý ETL, phát Kafka hay huấn luyện ML.
- `event_date` được tạo bằng cách lấy phần ngày từ `event_time` theo UTC (`cast(event_time as date)`).
- Dữ liệu Parquet được phân vùng theo `event_date` với định dạng: `event_date=YYYY-MM-DD`.
- Mọi Spark Session (Batch ETL, Funnel Analytics, Kafka Producer, ML Feature Extraction) bắt buộc cấu hình múi giờ UTC:

```python
.config("spark.sql.session.timeZone", "UTC")
```

---

## 4. Quy Ước Xử Lý Dữ Liệu

### 4.1. Phân Tầng Dữ Liệu: Raw → Curated → Quarantine

| Đường dẫn HDFS | Vai trò | Định dạng & Cấu trúc | Thành phần sử dụng |
|---|---|---|---|
| `/data/raw/ecommerce_events` | Dữ liệu thô nguyên bản | CSV gốc từ nguồn Kaggle | Lưu trữ đối chiếu, tái xử lý ETL |
| `/data/curated/events` | **Curated Production chính thức** | Snappy Parquet, phân vùng `event_date=YYYY-MM-DD` | Batch Analytics (Funnel), Kafka Producer, ML Pipeline |
| `/data/curated/events_cleaned` | Curated Intermediate (Trung gian) | Parquet đã làm sạch nhưng chưa phân vùng | Bảng trung gian trong pipeline ETL |
| `/data/curated/_quarantine/invalid_events` | Dữ liệu lỗi (Quarantine) | Parquet kèm lý do lỗi `reject_reason` | Kiểm tra chất lượng dữ liệu (Data Quality Audit) |

**Quy định triển khai:**
- **Nguồn dữ liệu sản xuất:** Toàn bộ các tác vụ sản xuất downstream gồm Batch Funnel Analytics (Người A), Kafka Producer (Người B) và Machine Learning Feature Extraction (Người B) bắt buộc phải đọc từ **`/data/curated/events`** (dữ liệu đã phân vùng theo `event_date`).
- Đường dẫn `/data/curated/events_cleaned` là dữ liệu trung gian trong quá trình thực thi ETL của Người A, không dùng làm đầu vào chính thức cho downstream.
- **Xác minh đường dẫn Quarantine:**
  - **Đã xác minh bằng code ETL (`etl_oct.py`):** Pipeline ETL thực tế (`rejected_output_path = "/data/curated/_quarantine/invalid_events"`) và Manifest (`MANIFEST_OCT.md`) xác nhận dữ liệu bị từ chối được ghi tại `/data/curated/_quarantine/invalid_events`.
  - **Chưa được Người A phê duyệt chính thức:** Tài liệu phân công A3 trước đây có đề cập tên `/data/curated/events_rejected`. Điểm khác biệt này được ghi nhận tại Mục 10, mục #12 (Pending, chờ Người A xác nhận làm chuẩn dứt khoát).
  - **Trong thời gian chờ xác nhận:** Sử dụng đường dẫn đã xác minh bằng code (`/data/curated/_quarantine/invalid_events`) trong tài liệu này, có chú thích rõ trạng thái.
- Raw CSV phải được bảo toàn nguyên vẹn, không chỉnh sửa trực tiếp trên file nguồn.
- Không xóa âm thầm các bản ghi lỗi khỏi luồng dữ liệu; mọi bản ghi vi phạm quy tắc làm sạch đều phải được phân loại và ghi vào Quarantine kèm lý do lỗi cụ thể.
- Hai máy ảo VM-A và VM-B vận hành cụm HDFS độc lập. Dữ liệu Curated được bàn giao từ VM-A sang VM-B thông qua VirtualBox Shared Folder kèm Manifest và Checksum SHA-256 (Mục 8).

### 4.2. Xử Lý Trường Đặc Biệt (Data Cleaning Rules)

| Trường hợp | Quy tắc xử lý | Phân loại kết quả | Ghi chú nghiệp vụ |
|---|---|---|---|
| `brand` NULL hoặc chuỗi rỗng | Gán giá trị mặc định `"Unknown"` | Giữ trong Curated | Áp dụng cho cả Curated và Kafka message |
| `category_code` NULL | Giữ nguyên giá trị NULL (`null`) | Giữ trong Curated | Cho phép khuyết taxonomy phân cấp |
| `user_session` NULL | Giữ nguyên trong Curated Parquet | Giữ trong Curated | **Bị loại khỏi** Funnel Analytics và ML Pipeline |
| `event_type` ngoài danh mục | Reject bản ghi | Đưa vào Quarantine | Chỉ chấp nhận: `view`, `cart`, `remove_from_cart`, `purchase` |
| `event_time` không parse được | Reject bản ghi | Đưa vào Quarantine | Lỗi định dạng thời gian |
| `price` NULL | Reject bản ghi | Đưa vào Quarantine | Thiếu giá trị giá niêm yết |
| `price < 0` (Giá âm) | Reject bản ghi | Đưa vào Quarantine | Sai lệch nghiệp vụ giá sản phẩm |
| `price = 0` (Giá bằng 0) | **Hợp lệ**, giữ nguyên trong Curated | Giữ trong Curated | Ghi nhận sản phẩm tặng kèm/khuyến mãi |
| `user_id` NULL | Reject bản ghi | Đưa vào Quarantine | Bắt buộc phải có mã định danh người dùng |
| Dòng CSV sai định dạng (malformed) | Ghi nhận riêng trong báo cáo kiểm kê | Theo dõi riêng | Đảm bảo tính toàn vẹn kiểm kê dữ liệu |

**Quyết định đối với `price = 0`:**
Khảo sát dữ liệu thực tế ghi nhận 73.446 bản ghi trong tháng 10 và 196.218 bản ghi trong tháng 11 có `price = 0` (tổng cộng 269.664 bản ghi). Do số lượng bản ghi tương đối lớn và phản ánh các hành vi nhận quà tặng, vật phẩm khuyến mại trong sàn thương mại điện tử, nhóm thống nhất giữ lại các bản ghi này trong Curated, không đưa vào Quarantine.

---

## 5. Thứ Tự Sự Kiện

### 5.1. Quy Tắc Sắp Xếp Sự Kiện Chuẩn

Mọi thành phần trong hệ thống khi cần xác định thứ tự tuần tự của các sự kiện trong cùng một phiên (`user_session`) bắt buộc phải sắp xếp theo bộ ba tiêu chí:

```text
event_time UTC ASC  -->  event_type_rank ASC  -->  event_sort_key ASC
```

| Tiêu chí | Vai trò | Quy ước kỹ thuật |
|---|---|---|
| 1. Sắp xếp chính | `event_time` | Timestamp theo UTC tăng dần |
| 2. Sắp xếp phụ | `event_type_rank` | Số nguyên 1..4 ưu tiên loại sự kiện khi trùng timestamp |
| 3. Tie-breaker cuối cùng | `event_sort_key` | Chuỗi SHA-256 64 ký tự hex chữ thường xác định thứ tự tất định |

### 5.2. Quy Ước event_type_rank

Nhóm thống nhất bảng trọng số thứ tự ưu tiên khi các sự kiện xảy ra trong cùng một giây (`event_time` trùng nhau):

| `event_type` | `event_type_rank` | Ý nghĩa nghiệp vụ |
|---|---|---|
| `view` | **1** | Xem sản phẩm |
| `cart` | **2** | Thêm sản phẩm vào giỏ hàng |
| `remove_from_cart` | **3** | Xóa sản phẩm khỏi giỏ hàng |
| `purchase` | **4** | Mua hàng |

> **Lưu ý nghiệp vụ:** Đây là quy ước chuẩn hóa kỹ thuật của hệ thống để phân định thứ tự xử lý khi timestamp trùng nhau, không phải bằng chứng khẳng định thứ tự hành vi thực tế tuyệt đối của người dùng. Tuyệt đối không thay thế quy tắc này bằng thời điểm nhận tin (Kafka arrival time).

### 5.3. Quy Tắc Tạo và Lưu Trữ event_sort_key

Trong trường hợp nhiều sự kiện có cùng `event_time` và `event_type_rank`, hệ thống sử dụng thuật toán băm SHA-256 để tạo `event_sort_key` từ cấu trúc dữ liệu chuẩn hóa gồm 9 trường nguồn.

Danh sách 9 trường đầu vào cấu trúc theo đúng thứ tự và chuẩn hóa:
1. `event_time`: định dạng `yyyy-MM-dd HH:mm:ss` (chuẩn hóa qua `date_format`)
2. `event_type`: chuỗi đã xử lý `trim`
3. `product_id`: chuỗi đã xử lý `trim`
4. `category_id`: chuỗi đã xử lý `trim`
5. `category_code`: chuỗi đã xử lý `trim`
6. `brand`: chuỗi đã xử lý `trim`
7. `price`: chuỗi số thực được ép kiểu chuẩn hóa `cast("decimal(20,6)").cast("string")`
8. `user_id`: chuỗi đã xử lý `trim`
9. `user_session`: chuỗi đã xử lý `trim`

**Thuật toán thực thi chính thức tại pipeline ETL (`etl_oct.py`):**

Toàn bộ 9 trường trên được đóng gói vào một `struct`, chuyển đổi sang chuỗi JSON định dạng chuẩn (giữ nguyên trường null thông qua cấu hình `{"ignoreNullFields": "false"}`), sau đó áp dụng hàm băm mật mã học SHA-256 (`sha2(..., 256)`):

```python
from pyspark.sql.functions import col, struct, date_format, trim, sha2, to_json

normalized_fields = struct(
    date_format(
        col("event_time"),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("event_time"),
    trim(col("event_type")).alias("event_type"),
    trim(col("product_id")).alias("product_id"),
    trim(col("category_id")).alias("category_id"),
    trim(col("category_code")).alias("category_code"),
    trim(col("brand")).alias("brand"),
    col("price").cast("decimal(20,6)").cast("string").alias("price"),
    trim(col("user_id")).alias("user_id"),
    trim(col("user_session")).alias("user_session")
)

valid_df = valid_df.withColumn(
    "event_sort_key",
    sha2(
        to_json(
            normalized_fields,
            {"ignoreNullFields": "false"}
        ),
        256
    )
)
```

**Quy định bắt buộc:**
- `event_sort_key` là chuỗi SHA-256 lowercase hex có độ dài đúng 64 ký tự.
- `event_sort_key` là một tie-breaker tất định dựa trên nội dung dữ liệu; đây **không phải UUID** và không bảo đảm tính duy nhất tuyệt đối nếu xuất hiện hai bản ghi trùng lặp hoàn toàn 100% cả 9 trường nguồn.
- **Không dùng công thức nối chuỗi thủ công:** Tuyệt đối không dùng công thức `concat_ws("||", ...)` vì sẽ tạo ra giá trị băm không tương thích với dữ liệu đã lưu trong Curated Parquet.
- **Tính toán tập trung tại ETL:** Cột `event_sort_key` và `event_type_rank` đã được tính toán sẵn và lưu trữ trực tiếp trong các file Curated Parquet bởi pipeline ETL của Người A.
- **Kế thừa tại downstream:**
  - **Kafka Producer**, **Batch Funnel Analytics** và **Machine Learning**: Đọc trực tiếp hai cột `event_type_rank` và `event_sort_key` từ Curated Parquet (`/data/curated/events`), tuyệt đối không tự tính lại.
  - **Streaming Consumer / Spark Structured Streaming**: Đọc trực tiếp hai trường này từ Kafka JSON payload (`ecommerce_events`) do Producer đã phát sang, kế thừa nguyên vẹn giá trị đã được chuẩn hóa bởi ETL, tuyệt đối không tự tính lại.
- **Thống nhất bộ ba ordering:** Bộ ba `(event_time ASC, event_type_rank ASC, event_sort_key ASC)` là tiêu chí sắp xếp tuần tự chính thức duy nhất. Tuyệt đối không đưa `product_id` hay hàm tự tăng `monotonically_increasing_id()` vào bộ ba sắp xếp.

### 5.4. Quy Tắc Tính Funnel Theo Phiên

Funnel chuyển đổi được tính toán theo từng phiên người dùng (`user_session`), không tính dựa trên tổng số sự kiện thô.

Mô hình Funnel chuẩn gồm ba giai đoạn:

| Giai đoạn | Điều kiện kích hoạt |
|---|---|
| **View** | Phiên có ít nhất một sự kiện `view`. |
| **Cart** | Có sự kiện `cart` đầu tiên xuất hiện **sau** sự kiện `view` đầu tiên của phiên. |
| **Purchase** | Có sự kiện `purchase` đầu tiên xuất hiện **sau** sự kiện `cart` hợp lệ đầu tiên. |

Các sự kiện trong phiên bắt buộc phải được sắp xếp theo quy tắc Mục 5.1 trước khi phân tích chuỗi hành vi.

Ví dụ minh họa tính hợp lệ:

| Phiên | Chuỗi sự kiện diễn ra | Kết quả phân loại Funnel |
|---|---|---|
| S1 | View $\rightarrow$ Cart $\rightarrow$ Purchase | Đạt đủ 3 giai đoạn (View, Cart, Purchase) |
| S2 | View $\rightarrow$ Cart | Đạt 2 giai đoạn (View, Cart) |
| S3 | Cart $\rightarrow$ View $\rightarrow$ Purchase | Chỉ tính giai đoạn View (Cart xuất hiện trước View) |
| S4 | View $\rightarrow$ Purchase $\rightarrow$ Cart | Đạt View và Cart; Purchase không hợp lệ vì xảy ra trước Cart |
| S5 | View $\rightarrow$ Cart $\rightarrow$ Purchase | Đạt đủ 3 giai đoạn |

Các phiên có `user_session` mang giá trị NULL vẫn được lưu trong Curated Parquet nhưng bị loại bỏ hoàn toàn khỏi các phân tích Funnel.

### 5.5. Công Thức Tính Tỷ Lệ Chuyển Đổi (Conversion Rates)

Tỷ lệ chuyển đổi View $\rightarrow$ Cart:

$$CR_{\text{View}\rightarrow\text{Cart}} = \frac{\text{Số phiên có View và có Cart hợp lệ sau View}}{\text{Số phiên có View}}$$

Tỷ lệ chuyển đổi Cart $\rightarrow$ Purchase:

$$CR_{\text{Cart}\rightarrow\text{Purchase}} = \frac{\text{Số phiên có đủ View, Cart hợp lệ và Purchase hợp lệ sau Cart}}{\text{Số phiên có View và có Cart hợp lệ sau View}}$$

Tỷ lệ chuyển đổi toàn phễu View $\rightarrow$ Purchase:

$$CR_{\text{View}\rightarrow\text{Purchase}} = \frac{\text{Số phiên có đủ 3 giai đoạn hợp lệ theo thứ tự}}{\text{Số phiên có View}}$$

### 5.6. Quy Tắc Xác Định Ngày Của Phiên (Funnel)

- Trong phân tích Funnel, ngày của một phiên được xác định theo **ngày UTC của sự kiện `view` đầu tiên** xuất hiện trong phiên đó.
- Quy tắc này được áp dụng thống nhất cho các báo cáo phân tích tổng hợp Funnel theo ngày hoặc theo tuần.

### 5.7. Quy Định Thống Nhất Giữa Batch và Streaming

Tất cả các thành phần downstream phải sử dụng thống nhất hai khóa `event_type_rank` và `event_sort_key` do pipeline ETL tạo ra, với cơ chế đọc phù hợp từng tầng:
- **Kafka Producer, Batch Analytics, Machine Learning:** Đọc trực tiếp từ Curated Parquet (`/data/curated/events`).
- **Streaming Consumer / Spark Structured Streaming:** Đọc trực tiếp từ Kafka JSON payload trên topic `ecommerce_events`.
- **Quy tắc sắp xếp chung:** Mọi thành phần đều sắp xếp nghiệp vụ tuân thủ bộ ba `(event_time UTC, event_type_rank, event_sort_key)`, tuyệt đối không tự tính lại hai khóa này tại bất kỳ tầng nào.

Bộ trường hợp kiểm thử tối thiểu để kiểm tra tính nhất quán giữa Batch và Streaming:

| Mã test | Chuỗi sự kiện đầu vào | Mục đích kiểm thử |
|---|---|---|
| TC01 | View $\rightarrow$ Cart $\rightarrow$ Purchase | Kiểm tra Funnel bình thường chuẩn 3 bước |
| TC02 | Cart $\rightarrow$ View $\rightarrow$ Cart $\rightarrow$ Purchase | Kiểm tra xử lý sự kiện Cart đảo trước View |
| TC03 | View và Cart có cùng timestamp | Kiểm tra phân định bằng `event_type_rank` |
| TC04 | View $\rightarrow$ Purchase $\rightarrow$ Cart | Kiểm tra loại trừ Purchase xuất hiện trước Cart |

---

## 6. Hợp Đồng Luồng Sự Kiện Kafka (Kafka Event Contract)

Mục này xác lập hợp đồng dữ liệu chính thức cho luồng sự kiện phát vào Apache Kafka (Task P2-B-S2-01), đảm bảo tính đồng bộ hoàn toàn giữa tầng Batch ETL của Người A và tầng Streaming / ML của Người B.

### 6.1. Nguồn Dữ Liệu và Topic

| Hạng mục | Giá trị chính thức | Trạng thái |
|---|---|---|
| **Nguồn dữ liệu đầu vào** | `/data/curated/events` (Curated Parquet đã phân vùng) | Đã xác nhận |
| **Topic sự kiện chính thức** | `ecommerce_events` | Đã chốt chính thức |
| **Schema Version** | `kafka-v1.0` | Đã chốt chính thức |
| **Số phân vùng (Partitions)** | `3` (cấu hình môi trường VM-B) | Cấu hình đề xuất (Chờ kiểm chứng trên VM-B) |
| **Hệ số nhân bản (Replication Factor)** | `1` (môi trường máy ảo đơn VM-B) | Cấu hình đề xuất (Chờ kiểm chứng trên VM-B) |
| **Broker / Port** | `localhost:9092` | Cấu hình mặc định (Chờ kiểm chứng trên VM-B) |

> **Lưu ý cấu hình & kiểm chứng hạ tầng:**
> - Topic `ecommerce_events` là topic duy nhất được sử dụng cho việc phát lại và tiêu thụ luồng sự kiện eCommerce clickstream theo nhiệm vụ S2. Cấu hình topic cảnh báo `ecommerce.clickstream.alerts` (nếu có) phục vụ riêng cho chức năng gửi thông báo bất thường độc lập, không thay thế topic sự kiện chính `ecommerce_events`.
> - Các thông số hạ tầng (số partitions = 3, replication factor = 1, broker port = 9092) là cấu hình kỹ thuật đề xuất cho môi trường máy ảo đơn VM-B. Trạng thái hoạt động thực tế chỉ được đánh dấu là "Đã xác minh" khi có bằng chứng chạy lệnh kiểm tra kết nối trực tiếp trên máy ảo VM-B tại mốc nghiệm thu.

### 6.2. Quy Định Kafka Message Key

Khóa của message Kafka quyết định phân vùng (`partitioning`) và thứ tự xử lý dữ liệu của Consumer:

1. **Khi `user_session` có giá trị (Khác NULL):**
   - **Kafka Key** bắt buộc là chuỗi byte UTF-8 của chính giá trị `user_session`.
   - *Mục đích:* Đảm bảo toàn bộ các sự kiện thuộc cùng một phiên truy cập được chuyển vào **cùng một partition**, phục vụ gom nhóm theo phiên.
2. **Khi `user_session` là NULL:**
   - **Kafka Key** bắt buộc mang giá trị **`None` (Python) / `null` (Kafka wire protocol)** — tương ứng bản ghi không có payload khóa (key length = -1). Khi đó, cơ chế phân vùng mặc định của Kafka sẽ phân phối các message này ngẫu nhiên/round-robin qua các partition, tránh dồn ứ dữ liệu.
   - **Phân biệt rõ ràng giữa các loại giá trị:**
     - **`None` / `null` key:** Bản ghi không có key (key length = -1), được Kafka phân phối đều qua các partitions.
     - **Null byte (`b'\x00'`):** Một byte nhị phân có giá trị `0x00` (key length = 1); vẫn bị coi là có key và bị băm vào một partition cố định.
     - **Chuỗi rỗng (`""` / `b""`):** Payload độ dài 0 byte (key length = 0); vẫn bị băm vào một partition cố định.
     - **Chuỗi ký tự `"null"` hoặc `"None"`:** Các chuỗi văn bản thông thường; sẽ khiến toàn bộ các bản ghi bị dồn vào chung một partition gây lệch tải nghiêm trọng (data skew).
   - **TUYỆT ĐỐI CẤM:** Không được chuyển giá trị NULL thành null byte, chuỗi rỗng `""`, hoặc chuỗi ký tự `"null"`, `"None"`.
3. **Thứ tự trong Kafka vs Trật tự nghiệp vụ:**
   - Kafka chỉ bảo đảm thứ tự offset ghi nhận tăng dần (`log append / write offset`) bên trong từng partition; Kafka **hoàn toàn không bảo đảm thứ tự theo thời gian nghiệp vụ (`event_time`)**.
   - Tuyệt đối không tuyên bố hoặc ngộ nhận rằng Kafka offset order bảo đảm được event-time order.
   - Downstream (Consumer và Spark Structured Streaming) **bắt buộc phải kết hợp bộ ba `(event_time, event_type_rank, event_sort_key)`** để phục hồi trật tự nghiệp vụ chính xác trước khi đưa vào phễu chuyển đổi hoặc tính toán thời gian thực.
4. **Độc lập giữa Key và Value:**
   - Key và Value là hai thành phần tách biệt trong Kafka Record. Trường `user_session` vẫn xuất hiện đầy đủ trong payload JSON Value.

### 6.3. JSON Value Schema (12 Trường Bắt Buộc)

Payload Value của mỗi sự kiện được serialize dưới dạng JSON Object tuân thủ đúng **12 trường**, quy định nghiêm ngặt `additionalProperties: false`:

| # | Tên trường | Kiểu JSON | Bắt buộc | Ràng buộc giá trị & Định dạng | Nguồn gốc & Diễn giải |
|---|---|---|---|---|---|
| 1 | `event_time` | `string` | **Có** | Regex ISO-8601 UTC: `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$` | Thời điểm phát sinh sự kiện theo UTC |
| 2 | `event_type` | `string` | **Có** | Enum: `["view", "cart", "remove_from_cart", "purchase"]` | Loại tương tác clickstream |
| 3 | `product_id` | `string` | **Có** | Chuỗi ký tự định danh sản phẩm (kế thừa từ Curated) | Mã định danh sản phẩm dạng chuỗi |
| 4 | `category_id` | `string` | **Có** | Chuỗi ký tự định danh danh mục (kế thừa từ Curated) | Mã định danh danh mục dạng chuỗi |
| 5 | `category_code` | `string` hoặc `null` | **Có** | Cho phép mang giá trị chuỗi hoặc `null` | Mã phân cấp danh mục hàng hóa |
| 6 | `brand` | `string` | **Có** | `minLength: 1`, không rỗng, không `null` | Thương hiệu (đã chuẩn hóa `"Unknown"`) |
| 7 | `price` | `number` | **Có** | `minimum: 0.0` (số thực $\ge 0.0$, chấp nhận 0.0) | Giá niêm yết sản phẩm |
| 8 | `user_id` | `string` | **Có** | Chuỗi ký tự định danh người dùng (kế thừa Curated) | Mã định danh khách hàng dạng chuỗi |
| 9 | `user_session` | `string` hoặc `null` | **Có** | Chuỗi UUID hoặc `null` | Mã phiên truy cập của khách hàng |
| 10 | `event_type_rank` | `integer` | **Có** | Enum: `[1, 2, 3, 4]` | Trọng số thứ tự ưu tiên sự kiện |
| 11 | `event_sort_key` | `string` | **Có** | Regex SHA-256 64 hex: `^[a-f0-9]{64}$` | Khóa băm tie-breaker từ Curated Parquet |
| 12 | `schema_version` | `string` | **Có** | Hằng số chính thức: `"kafka-v1.0"` | Phiên bản schema của message Kafka |

**Ràng buộc cấu trúc JSON Schema:**
- Ba trường định danh `product_id`, `category_id`, `user_id` **bắt buộc là JSON String**, kế thừa chuẩn hóa từ `curated-v1.0`. Tuyệt đối không ép kiểu ngược lại thành Integer.
- Không đưa trường `event_date` hoặc `reject_reason` vào Kafka message payload (hai trường này chỉ phục vụ phân vùng Parquet và xử lý lỗi tại tầng Batch).
- Không tự ý thêm các trường tùy biến ngoài 12 trường trên (`additionalProperties: false`).

Ví dụ một Kafka Message JSON Value hợp lệ:

```json
{
  "event_time": "2019-10-01T00:00:00Z",
  "event_type": "view",
  "product_id": "44600062",
  "category_id": "2103807459595387724",
  "category_code": "electronics.smartphone",
  "brand": "shiseido",
  "price": 35.79,
  "user_id": "541312140",
  "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c",
  "event_type_rank": 1,
  "event_sort_key": "b472e987c67e914eb16bb2153b6fa286e4ea5da11cf5ea9d6fc578c78d59dc6b",
  "schema_version": "kafka-v1.0"
}
```

> ⚠️ **Lưu ý về giá trị hash:** Chuỗi `event_sort_key` trong JSON mẫu trên chỉ là giá trị chuỗi SHA-256 minh họa cấu trúc (đủ 64 ký tự hex hợp lệ), **không phải là test vector đã được xác nhận** qua thuật toán ETL tại Mục 5.3. Các thành phần hệ thống bắt buộc đọc trực tiếp giá trị `event_sort_key` được ETL sinh ra từ Curated Parquet, không dùng chuỗi mẫu này làm căn cứ kiểm thử logic.

### 6.4. Quy Định Timestamp và Chuẩn Hóa UTC

- Thời gian trong JSON Value bắt buộc hiển thị theo chuẩn **ISO-8601 UTC** với hậu tố `Z` (Zulu time) và độ chính xác đến giây.
- **Định dạng chuẩn duy nhất:** `YYYY-MM-DDTHH:MM:SSZ` (ví dụ: `2019-10-01T00:00:00Z`).
- **Các quy tắc cấm:**
  - Không dùng định dạng chuỗi CSV thô có khoảng trắng (ví dụ `2019-10-01 00:00:00 UTC`).
  - Không sử dụng múi giờ địa phương lệch UTC (ví dụ `+07:00`).
  - Không truyền epoch timestamp dạng số nguyên trong trường này.
  - Timestamp phải được chuyển đổi từ cột `TimestampType` chuẩn của Curated Parquet, không tự diễn giải theo múi giờ hệ điều hành.

### 6.5. Ordering Columns Trong Kafka Stream

- **Producer:** Đọc trực tiếp giá trị `event_type_rank` và `event_sort_key` từ Curated Parquet (`/data/curated/events`), đưa nguyên vẹn vào message JSON payload, tuyệt đối không tự tính lại mã băm SHA-256 hay rank.
- **Consumer & Spark Structured Streaming:** Đọc trực tiếp các trường này từ Kafka JSON payload (không đọc Curated Parquet), kết hợp bộ ba `(event_time, event_type_rank, event_sort_key)` để phục hồi trật tự nghiệp vụ chính xác trước khi đưa vào phễu chuyển đổi hoặc tính toán phân tích thời gian thực.

### 6.6. Quy Định Validation và Kiểm Tra Chéo (Cross-Field Validation)

Mọi message trước khi phát hoặc sau khi nhận từ Kafka cần vượt qua 10 tiêu chí validation:
1. Đúng cấu trúc 12 trường bắt buộc, không thừa trường (`additionalProperties: false`).
2. Đúng kiểu dữ liệu JSON của từng trường (đặc biệt 3 trường ID là String).
3. Trường `schema_version` bằng `"kafka-v1.0"`.
4. Trường `event_type` thuộc danh mục 4 loại sự kiện hợp lệ.
5. Trường `event_type_rank` là số nguyên trong khoảng `[1, 4]`.
6. **Kiểm tra chéo tương quan giữa `event_type` và `event_type_rank`:** Bắt buộc giá trị rank phải tương ứng chính xác với loại sự kiện (`view` $\leftrightarrow$ 1, `cart` $\leftrightarrow$ 2, `remove_from_cart` $\leftrightarrow$ 3, `purchase` $\leftrightarrow$ 4). Tránh lỗi JSON Schema độc lập chỉ check enum riêng lẻ từng cột dẫn đến lọt dữ liệu bất nhất (ví dụ `event_type="cart"` nhưng `rank=1`).
7. Định dạng `event_time` khớp regex ISO-8601 UTC có hậu tố `Z`.
8. Định dạng `event_sort_key` khớp chuỗi SHA-256 hex 64 ký tự chữ thường.
9. Kiểm tra NULL: `brand`, `user_id`, `product_id`, `category_id`, `price` không được mang giá trị NULL.
10. Kiểm tra giá trị số: `price >= 0.0`.

### 6.7. Phân Tầng Phiên Bản Schema (Schema Versioning Tiers)

Hệ thống phân định rõ ràng phiên bản schema độc lập cho từng tầng:

| Tầng dữ liệu | Schema Version chính thức | Định dạng lưu trữ / Truyền tải | Ghi chú |
|---|---|---|---|
| **Raw Layer** | `raw-v1.0` | CSV thô trên HDFS VM-A | 9 trường dữ liệu nguồn |
| **Curated Layer** | `curated-v1.0` | Parquet trên HDFS VM-A/VM-B | 13 trường (chuẩn hóa String ID, rank, sort key) |
| **Kafka Stream** | `kafka-v1.0` | JSON message trên Kafka Broker | 12 trường payload (loại bỏ partition key và reject reason) |

---

## 7. Hive Metastore & Bảng Dữ Liệu

| Hạng mục | Giá trị quy ước | Trạng thái |
|---|---|---|
| **Database** | `ecommerce_db` | ⬜ Chờ Người A xác nhận |
| **Bảng sự kiện chính (`fact_events`)** | External Table $\rightarrow$ `/data/curated/events` | ⬜ Chờ Người A xác nhận |
| **Bảng chiều phiên (`dim_sessions`)** | TBD | ⬜ Chờ phân tích A6 |
| **Bảng tổng hợp phễu (`agg_funnel_metrics`)** | TBD | ⬜ Chờ hoàn thiện A5 |

> **Cập nhật đường dẫn:** Bảng `fact_events` trong Hive (nếu được tạo) phải trỏ trực tiếp đến thư mục Curated Parquet chính thức đã phân vùng: `/data/curated/events`, không trỏ vào thư mục trung gian `events_cleaned`. Tên database và cấu trúc DDL cụ thể sẽ do Người A chốt sau khi hoàn thành Task A4.

---

## 8. S3 Manifest & Quy Trình Bàn Giao Dữ Liệu (A $\rightarrow$ B)

Manifest là file metadata đi kèm mỗi lần bàn giao dữ liệu Parquet từ máy ảo VM-A sang máy ảo VM-B.

### 8.1. Cấu Trúc Manifest Bàn Giao

Mỗi lần bàn giao Parquet từ VM-A sang VM-B cần kèm theo file manifest có các thông tin bắt buộc:

| Trường metadata | Ý nghĩa nghiệp vụ |
|---|---|
| `dataset_name` | Tên bộ dữ liệu được bàn giao (ví dụ: `REES46 eCommerce Behavior Data`) |
| `snapshot_id` | Mã định danh của phiên bản dữ liệu bàn giao |
| `source_hdfs_path` | Đường dẫn nguồn trên HDFS VM-A (`/data/curated/events`) |
| `destination_hdfs_path` | Đường dẫn đích trên HDFS VM-B (`/data/curated/events`) |
| `data_scope` | Phạm vi dữ liệu bàn giao (ví dụ: `October 2019`) |
| `record_count` | Tổng số bản ghi thực tế trong snapshot |
| `partition_count` | Số lượng thư mục phân vùng ngày (`event_date=YYYY-MM-DD`) |
| `min_event_date_utc` | Ngày nhỏ nhất trong dữ liệu theo UTC |
| `max_event_date_utc` | Ngày lớn nhất trong dữ liệu theo UTC |
| `schema_version` | Phiên bản schema của bộ dữ liệu bàn giao: **`curated-v1.0`** |
| `files` | Danh sách toàn bộ các file Parquet kèm dung lượng và mã băm SHA-256 từng file |
| `commit_hash` | Mã commit Git hoặc Git tag của phiên bản code sinh dữ liệu |

### 8.2. Quy Tắc Tạo Checksum SHA-256

- Bắt buộc tính toán mã băm SHA-256 cho từng file Parquet riêng biệt trong thư mục dữ liệu bàn giao.
- Lưu trữ danh sách file, dung lượng và mã băm tương ứng trong file manifest hoặc file đính kèm (`SHA256_OCT.txt`).
- Tuyệt đối không chỉ tính mã băm cho một file đại diện.
- Sau khi Người B nhận dữ liệu trên VM-B, phải thực hiện tính lại checksum độc lập và đối chiếu với manifest.

### 8.3. Tiêu Chí Nghiệm Thu Bàn Giao Dữ Liệu

| Tiêu chí | Điều kiện nghiệm thu đạt |
|---|---|
| **Record count** | Tổng số bản ghi trên VM-A và VM-B khớp nhau 100% |
| **Partition count** | Số lượng thư mục partition ngày khớp nhau |
| **SHA-256 Checksum** | Mã băm của từng file Parquet tại VM-B khớp hoàn toàn với VM-A |
| **Schema** | Cấu trúc dữ liệu khớp đúng `curated-v1.0` (13 trường) |
| **Snapshot ID** | Đúng phiên bản dữ liệu được phát hành |
| **Git Commit** | Xác định chính xác commit hash tạo dữ liệu trên Git |

### 8.4. Phương Thức Bàn Giao Dữ Liệu Qua Shared Folder

Nhóm thống nhất sử dụng Shared Folder của VirtualBox làm cầu nối truyền dữ liệu Parquet giữa hai máy ảo độc lập VM-A và VM-B:

**Bước 1. Trên VM-A (Người A chuẩn bị dữ liệu):**
Xuất dữ liệu Curated chính thức từ HDFS ra Shared Folder:

```bash
hdfs dfs -get /data/curated/events /media/sf_CapstoneData/
```

**Bước 2. Tạo Manifest và Checksum:**
- Người A tạo file manifest và file checksum (`SHA256_OCT.txt`) tại thư mục gốc bàn giao `/media/sf_CapstoneData/` (hoặc thư mục metadata riêng `/media/sf_CapstoneData/manifest/`).
- **Quy tắc bắt buộc:** **Tuyệt đối không lưu file manifest hoặc checksum bên trong thư mục `/media/sf_CapstoneData/events/`**. Thư mục này chỉ được chứa duy nhất các thư mục con phân vùng Parquet (`event_date=YYYY-MM-DD/`), đảm bảo khi nạp vào HDFS, Spark chỉ đọc dữ liệu Parquet thuần túy từ `/data/curated/events` mà không gặp lỗi do lẫn tệp văn bản phi cấu trúc.

**Bước 3. Trên VM-B (Người B tiếp nhận dữ liệu):**
- Kiểm tra tính tồn tại của thư mục đích trên HDFS VM-B. Nếu đường dẫn `/data/curated/events` đã tồn tại, cần sao lưu hoặc kiểm tra snapshot an toàn trước khi nạp dữ liệu mới.
- Nạp thư mục dữ liệu sạch từ Shared Folder vào HDFS VM-B:

```bash
# Đảm bảo nạp đúng vào /data/curated/events, tránh tạo đường dẫn lặp /data/curated/events/events
hdfs dfs -put /media/sf_CapstoneData/events /data/curated/
```

**Bước 4. Xác minh tính toàn vẹn:**
- Đọc lại dữ liệu Parquet bằng PySpark trên VM-B: `spark.read.parquet("/data/curated/events")`.
- Đối chiếu record count, partition count và tính toán lại checksum SHA-256 đối chiếu với file manifest nằm ngoài thư mục dữ liệu.
- Xác nhận nghiệm thu dữ liệu với Người A.

---

## 9. Hợp Đồng Dữ Liệu Machine Learning (ML Data Contract)

Mục này xác lập hợp đồng dữ liệu cho bài toán Machine Learning (Task P2-B-S2-02) phục vụ mô hình hóa hành vi mua hàng tại điểm quan sát tiền tố của phiên truy cập (*Session-level Purchase Prediction at Prefix $N$*).

### 9.1. Nguồn Dữ Liệu ML và Event Ordering

- **Nguồn dữ liệu:** Pipeline huấn luyện và đánh giá mô hình ML chính thức đọc trực tiếp từ thư mục Curated Parquet: **`/data/curated/events`**.
- **Thứ tự sự kiện trong phiên:** Các sự kiện thuộc cùng một phiên (`user_session`) bắt buộc phải được sắp xếp theo đúng quy tắc chung tại Mục 5.1:

```text
(event_time UTC ASC, event_type_rank ASC, event_sort_key ASC)
```

- **Kế thừa trực tiếp:** Pipeline ML (Dataset Builder và Feature Pipeline) đọc trực tiếp hai cột `event_type_rank` và `event_sort_key` từ Curated Parquet (`/data/curated/events`), tuyệt đối không tự tính lại hàm băm SHA-256 hay rank.
- **Cấm tie-breaker tùy biến:** Tuyệt đối không sử dụng `product_id` làm tiêu chí tie-breaker thứ ba thay cho `event_sort_key`.
- **Đánh số thứ tự sự kiện:** Hàm cửa sổ `row_number()` ($rn$) trong từng phiên phải được đánh số dựa trên thứ tự sắp xếp chuẩn ở trên trước khi phân chia Head và Tail.

### 9.2. Prediction Point $N$

1. **Thời điểm dự đoán:** Được xác định ngay sau khi sự kiện thứ $N$ của phiên truy cập diễn ra.
2. **Phân định Head và Tail:**
   - **Tập tiền tố (Head):** Gồm tất cả các sự kiện có số thứ tự $rn \le N$. Toàn bộ đặc trưng (features) của phiên **chỉ được phép trích xuất từ tập này**. Sự kiện thứ $N$ (`rn == N`) nằm trong Head và được đưa vào tính toán đặc trưng.
   - **Tập phần đuôi (Tail):** Gồm tất cả các sự kiện có số thứ tự $rn > N$. Tập này **chỉ dùng duy nhất cho việc xác định nhãn mục tiêu (`label`)** và kiểm tra tính hoàn chỉnh của phiên; tuyệt đối không tham gia vào việc trích xuất đặc trưng.
3. **Giá trị $N$ chính thức:** Hiện tại là **TBD (Chưa chốt)**. Giá trị $k=5$ trong mã nguồn thử nghiệm hiện tại chỉ là tham số mặc định cho việc phát triển code, không phải giá trị hợp đồng chính thức. Người A và B sẽ chốt giá trị $N$ chính thức sau khi có kết quả phân tích phân phối độ dài phiên từ Task A6 trên tập Train/Validation. Tuyệt đối không dùng tập Test để lựa chọn $N$.

### 9.3. Nhãn Mục Tiêu (Label Contract)

Mô hình giải bài toán phân loại nhị phân: $\text{label} \in \{0, 1\}$.

1. **Gán nhãn dương (`label = 1`):**
   - Phiên có **ít nhất một sự kiện `purchase`** xảy ra ở phần đuôi ($rn > N$).
   - Nếu trong phần đuôi có nhiều hơn 1 sự kiện `purchase`, nhãn vẫn là `1`.
2. **Gán nhãn âm (`label = 0`):**
   - Phiên **không có bất kỳ sự kiện `purchase` nào** ở phần đuôi ($rn > N$), đồng thời thỏa mãn đầy đủ các điều kiện là một phiên hoàn chỉnh (không bị cắt biên dữ liệu).
   - Tuyệt đối không mặc định một phiên chưa thấy purchase trong phạm vi dữ liệu quan sát là `label = 0` nếu chưa xác minh tính hoàn chỉnh của phiên.
3. **Loại bỏ phiên có Purchase sớm (Early Purchase Exclusion):**
   - Nếu một phiên có bất kỳ sự kiện `purchase` nào xuất hiện tại hoặc trước Prediction Point ($rn \le N$), phiên đó **bắt buộc bị loại bỏ hoàn toàn khỏi bộ dữ liệu ML**.
   - *Cơ sở nghiệp vụ:* Bài toán tập trung dự đoán ý định mua sắm của khách hàng chưa chuyển đổi. Nếu khách hàng đã mua hàng ngay trong $N$ tương tác đầu tiên, việc tiếp tục dự đoán chuyển đổi sau đó không còn mang ý nghĩa kinh doanh của bài toán chuyển đổi ban đầu.

### 9.4. Các Quy Tắc Loại Trừ Phiên (Exclusion Rules)

Để bảo đảm chất lượng dữ liệu huấn luyện, các phiên bị loại bỏ theo các quy tắc sau:

1. **`user_session` không hợp lệ:** Loại bỏ các bản ghi có `user_session` là NULL, chuỗi rỗng (`""`), hoặc chuỗi chỉ chứa khoảng trắng.
2. **Phiên không đủ độ dài ($n\_events < N$):** Quy tắc loại bỏ đã được định nghĩa chính thức. Bắt buộc loại bỏ vì không đủ dữ liệu hình thành tập tiền tố Head.
3. **Trường hợp phiên có đúng $N$ sự kiện ($n\_events = N$):** Hiện tại là **TBD (Chưa chốt)**. Cần Người A và B thống nhất xem các phiên này bị loại bỏ vì thiếu thông tin quan sát ở đuôi hay được gán nhãn âm theo chính sách cụ thể.
4. **Phiên có Purchase sớm ($rn \le N$):** Bắt buộc loại bỏ theo quy định tại Mục 9.3.
5. **Phiên bị cắt biên dữ liệu:** Bị loại bỏ hoặc không được gán nhãn theo quy tắc tại Mục 9.5.

*Nguyên tắc thống kê loại trừ:* Khi lập báo cáo thống kê chất lượng dữ liệu (EDA), việc tính toán số lượng phiên bị loại phải được thực hiện theo cơ chế lọc phân cấp tuần tự (hierarchical filtering) để đảm bảo các nhóm bị loại là hoàn toàn rời rạc (disjoint), không bị đếm trùng lặp.

### 9.5. Nguyên Tắc Kiểm Soát Cắt Biên Dữ Liệu (Censoring Control)

1. **Nguyên tắc quan sát nhãn:** Mọi nhãn mục tiêu trong bài toán ML bắt buộc phải có đủ điều kiện quan sát đầy đủ để xác nhận tính chính xác của nhãn âm.
2. **Nguy cơ cắt biên hai đầu thời gian:**
   - **Left-Censoring (Đầu kỳ):** Các phiên bắt đầu trước ranh giới dữ liệu nhưng chỉ được ghi nhận phần sau có nguy cơ thiếu các sự kiện ở Head.
   - **Right-Censoring (Cuối kỳ):** Các phiên diễn ra sát ranh giới kết thúc của tập dữ liệu có nguy cơ bị cắt dở dang, dẫn đến việc hành vi mua hàng ở Tail diễn ra sau ranh giới dữ liệu không được ghi nhận, gây ra nhãn âm giả (false negative).
3. **Chính sách chính thức: TBD.**
   - Nhóm cần chốt chính thức tiêu chí xác định phiên hoàn chỉnh và cửa sổ thời gian quan sát nhãn (Label Observation Window).
   - Tuyệt đối không đưa heuristic loại bỏ toàn bộ ngày cuối cùng vào làm quy tắc production chính thức khi chưa có sự thống nhất giữa hai thành viên.
   - Không sử dụng nhãn của các phiên bị cắt biên chưa qua xử lý hợp lệ để báo cáo kết quả đánh giá mô hình.

### 9.6. Danh Mục Đặc Trưng Tiền Tố (Feature Contract)

Dưới đây là **danh mục 15 đặc trưng tiền tố đang được đề xuất từ codebase**, toàn bộ được trích xuất từ tập tiền tố Head ($rn \in [1, N]$):

| # | Tên đặc trưng | Kiểu dữ liệu | Phạm vi tính toán | Diễn giải nghiệp vụ |
|---|---|---|---|---|
| 1 | `view_count` | `LongType` | $rn \le N$ | Tổng số lần xem sản phẩm trong $N$ sự kiện đầu |
| 2 | `cart_count` | `LongType` | $rn \le N$ | Tổng số lần thêm vào giỏ hàng trong $N$ sự kiện đầu |
| 3 | `remove_count` | `LongType` | $rn \le N$ | Tổng số lần xóa khỏi giỏ hàng trong $N$ sự kiện đầu |
| 4 | `has_cart` | `IntegerType` | $rn \le N$ | Cờ nhị phân: `1` nếu `cart_count > 0`, ngược lại `0` |
| 5 | `unique_product_count` | `IntegerType` | $rn \le N$ | Số lượng `product_id` phân biệt tương tác trong $N$ sự kiện đầu |
| 6 | `unique_category_count` | `IntegerType` | $rn \le N$ | Số lượng danh mục phân biệt tương tác trong $N$ sự kiện đầu |
| 7 | `unique_brand_count` | `IntegerType` | $rn \le N$ | Số lượng `brand` phân biệt tương tác trong $N$ sự kiện đầu |
| 8 | `avg_price` | `DoubleType` | $rn \le N$ | Giá trung bình của các sản phẩm tương tác trong $N$ sự kiện đầu |
| 9 | `max_price` | `DoubleType` | $rn \le N$ | Giá cao nhất của sản phẩm tương tác trong $N$ sự kiện đầu |
| 10 | `min_price` | `DoubleType` | $rn \le N$ | Giá thấp nhất của sản phẩm tương tác trong $N$ sự kiện đầu |
| 11 | `interaction_duration` | `LongType` | $rn \le N$ | Thời lượng tương tác tính bằng giây: $\text{time}(rn=N) - \text{time}(rn=1)$ |
| 12 | `hour_of_day` | `IntegerType` | $rn = N$ | Giờ trong ngày (0 đến 23 theo UTC) tại sự kiện thứ $N$ |
| 13 | `day_of_week` | `IntegerType` | $rn = N$ | Ngày trong tuần (1 = Chủ nhật, 7 = Thứ bảy) tại sự kiện thứ $N$ |
| 14 | `last_event_type` | `StringType` | $rn = N$ | Loại tương tác tại đúng sự kiện thứ $N$ (`view`, `cart`, `remove_from_cart`) |
| 15 | `prev_event_type` | `StringType` | $rn = N - 1$ | Loại tương tác tại sự kiện liền trước $N$ (bằng `null` nếu $N=1$) |

**Ràng buộc nghiêm ngặt về đặc trưng:**
- **Trạng thái phê duyệt:** Danh mục 15 đặc trưng trên là **danh mục đề xuất**, đang chờ Người A và Người B rà soát và phê duyệt chính thức tại mốc H04.
- **Ranh giới tính toán:** Toàn bộ đặc trưng chỉ được tính từ Head ($rn \le N$). Tuyệt đối cấm sử dụng bất kỳ thông tin nào từ Tail ($rn > N$).
- **Cấm thông tin toàn phiên:** Cấm sử dụng tổng số sự kiện của cả phiên (`n_events`), tổng thời lượng cả phiên hoặc thời điểm kết thúc phiên làm đặc trưng.
- **Cấm định danh trong feature vector:** Các trường định danh (`user_id`, `user_session`, `product_id`) chỉ phục vụ phân vùng, gom nhóm hoặc chia tập dữ liệu; tuyệt đối không đưa trực tiếp vào vector đặc trưng của mô hình.
- *Lưu ý về định nghĩa danh mục:* Trong đề xuất tính `unique_category_count`, trường `category_code` được sử dụng để đếm danh mục phân biệt; nếu mã nguồn thực tế sử dụng `category_id`, điểm khác biệt này được ghi nhận để hai bên chốt thống nhất tại Mục 10.

### 9.7. Phân Chia Tập Dữ Liệu Theo Thời Gian (Temporal Split)

1. **Nguyên tắc cốt lõi:**
   - **Tuyệt đối cấm Random Split.** Việc chia ngẫu nhiên trên dữ liệu chuỗi hành vi người dùng sẽ dẫn đến hiện tượng rò rỉ thông tin tương lai về quá khứ (Lookahead Bias), làm sai lệch kết quả đánh giá mô hình.
   - Việc phân chia bắt buộc phải tuân thủ nghiêm ngặt dòng thời gian: Toàn bộ dữ liệu **Train** xảy ra trước dữ liệu **Validation**, và toàn bộ dữ liệu **Validation** xảy ra trước dữ liệu **Test**.
   - Mỗi phiên chỉ thuộc về một tập duy nhất.
2. **Mốc phân chia thời gian chính thức (Fixed Cutoff Dates):**
   - `train_end_utc`: **TBD** (Chưa chốt).
   - `validation_end_utc`: **TBD** (Chưa chốt).
   - Tập Test: Bắt đầu từ thời điểm ngay sau `validation_end_utc` đến hết phạm vi dữ liệu hợp lệ.
3. **Các quyết định kỹ thuật còn TBD:**
   - **Định nghĩa ngày của Session ML:** Cần xác nhận ML sử dụng ngày của sự kiện đầu tiên trong phiên (`min(to_date(event_time))`) hay đồng bộ với quy ước Funnel (ngày của sự kiện `view` đầu tiên).
   - **Xử lý phiên vượt qua ranh giới split:** Cần thống nhất chính sách đối với các phiên bắt đầu tại tập trước nhưng có các sự kiện ở Tail kéo dài sang tập sau để ngăn chặn việc nhãn tương lai gây rò rỉ dữ liệu.

### 9.8. Quy Tắc Ngăn Chặn Rò Rỉ Dữ Liệu (Data Leakage Prevention)

Hệ thống thiết lập 8 ranh giới nghiêm ngặt để ngăn chặn rò rỉ dữ liệu:
1. **Ranh giới Head / Tail (Feature - Label Separation):** Mọi đặc trưng chỉ được trích xuất từ $rn \le N$. Mọi nhãn mục tiêu chỉ được xác định từ $rn > N$.
2. **Ranh giới phân chia phiên (Session Disjointness):** Mỗi phiên chỉ xuất hiện trong duy nhất một tập (Train, Validation hoặc Test), không có sự giao thoa phiên giữa các tập.
3. **Ranh giới nhân quả thời gian (Temporal Causality):** Mô hình chỉ học từ quá khứ để dự đoán tương lai, tuân thủ dòng thời gian $\text{Train} < \text{Validation} < \text{Test}$.
4. **Ranh giới tiền xử lý đặc trưng (Pipeline Fit Separation):** Mọi phép biến đổi đặc trưng có tham số (tính mean, std của biến số, tạo từ điển nhãn của `StringIndexer`, OneHotEncoder) **chỉ được phép `fit()` trên tập Train**. Tập Validation và Test chỉ áp dụng phép `transform()` từ mô hình tiền xử lý đã fit trên Train.
5. **Ranh giới tối ưu siêu tham số và đánh giá (Evaluation Integrity):** Giá trị $N$, lựa chọn mô hình, tối ưu siêu tham số và ngưỡng phân loại tối ưu (classification threshold) **chỉ được thực hiện trên tập Train và Validation**. Tập Test được giữ độc lập tuyệt đối và chỉ dùng để đánh giá mô hình sau khi đã đóng băng toàn bộ cấu hình.
6. **Ranh giới loại bỏ chuyển đổi sớm (Early Purchase Exclusion):** Các phiên đã phát sinh Purchase tại hoặc trước $N$ bị loại bỏ hoàn toàn, không để mô hình học các mẫu đã chuyển đổi trước điểm quan sát.
7. **Ranh giới định danh (No Identifiers in Features):** Tuyệt đối không đưa các định danh cá nhân hoặc phiên (`user_id`, `user_session`) vào vector đặc trưng để tránh mô hình ghi nhớ khách hàng thay vì học quy luật hành vi.
8. **Ranh giới dữ liệu cắt biên (Censoring Integrity):** Không đưa các phiên bị cắt biên dữ liệu chưa qua xử lý hợp lệ vào tập đánh giá mô hình nhằm tránh làm lệch phân phối nhãn âm.

---

## 10. Quản Lý Các Quyết Định Chưa Chốt (Pending Decisions)

### 10.1. Quyết Định Kỹ Thuật Chờ A/B Thống Nhất Tại Mốc Nghiệm Thu H04

Bảng theo dõi các quyết định kỹ thuật cốt lõi của Data Contract cần sự thống nhất chính thức giữa Người A và Người B tại mốc nghiệm thu H04:

| # | Hạng mục quyết định | Trạng thái | Owner xác nhận | Điều kiện và căn cứ chốt |
|---|---|---|---|---|
| 1 | **Giá trị $N$ chính thức cho Prediction Point** | `TBD` | Người A & B | Dựa trên kết quả phân tích phân phối độ dài phiên từ Task A6 trên tập Train/Validation; không gán cứng giá trị. |
| 2 | **Quy tắc đối với phiên có đúng $N$ sự kiện ($n\_events = N$)** | `TBD` | Người A & B | Thống nhất loại bỏ vì thiếu thông tin ở Tail hay gán `label = 0` theo chính sách phiên hoàn chỉnh. *(Lưu ý: Quy tắc loại bỏ phiên $n\_events < N$ đã được chốt chính thức tại Mục 9.4)*. |
| 3 | **Định nghĩa ngày của Session cho ML** | `TBD` | Người A | Xác nhận ML dùng `min(to_date(event_time))` hay dùng ngày của sự kiện `view` đầu tiên như Funnel. |
| 4 | **Tiêu chí phiên hoàn chỉnh & Label Observation Window** | `TBD` | Người A & B | Thống nhất cửa sổ thời gian quan sát nhãn để xác định tính hợp lệ của `label = 0`. |
| 5 | **Chính sách xử lý Left-Censoring** | `TBD` | Người A & B | Xác định phương pháp xử lý các phiên bắt đầu trước ranh giới dữ liệu nguồn. |
| 6 | **Chính sách xử lý Right-Censoring** | `TBD` | Người A & B | Thống nhất quy tắc xử lý cắt biên tổng quát thay thế cho heuristic loại bỏ ngày cuối cùng. |
| 7 | **Mốc thời gian `train_end_utc`** | `TBD` | Người A & B | Chốt ngày kết thúc tập Train dựa trên phân bố dữ liệu thực tế. |
| 8 | **Mốc thời gian `validation_end_utc`** | `TBD` | Người A & B | Chốt ngày kết thúc tập Validation. |
| 9 | **Xử lý phiên vượt ranh giới Temporal Split** | `TBD` | Người A & B | Thống nhất chính sách đối với các phiên có sự kiện Tail kéo dài sang tập dữ liệu kế tiếp. |
| 10 | **Phê duyệt danh mục 15 đặc trưng tiền tố đề xuất** | `Chờ A/B review` | Người A & B | Rà soát danh mục 15 features ứng viên và chốt định nghĩa `unique_category_count` (`code` vs `id`). |
| 11 | **Ràng buộc NULL của ID trong Curated vs Kafka** | `Chờ A xác nhận` | Người A | Xác nhận `product_id` và `category_id` có thể mang giá trị NULL trong Curated Parquet hay không để đồng bộ với Kafka Schema. |
| 12 | **Chuẩn hóa đường dẫn thư mục Quarantine** | `Chờ A xác nhận` | Người A | Xác nhận `/data/curated/_quarantine/invalid_events` (theo code ETL thực tế) thay cho `events_rejected`. |
| 13 | **Xác nhận DDL Hive Metastore cho bảng `fact_events`** | `Chờ A xác nhận` | Người A | Xác nhận tên database `ecommerce_db` và DDL bảng `fact_events` trỏ tới `/data/curated/events`. |

> **Ghi chú về Tie-breaker:** Bộ ba sắp xếp `(event_time ASC, event_type_rank ASC, event_sort_key ASC)` đã được thống nhất tuyệt đối tại Mục 5.1 và Mục 9.1; không đưa `product_id` vào tiêu chí tie-breaker.

### 10.2. Technical Backlog (Hạng Mục Vận Hành Nâng Cao Ngoài Phạm Vi Nghiệm Thu H04)

Các đề xuất kiến trúc và vận hành dưới đây thuộc giai đoạn triển khai mở rộng sau H04, **không coi là điều kiện bắt buộc để nghiệm thu Kafka/ML Contract**:

1. **Giám sát độ trôi dữ liệu (Data Drift Monitoring):** Thiết lập ngưỡng PSI cho `price` và giám sát trôi phân phối `event_type` theo thời gian.
2. **Cấu hình Dead Letter Queue (DLQ):** Topic `ecommerce_events_dlq`, thời gian lưu trữ 14 ngày và cơ chế định tuyến tin nhắn lỗi.
3. **Tần suất xuất bản Parquet từ luồng Kafka:** Chiến lược micro-batch interval hoặc commit threshold từ streaming ghi vào Data Lake.
4. **Phân quyền và bảo mật Hive Metastore:** Quy định IAM/RBAC chi tiết giữa dịch vụ ghi của Spark ETL/Streaming và tầng đọc của Analytics/ML.
5. **Chiến lược Schema Evolution:** Cơ chế tương thích ngược (backward-compatible) khi mở rộng các trường JSON trong các phiên bản Kafka schema tiếp theo.

---

## 11. Lịch Sử Thay Đổi và Trạng Thái Hợp Đồng (Changelog & Document Status)

### 11.1. Lịch Sử Thay Đổi (Changelog S2)

| Phiên bản | Ngày | Tác giả | Tóm tắt các thay đổi chính |
|---|---|---|---|
| `v0.1-draft` | 2026-11-01 | Người A | Soạn thảo bản khung Data Contract ban đầu (Raw, Curated, UTC, Funnel, Manifest). |
| `v0.2-draft` | 2026-11-02 | Người A | Bổ sung quy định `price = 0`, chuẩn hóa schema Curated 13 trường, quy tắc `event_sort_key`. |
| `v1.0-draft-integrated` | 2026-11-03 | Người B (Review A) | **Hợp nhất toàn diện Kafka Contract và ML Contract:**<br>1. Đồng bộ đường dẫn Curated production thành `/data/curated/events` trên toàn bộ tài liệu.<br>2. Xác nhận 3 trường ID (`product_id`, `category_id`, `user_id`) là String trong Curated và Kafka JSON.<br>3. Quy định Producer/ML kế thừa trực tiếp `event_type_rank` và `event_sort_key` có sẵn trong Parquet, áp dụng chuẩn hóa thuật toán SHA-256 từ `etl_oct.py`.<br>4. Hợp nhất toàn bộ Kafka Contract vào Mục 6: Topic `ecommerce_events`, Schema `kafka-v1.0`, 12 trường JSON payload, xử lý `null` key, kiểm tra chéo rank.<br>5. Cập nhật đường dẫn bảng Hive `fact_events` và quy trình bàn giao HDFS Manifest tại Mục 7 và 8.<br>6. Hợp nhất ML Contract vào Mục 9: Prediction point $N$, Head/Tail, Label nhị phân, Early Purchase Exclusion, 15 features đề xuất, Temporal Split (Train < Validation < Test), 8 ranh giới chống rò rỉ dữ liệu.<br>7. Tách riêng các chi tiết triển khai/lỗi code H02 sang báo cáo review độc lập; thiết lập bảng theo dõi Pending Decisions (Mục 10.1) và Technical Backlog (Mục 10.2). |

### 11.2. Trạng Thái Hợp Đồng Hiện Tại

- **Trạng thái tài liệu:** 🟡 **READY FOR A/B REVIEW (SẴN SÀNG CHO A/B DUYỆT)**
- **Mốc nghiệm thu:** Chuẩn bị cho mốc nghiệm thu **H04 – Data Contract**.
- **Lưu ý:** Tài liệu này phản ánh bản hợp nhất kỹ thuật hoàn chỉnh giữa hai nhiệm vụ S2-01 và S2-02 trên nhánh làm việc. Tài liệu **chưa được đánh dấu là APPROVED hay PRODUCTION READY** cho đến khi Người A và Người B hoàn tất việc rà soát và thống nhất các quyết định tại Mục 10.1.

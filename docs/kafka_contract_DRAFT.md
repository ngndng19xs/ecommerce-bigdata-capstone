# Kafka JSON Contract (Bản Đề Xuất DRAFT-S2-01)

> **Task:** P2-B-S2-01 – Kafka JSON Contract<br>
> **Người đề xuất:** Người B (Streaming & ML)<br>
> **Người đánh giá (Reviewer):** Người A (Data & Analytics)<br>
> **Trạng thái:** 🟡 **BẢN ĐỀ XUẤT (DRAFT) – CHỜ NGƯỜI A PHÊ DUYỆT (H04)**<br>
> **Tài liệu tham chiếu:** [data_contract.md](./data_contract.md) | [b2_producer_guide.md](./b2_producer_guide.md)

---

## 1. Mục Tiêu Task P2-B-S2-01

Task **P2-B-S2-01** do Người B thực hiện nhằm thiết lập quy chuẩn cấu trúc message (Schema Contract) cho luồng sự kiện phát vào Apache Kafka, làm cầu nối đồng bộ giữa tầng **Batch ETL (Người A)** và tầng **Streaming / ML (Người B)**:
- Chuẩn hóa định dạng JSON payload và khóa phân vùng (Kafka Message Key).
- Kế thừa đầy đủ các quy tắc làm sạch dữ liệu, thứ tự ưu tiên sự kiện từ Data Contract của Người A tại [data_contract.md](./data_contract.md).
- Cung cấp Schema JSON chuẩn (`kafka_event_schema_DRAFT.json`), dữ liệu mẫu (`kafka_records_DRAFT.jsonl`), công cụ kiểm tra độc lập (`validate_kafka_contract.py`) và bộ unit test tự động (20 test cases).
- Chuẩn bị nền tảng kỹ thuật trước khi bước vào nghiệm thu Task **B2-02** (Phát lại dữ liệu chính thức vào Kafka).

> ⚠️ **Lưu ý quan trọng:** Đây là bản dự thảo kỹ thuật do Người B đề xuất cho Task P2-B-S2-01. Tài liệu này **KHÔNG thay thế** [data_contract.md](./data_contract.md) và chưa được áp dụng vào pipeline production cho đến khi có sự rà soát và thống nhất chính thức giữa hai thành viên tại mốc H04.

---

## 2. Nguồn Dữ Liệu và Quy Tắc Kế Thừa Từ Data Contract Của Người A

### 2.1. Phân biệt nguồn dữ liệu phát triển vs dữ liệu production
1. **Tập mẫu H02 (Môi trường phát triển & kiểm thử):**
   - Tập file CSV mẫu được trích xuất phục vụ phát triển, benchmark hiệu năng và kiểm thử sơ bộ trên máy ảo VM-B.
   - Gồm 2 file CSV thô với 1.244.245 bản ghi phát sinh trong ngày `2019-10-01 UTC`, đặt tại thư mục chia sẻ `/media/sf_CapstoneSample/`.
   - Dùng để kiểm tra khả năng replay của Producer và benchmark throughput.
2. **Dữ liệu Production chính thức:**
   - Toàn bộ pipeline chính thức khi vận hành sẽ tiêu thụ trực tiếp từ thư mục **Curated Parquet** (`/data/curated/events_cleaned` hoặc đường dẫn theo cấu hình dự án) do Người A bàn giao từ pipeline PySpark ETL.
   - Producer B2 **không tự ý parse lại file CSV thô hay tự làm sạch lại dữ liệu**, mà kế thừa trọn vẹn kết quả chuẩn hóa của Người A.

### 2.2. Các quy tắc làm sạch kế thừa từ Người A
Bản đề xuất Kafka Contract kế thừa nghiêm ngặt toàn bộ các quyết định về dữ liệu đã được Người A ghi nhận tại [data_contract.md](./data_contract.md):
- **`brand`**: Các giá trị NULL hoặc rỗng trong dữ liệu Curated đã được chuyển đổi thành chuỗi `"Unknown"`. Do đó, message phát vào Kafka **bắt buộc không được để rỗng hoặc NULL** (độ dài tối thiểu 1 ký tự).
- **`category_code`**: Giữ nguyên `null` nếu dữ liệu gốc bị khuyết.
- **`price = 0`**: Được công nhận là hợp lệ (dữ liệu khuyến mãi/quà tặng, có ~269.664 bản ghi trong tập Curated).
- **`price < 0` hoặc NULL**: Bị loại bỏ từ bước Quarantine của ETL, tuyệt đối không xuất hiện trong Curated cũng như Kafka stream.
- **`user_id`**: Bắt buộc không NULL trong dữ liệu Curated và Kafka message.
- **`user_session`**: Được phép mang giá trị `null` trong Curated (nhưng có quy tắc phân vùng Kafka và xử lý Stream/ML riêng).

---

## 3. Kafka Topic & Thông Số Môi Trường Thực Tế

### 3.1. Thông số cụm Kafka trên VM-B (Đã xác minh thực tế)
Qua kiểm tra và chạy thử nghiệm thực tế trên môi trường máy ảo VM-B:
- **ZooKeeper:** `localhost:2181` (đã khởi động và hoạt động ổn định).
- **Kafka Broker:** `localhost:9092` (broker id = 0, đang hoạt động).
- **Topic thử nghiệm kỹ thuật:** `ecommerce_events`
  - Số phân vùng (Partitions): **3**
  - Hệ số nhân bản (Replication Factor): **1**
  - Trạng thái: Cả 3 partition đều có Leader = 0, Replicas = [0], Isr = [0].

### 3.2. Điểm cần đối chiếu cấu hình
- Trong tài liệu kiến trúc trước đó và file `configs/config.example.yaml`, tên topic mẫu được ghi là `ecommerce.clickstream.raw`.
- Hiện tại trên VM-B đang tồn tại và thử nghiệm với topic `ecommerce_events`.
- **Đề xuất thống nhất:** Giữ nguyên topic `ecommerce_events` cho các thử nghiệm kỹ thuật nội bộ hiện tại; cần Người A và Người B chốt chính thức sử dụng tên topic `ecommerce_events` (chuẩn tên bảng) hay `ecommerce.clickstream.raw` (chuẩn domain entity) trước khi tiến hành nghiệm thu B2-02.

---

## 4. Quy Định Kafka Message Key

Khóa của bản ghi Kafka đóng vai trò quyết định phân vùng (`partitioning`) và đảm bảo tính tuần tự của sự kiện:

1. **Khi `user_session` có giá trị (Khác NULL):**
   - **Kafka Key** bắt buộc phải là chuỗi byte UTF-8 của chính `user_session` đó.
   - *Mục đích:* Đảm bảo tất cả các hành động trong cùng một phiên của người dùng được phân phối vào **cùng một partition**, duy trì trật tự thời gian chính xác cho Consumer tính toán phễu chuyển đổi (Funnel).
   - *Lưu ý kỹ thuật về thứ tự:* Kafka chỉ bảo đảm thứ tự tuần tự trong một partition **theo thứ tự ghi (write sequence/offset)**, không tự động sắp xếp theo `event_time`. Do đó tầng Producer phải duy trì thứ tự phát sự kiện chuẩn hoặc Consumer phải thực hiện sort theo bộ 3 `(event_time, event_type_rank, event_sort_key)`.
2. **Khi `user_session` là NULL:**
   - **Kafka Key** bắt buộc phải mang giá trị **`null` (None trong Python)**.
   - **TUYỆT ĐỐI CẤM:** Không được chuyển thành chuỗi ký tự `"null"` hoặc `"None"`, vì điều này sẽ gom toàn bộ các sự kiện mồ côi vào chung một partition duy nhất gây lệch tải nghiêm trọng (data skew).
   - *Hành vi Kafka:* Khi key là `null`, Kafka partitioner mặc định (DefaultPartitioner trong Kafka-Python hoặc Kafka Java Producer) sẽ phân phối bản ghi theo thuật toán Round-Robin hoặc Sticky Partitioner ngẫu nhiên giữa các partition.
3. **Độc lập giữa Key và Value:**
   - Kafka Key và Kafka Value JSON là hai thành phần tách biệt trong cấu trúc một Kafka Record. Trường `user_session` vẫn xuất hiện đầy đủ bên trong payload JSON Value.

---

## 5. Kafka JSON Value Schema (12 Trường)

File định nghĩa JSON Schema chuẩn: [`../src/streaming/schemas/kafka_event_schema_DRAFT.json`](../src/streaming/schemas/kafka_event_schema_DRAFT.json).

Payload Value gồm **12 trường**, quy định `additionalProperties: false` để cấm các trường ngoài danh mục:

| # | Tên trường | JSON Schema Type | Bắt buộc | Ràng buộc giá trị / Định dạng | Mô tả nghiệp vụ |
|---|---|---|---|---|---|
| 1 | `event_time` | `string` | **Có** | Regex ISO-8601 UTC: `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$` | Thời điểm phát sinh sự kiện |
| 2 | `event_type` | `string` | **Có** | Enum: `["view", "cart", "remove_from_cart", "purchase"]` | Loại tương tác clickstream |
| 3 | `product_id` | `integer` | **Có** | Minimum: 0 (Long) | Mã định danh sản phẩm |
| 4 | `category_id` | `integer` | **Có** | Minimum: 0 (Long) | Mã định danh danh mục hàng |
| 5 | `category_code` | `["string", "null"]`| **Có** | String hoặc `null` | Phân cấp danh mục (ví dụ `electronics.smartphone`) |
| 6 | `brand` | `string` | **Có** | MinLength: 1 (Không được rỗng hoặc null) | Thương hiệu (đã chuẩn hóa `"Unknown"` nếu khuyết) |
| 7 | `price` | `number` | **Có** | Minimum: 0.0 (Double, chấp nhận 0.0) | Giá niêm yết sản phẩm |
| 8 | `user_id` | `integer` | **Có** | Minimum: 0 (Long, không null) | Mã định danh khách hàng |
| 9 | `user_session` | `["string", "null"]`| **Có** | String (UUID) hoặc `null` | Mã phiên truy cập của người dùng |
| 10| `event_type_rank`| `integer` | **Có** | Enum: `[1, 2, 3, 4]` tương ứng loại sự kiện | Thứ tự ưu tiên giải quyết xung đột timestamp |
| 11| `event_sort_key` | `string` | **Có** | Regex SHA-256 hex 64 ký tự: `^[a-f0-9]{64}$` | Khóa băm xác định thứ tự duy nhất |
| 12| `schema_version` | `string` | **Có** | Hằng số đề xuất: `"DRAFT-S2-01"` | Phiên bản hợp đồng message Kafka (đề xuất nâng lên `1.0.0` khi chính thức) |

---

## 6. Quy Định Timestamp và UTC

- Chuẩn hiển thị thời gian trong Kafka JSON: **ISO-8601 UTC** với hậu tố `Z` (Zulu time), độ chính xác đến giây.
- **Định dạng chuẩn:** `YYYY-MM-DDTHH:MM:SSZ` (ví dụ: `2019-10-01T00:00:00Z`).
- **Quy tắc cấm:**
  - Không sử dụng định dạng CSV gốc có chứa khoảng trắng (ví dụ `2019-10-01 00:00:00 UTC`).
  - Không sử dụng offset múi giờ địa phương (ví dụ `+07:00`).
  - Không truyền epoch timestamp dạng số nguyên (ví dụ `1569888000`) trong trường này.

---

## 7. Quy Định Event Type Rank

Kế thừa bảng thứ tự ưu tiên của Người A để giải quyết các sự kiện xảy ra trong cùng một giây (`event_time` trùng nhau):

| `event_type` | `event_type_rank` | Ý nghĩa nghiệp vụ |
|---|---|---|
| `view` | **1** | Xem sản phẩm |
| `cart` | **2** | Thêm vào giỏ hàng |
| `remove_from_cart` | **3** | Xóa khỏi giỏ hàng |
| `purchase` | **4** | Mua hàng thành công |

- Bộ kiểm tra (`validator`) sẽ từ chối bất kỳ message nào có `event_type_rank` không khớp với bảng quy chiếu trên (ví dụ: `event_type = "view"` nhưng gán `rank = 2` là vi phạm hợp đồng).

---

## 8. Quy Định Event Sort Key (Đồng Bộ Mục 5.3 Của Data Contract A)

1. **Mục đích:** Khi cả `event_time` và `event_type_rank` đều trùng nhau, `event_sort_key` đóng vai trò là tiêu chí phân định thứ tự cuối cùng (**tie-breaker tất định**) để đảm bảo kết quả sắp xếp hoàn toàn nhất quán và có thể tái lập (reproducible).
2. **Bản chất kỹ thuật (Không phải UUID):**
   - `event_sort_key` là một tie-breaker tất định dựa trên giá trị dữ liệu, **KHÔNG đảm bảo tính duy nhất tuyệt đối** nếu dữ liệu nguồn xuất hiện các bản ghi trùng lặp hoàn toàn 100% cả 9 trường (duplicate records sẽ sinh ra cùng mã SHA-256).
   - Mục đích duy nhất của nó là giữ thứ tự sắp xếp ổn định, không phụ thuộc vào thứ tự partition vật lý của Spark.
3. **Thuật toán & Định dạng bắt buộc:**
   - Thuật toán băm: **SHA-256**.
   - Định dạng đầu ra: Chuỗi mã băm Hexadecimal chữ thường (lowercase hex), đúng **64 ký tự** (regex `^[a-f0-9]{64}$`).
4. **Thứ tự 9 trường đầu vào cố định (Đồng bộ tuyệt đối với mục 5.3 của Người A):**
   1. `event_time`
   2. `event_type`
   3. `product_id`
   4. `category_id`
   5. `category_code`
   6. `brand`
   7. `price`
   8. `user_id`
   9. `user_session`
5. **Quy tắc tiền xử lý chuỗi trước khi hash:**
   - Các giá trị được chuyển sang biểu diễn chuỗi sau khi đã được chuẩn hóa.
   - Giá trị NULL được thay thế đồng nhất bằng chuỗi `<NULL>`.
   - Ký tự phân cách giữa các trường: chuỗi `||` (hai dấu gạch đứng liên tiếp).
   - Tuyệt đối không sử dụng `monotonically_increasing_id()` vì phụ thuộc vào cách Spark phân chia partition và thực thi.
6. **Bộ ba thứ tự sự kiện chuẩn của Funnel:**
   ```
   (event_time UTC, event_type_rank, event_sort_key)
   ```
   Không sử dụng Kafka arrival time (thời điểm message tới broker) làm thứ tự chuẩn của Funnel.
7. **Trạng thái công thức và điểm lưu ý:**
   - Công thức PySpark của Người A tại `docs/data_contract.md` (mục 5.3) hiện tại mang tính chất **minh họa cấu trúc logic**:
     ```python
     F.sha2(
         F.concat_ws(
             "||",
             *[
                 F.coalesce(
                     F.col(c).cast("string"),
                     "<NULL>"
                 )
                 for c in source_columns
             ]
         ),
         256
     )
     ```
   - Quy chuẩn ép kiểu chuỗi cụ thể đối với kiểu `Timestamp` (ví dụ `YYYY-MM-DD HH:MM:SS` vs `YYYY-MM-DDTHH:MM:SSZ`) và kiểu `Double` (ví dụ `0.0` vs `0.00`) vẫn cần được hai thành viên chốt chính thức qua **Test Vector chuẩn** từ PySpark ETL của Người A.
   - Điển hình tại ví dụ JSON mục 10.3: Mã hash mẫu trong fixture hiện tại được tạo với chuỗi ép kiểu `price = "0.00"`, trong khi payload JSON thể hiện `0.0`. Đây là điểm kỹ thuật mở cần A và B thống nhất cụ thể (xem chi tiết tại Mục 12).
8. **Khuyến nghị kiến trúc cho Producer B2:**
   - Trong môi trường thực tế, **Kafka Producer nên đọc trực tiếp cột `event_sort_key` đã được tính toán sẵn từ Curated Parquet của Người A**.
   - Việc này tránh việc tầng Streaming phải tính toán lại hash SHA-256 trên từng sự kiện (tiết kiệm CPU) và loại bỏ hoàn toàn nguy cơ sai lệch định dạng ép kiểu chuỗi giữa PySpark ETL và Python Producer.

---

## 9. Quy Tắc Xử Lý `user_session = NULL`

1. **Trong tập Curated Parquet:** Vẫn được giữ lại theo quy định tại mục 4.2 của [data_contract.md](./data_contract.md).
2. **Trong Kafka:**
   - Record Value vẫn chứa `"user_session": null`.
   - Record Key bắt buộc gán là `null` (None trong Python).
3. **Trong Stream Processing / ML:** Các sự kiện có `user_session` là NULL sẽ tự động được lọc bỏ trước khi đưa vào tính toán Phễu chuyển đổi (Funnel) và trích xuất đặc trưng Session ML, vì không thể gắn kết hành vi vào bất kỳ phiên nào.

---

## 10. JSON Message Minh Họa (Simulated Test Data)

> ℹ️ **Ghi chú:** Các ví dụ JSON dưới đây là **dữ liệu thử nghiệm mô phỏng (simulated test data)** được trích từ file fixture `tests/fixtures/kafka_records_DRAFT.jsonl` phục vụ kiểm tra Schema và Unit Test.

### 10.1. Bản ghi tiêu chuẩn (Đầy đủ thông tin)
```json
{
  "key": "d6c3d980-d281-4203-bc85-999ac65daec9",
  "value": {
    "event_time": "2019-10-01T00:00:00Z",
    "event_type": "view",
    "product_id": 1004856,
    "category_id": 2053013555631882655,
    "category_code": "electronics.smartphone",
    "brand": "samsung",
    "price": 130.25,
    "user_id": 520088429,
    "user_session": "d6c3d980-d281-4203-bc85-999ac65daec9",
    "event_type_rank": 1,
    "event_sort_key": "1e2cfbffbd40220c027a1d6ae279d4586ce479677b6916c7bf1e431bb6dca92a",
    "schema_version": "DRAFT-S2-01"
  }
}
```

### 10.2. Bản ghi có `user_session = null` và `category_code = null`
```json
{
  "key": null,
  "value": {
    "event_time": "2019-10-01T00:00:20Z",
    "event_type": "view",
    "product_id": 5100816,
    "category_id": 2053013553375346967,
    "category_code": null,
    "brand": "xiaomi",
    "price": 29.99,
    "user_id": 512741982,
    "user_session": null,
    "event_type_rank": 1,
    "event_sort_key": "58ebd7cb26b36fb302686b830f6e70790c177f6e0f2ab807917f45e7c434b7ec",
    "schema_version": "DRAFT-S2-01"
  }
}
```

### 10.3. Bản ghi có `price = 0.0` và `brand = "Unknown"`
```json
{
  "key": "f4a20b92-6242-4217-b712-70b92c6b412e",
  "value": {
    "event_time": "2019-10-01T00:00:35Z",
    "event_type": "view",
    "product_id": 3801158,
    "category_id": 2053013566100865955,
    "category_code": null,
    "brand": "Unknown",
    "price": 0.0,
    "user_id": 535878005,
    "user_session": "f4a20b92-6242-4217-b712-70b92c6b412e",
    "event_type_rank": 1,
    "event_sort_key": "f5d7a6efb54eb8a9626b62267a7170f798c546cf8f4e88847caab19357e629bf",
    "schema_version": "DRAFT-S2-01"
  }
}
```
*(Ghi chú: Giá trị `event_sort_key` trong mẫu 10.3 hiện được sinh với chuỗi hash có `price` là `"0.00"`. Giá trị này sẽ được chuẩn hóa đồng bộ khi hai bên chốt test vector chính thức).*

---

## 11. Hướng Dẫn Chạy Kiểm Thử Offline

Toàn bộ quy trình kiểm thử đều chạy ngoại tuyến, không cần broker Kafka hay cụm Spark:

1. **Kiểm tra tính hợp lệ của file JSON Schema:**
   ```bash
   python -m json.tool src/streaming/schemas/kafka_event_schema_DRAFT.json
   ```
2. **Kiểm tra bộ dữ liệu mẫu JSONL bằng validator độc lập:**
   ```bash
   python scripts/validate_kafka_contract.py tests/fixtures/kafka_records_DRAFT.jsonl
   ```
   *(Đã kiểm thử: 6/6 records PASS)*.
3. **Chạy trọn bộ Unit Test tự động (20 test cases):**
   ```bash
   python -m unittest tests/unit/test_kafka_contract.py -v
   ```
   *(Hoặc sử dụng `python -m pytest tests/unit/test_kafka_contract.py -v`)*.
   - Kết quả xác minh thực tế: **20/20 test cases PASS** (bao gồm 15 bài test quy tắc nghiệp vụ ban đầu và 5 bài test mở rộng kiểm tra đường dẫn schema không tồn tại, schema không hợp lệ, và mã thoát CLI).

---

## 12. Những Điểm Cần Người A và Người B Chốt Chính Thức (Mốc H04)

| # | Hạng mục | Hiện trạng | Đề xuất / Điểm cần thống nhất |
|---|---|---|---|
| 1 | **Chuẩn hóa chuỗi trước khi hash SHA-256** | Công thức PySpark minh họa tại mục 5.3 của `data_contract.md`. | Thống nhất cách ép kiểu chuỗi cho `event_time` (chuẩn ISO hay khoảng trắng), `price` (`0.0` hay `0.00`), và cách chuẩn hóa rỗng/NULL trước khi hash. |
| 2 | **Cung cấp Test Vector từ Spark ETL** | Chưa có bộ dữ liệu kiểm chứng chéo giữa hai tầng. | Người A cung cấp 3-5 test vector cụ thể (chuỗi input gốc và hash SHA-256 tương ứng) từ pipeline PySpark ETL để Người B tích hợp kiểm thử đối chiếu. |
| 3 | **Tên Kafka Topic chính thức** | Cấu hình ghi `ecommerce.clickstream.raw`, cụm VM-B đang có `ecommerce_events`. | Thống nhất 01 tên duy nhất cho cả `config.yaml` và kịch bản vận hành thực tế. |
| 4 | **Phiên bản Schema (`schema_version`)** | Hiện dùng hằng số tạm thời `"DRAFT-S2-01"`. | Đề xuất chuyển sang semantic versioning `"1.0.0"` khi hợp nhất chính thức vào mốc H04. |
| 5 | **Định dạng Timestamp trong JSON Payload** | Hiện quy định chuẩn ISO-8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`). | Xác nhận định dạng này đã tối ưu cho cả Kafka Consumer và Spark Structured Streaming hay cần điều chỉnh. |
| 6 | **Quy trình phê duyệt và hợp nhất tài liệu** | Tách riêng tài liệu đề xuất tại `docs/kafka_contract_DRAFT.md`. | Sau khi Người A rà soát, chính thức cập nhật và sáp nhập nội dung này vào **Mục 6** của [data_contract.md](./data_contract.md). |

---

## 13. Checklist Nghiệm Thu Tiêu Chí H04

- [x] Schema JSON bao phủ đủ 12 trường dữ liệu đề xuất (`kafka_event_schema_DRAFT.json`).
- [x] Đã cấu hình cấm thuộc tính thừa (`additionalProperties: false`).
- [x] Đã có quy chuẩn cho Kafka Key khi `user_session` là NULL (`key = null`).
- [x] Đã tạo file dữ liệu mẫu JSONL tại `tests/fixtures/kafka_records_DRAFT.jsonl`.
- [x] Đã viết script kiểm tra độc lập tại `scripts/validate_kafka_contract.py` (sử dụng `Draft7Validator.check_schema`, xử lý lỗi chặt chẽ).
- [x] Đã hoàn thành và xác minh thực tế 20/20 kịch bản Unit Test (15 bài test nghiệp vụ + 5 bài test schema loading & CLI error handling).
- [ ] Người A và Người B đối chiếu test vector `event_sort_key` từ PySpark ETL.
- [ ] Người A review và phê duyệt chính thức hợp đồng Kafka tại mốc H04.

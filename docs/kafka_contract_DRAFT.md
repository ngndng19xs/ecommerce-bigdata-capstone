# Kafka JSON Contract (Bản Đề Xuất DRAFT-S2-01)

> **Task:** P2-B-S2-01 – Kafka JSON Contract  
> **Người đề xuất:** Người B (Streaming & ML)  
> **Người đánh giá (Reviewer):** Người A (Data & Analytics)  
> **Trạng thái:** 🟡 **BẢN ĐỀ XUẤT (DRAFT) – CHỜ NGƯỜI A PHÊ DUYỆT**  
> **Tài liệu tham chiếu:** [docs/data_contract.md](docs/data_contract.md) | [docs/b2_producer_guide.md](docs/b2_producer_guide.md)

---

## 1. Mục Tiêu Task P2-B-S2-01

Task **P2-B-S2-01** do Người B thực hiện nhằm thiết lập quy chuẩn cấu trúc message (Schema Contract) cho luồng sự kiện phát vào Apache Kafka, làm cầu nối đồng bộ giữa tầng **Batch ETL (Người A)** và tầng **Streaming / ML (Người B)**:
- Chuẩn hóa định dạng JSON payload và khóa phân vùng (Kafka Message Key).
- Kế thừa đầy đủ các quy tắc làm sạch dữ liệu, thứ tự ưu tiên sự kiện từ Data Contract của Người A.
- Cung cấp Schema JSON chuẩn (`kafka_event_schema_DRAFT.json`), dữ liệu mẫu (`kafka_records_DRAFT.jsonl`), công cụ kiểm tra độc lập (`validate_kafka_contract.py`) và bộ unit test tự động.
- Chuẩn bị nền tảng kỹ thuật trước khi bước vào nghiệm thu Task **B2-02** (Phát lại dữ liệu chính thức vào Kafka).

> ⚠️ **Lưu ý quan trọng:** Đây là bản dự thảo kỹ thuật do Người B đề xuất. Tài liệu này **KHÔNG thay thế** `docs/data_contract.md` và chưa được áp dụng vào pipeline production cho đến khi có sự thống nhất bằng văn bản giữa hai thành viên.

---

## 2. Những Quy Tắc Kế Thừa Từ Data Contract Của Người A

Bản đề xuất Kafka Contract kế thừa nghiêm ngặt toàn bộ các quyết định về dữ liệu đã được Người A ghi nhận tại `docs/data_contract.md`:

1. **Nguồn dữ liệu:** Bộ dữ liệu hành vi thương mại điện tử REES46 (tháng 10 và tháng 11/2019), gốc UTC.
2. **Dữ liệu đầu vào của Producer:** Kafka Producer trên VM-B tiêu thụ dữ liệu từ thư mục **Curated Parquet** (`/data/curated/events_cleaned`) được bàn giao từ VM-A, không tự ý làm sạch lại file CSV thô.
3. **Quy tắc làm sạch kế thừa:**
   - `brand`: Các giá trị NULL hoặc rỗng trong dữ liệu Curated đã được chuyển đổi thành chuỗi `"Unknown"`. Do đó, message phát vào Kafka **bắt buộc không được để rỗng hoặc NULL**.
   - `category_code`: Giữ nguyên NULL nếu nguồn thiếu.
   - `price = 0`: Được công nhận là hợp lệ (dữ liệu khuyến mãi/quà tặng, có ~269.664 bản ghi trong tập Curated).
   - `price < 0` hoặc NULL: Bị loại bỏ từ bước Quarantine của ETL, không bao giờ xuất hiện trong Curated.
   - `user_id`: Bắt buộc không NULL trong dữ liệu Curated.
   - `user_session`: Được phép NULL trong Curated (nhưng có quy tắc phân vùng và tính toán riêng).

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
- Hiện tại trên VM-B đang tồn tại topic `ecommerce_events`.
- **Đề xuất thống nhất:** Cần Người A và Người B chốt chính thức sử dụng tên topic `ecommerce_events` (chuẩn tên bảng) hay `ecommerce.clickstream.raw` (chuẩn domain entity) trước khi tiến hành nghiệm thu B2-02.

---

## 4. Quy Định Kafka Message Key

Khóa của bản ghi Kafka đóng vai trò quyết định phân vùng (`partitioning`) và đảm bảo tính tuần tự của sự kiện:

1. **Khi `user_session` có giá trị (Khác NULL):**
   - **Kafka Key** bắt buộc phải là chuỗi byte UTF-8 của chính `user_session` đó.
   - *Mục đích:* Đảm bảo tất cả các hành động trong cùng một phiên của người dùng được phân phối vào **cùng một partition**, duy trì trật tự thời gian chính xác cho Consumer tính toán phễu chuyển đổi (Funnel).
2. **Khi `user_session` là NULL:**
   - **Kafka Key** bắt buộc phải mang giá trị **`null` (None trong Python)**.
   - **TUYỆT ĐỐI CẤM:** Không được chuyển thành chuỗi ký tự `"null"` hoặc `"None"`, vì điều này sẽ gom toàn bộ các sự kiện mồ côi vào chung một partition duy nhất gây lệch tải nghiêm trọng.
   - *Hành vi Kafka:* Khi key là `null`, Kafka partitioner mặc định sẽ phân phối bản ghi theo thuật toán Round-Robin hoặc Sticky Partitioner ngẫu nhiên giữa các partition.
3. **Độc lập giữa Key và Value:**
   - Kafka Key và Kafka Value JSON là hai thành phần tách biệt trong cấu trúc một Kafka Record. Trường `user_session` vẫn xuất hiện đầy đủ bên trong payload JSON Value.

---

## 5. Kafka JSON Value Schema (12 Trường)

File định nghĩa JSON Schema chuẩn: [`src/streaming/schemas/kafka_event_schema_DRAFT.json`](../src/streaming/schemas/kafka_event_schema_DRAFT.json).

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
| 12| `schema_version` | `string` | **Có** | Hằng số đề xuất: `"DRAFT-S2-01"` | Phiên bản hợp đồng message Kafka |

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

## 8. Quy Định Event Sort Key

1. **Mục đích:** Khi cả `event_time` và `event_type_rank` đều trùng nhau, `event_sort_key` là khóa phân định thứ tự cuối cùng, đảm bảo tính tất định (deterministic).
2. **Định dạng bắt buộc:** Chuỗi mã băm Hexadecimal chữ thường, đúng **64 ký tự** của thuật toán SHA-256 (`^[a-f0-9]{64}$`).
3. **Thứ tự 9 trường đầu vào (theo hợp đồng Người A):**
   1. `event_time`
   2. `event_type`
   3. `product_id`
   4. `category_id`
   5. `category_code`
   6. `brand`
   7. `price`
   8. `user_id`
   9. `user_session`
4. **Khuyến nghị kiến trúc cho Producer:**
   - Trong môi trường thực tế, **Kafka Producer nên đọc trực tiếp cột `event_sort_key` đã được tính toán sẵn từ Curated Parquet của Người A**.
   - Việc này tránh việc tầng Streaming phải tính toán lại hash SHA-256 trên từng sự kiện (tiết kiệm CPU) và loại bỏ hoàn toàn nguy cơ sai lệch định dạng ép kiểu chuỗi giữa PySpark ETL và Python Producer.

---

## 9. Quy Tắc Xử Lý `user_session = NULL`

1. **Trong tập Curated:** Vẫn được giữ lại theo quy định tại mục 4.2 của `docs/data_contract.md`.
2. **Trong Kafka:**
   - Record Value vẫn chứa `"user_session": null`.
   - Record Key bắt buộc gán là `null` (None).
3. **Trong Stream Processing / ML:** Các sự kiện có `user_session` là NULL sẽ tự động được lọc bỏ trước khi đưa vào tính toán Phễu chuyển đổi (Funnel) và trích xuất đặc trưng Session ML.

---

## 10. JSON Message Minh Họa

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

---

## 11. Hướng Dẫn Chạy Kiểm Thử Offline

Toàn bộ quy trình kiểm thử đều chạy ngoại tuyến, không cần broker Kafka hay cụm Spark:

1. **Kiểm tra tính hợp lệ của file JSON Schema:**
   ```bash
   python -m json.tool src/streaming/schemas/kafka_event_schema_DRAFT.json > /dev/null
   ```
2. **Kiểm tra bộ dữ liệu mẫu JSONL bằng script độc lập:**
   ```bash
   python scripts/validate_kafka_contract.py tests/fixtures/kafka_records_DRAFT.jsonl
   ```
3. **Chạy trọn bộ Unit Test kiểm thử biên và negative tests:**
   ```bash
   python -m unittest tests/unit/test_kafka_contract.py -v
   ```
   *(Hoặc sử dụng `pytest tests/unit/test_kafka_contract.py -v` nếu môi trường đã cài đặt pytest)*.

---

## 12. Những Điểm Cần Người A và Người B Chốt Chính Thức

| # | Hạng mục | Hiện trạng | Điểm cần thảo luận / Thống nhất |
|---|---|---|---|
| 1 | **Tên Topic chính thức** | Cấu hình ghi `ecommerce.clickstream.raw`, VM-B có `ecommerce_events`. | Thống nhất 01 tên duy nhất cho cả `config.yaml` và Kafka scripts. |
| 2 | **Cột `event_sort_key`** | Hiện mô tả PySpark minh họa tại `docs/data_contract.md`. | Xác nhận Người A sẽ lưu sẵn cột `event_sort_key` trong Curated Parquet hay Producer tự băm. |
| 3 | **Cột `event_type_rank`** | Chưa có trong bảng schema Parquet ở mục 2 của contract. | Xác nhận có ghi cột này ra Parquet Curated không, hay Producer tự map từ `event_type`. |
| 4 | **Phê duyệt Schema** | Đang mang nhãn `DRAFT-S2-01`. | Người A review bản đề xuất này và cập nhật trạng thái vào Mục 6 của `docs/data_contract.md`. |

---

## 13. Checklist Nghiệm Thu Tiêu Chí H04

- [x] Schema JSON bao phủ đủ 12 trường dữ liệu đề xuất.
- [x] Đã cấu hình cấm thuộc tính thừa (`additionalProperties: false`).
- [x] Đã có quy chuẩn cho Kafka Key khi `user_session` là NULL.
- [x] Đã tạo file dữ liệu mẫu JSONL tại `tests/fixtures/kafka_records_DRAFT.jsonl`.
- [x] Đã viết script kiểm tra độc lập tại `scripts/validate_kafka_contract.py`.
- [x] Đã hoàn thành 15 kịch bản unit test (bao gồm các ca kiểm thử phủ định).
- [ ] Người A review và phê duyệt chính thức hợp đồng Kafka.


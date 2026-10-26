# Data Dictionary bảng sự kiện Hive và Parquet

Ngày cập nhật: 26/10/2026. Phạm vi: cấu trúc được triển khai bởi [clean_events.py](../src/batch/etl/clean_events.py), đối chiếu với [schema đã xuất](../data/processed/t003_full_run_01/quality/schema/) và [báo cáo chất lượng](../data/processed/t003_full_run_01/quality/).

## 1. Thông tin tập dữ liệu

| Thuộc tính | Giá trị |
|---|---|
| Tên logic | Sự kiện tương tác thương mại điện tử |
| Tên bảng Hive dùng trong hướng dẫn tích hợp | ecommerce_db.fact_events_t003_v1 |
| Đơn vị một dòng | Một bản ghi sự kiện nguồn được giữ lại sau kiểm tra ETL |
| Khóa chính | Chưa có event_id hoặc khóa duy nhất được xác lập; không coi user_session là khóa chính |
| Định dạng | Parquet, nén Snappy |
| Phân vùng | event_date, ngày UTC của từng sự kiện |
| Parquet local | data/processed/t003_full_run_01/events |
| Phạm vi nguồn | Hai file CSV tháng 10 và tháng 11/2019 |
| Số dòng sạch đã xác nhận local | 109.950.731 |
| Số phân vùng đã xác nhận local | 61 |
| Bảng Hive | External table theo DDL tích hợp bên dưới; cần đối chiếu DESCRIBE FORMATTED trên môi trường đang chạy |

Đường dẫn HDFS đã sử dụng gồm /ecommerce/curated/t003_full_run_01/events và /ecommerce/curated/t003_upload_02/events. Tài liệu không tự chọn LOCATION cuối cùng: cần xác nhận nơi nạp đủ dữ liệu và LOCATION của từng partition trước khi dùng bảng cho phân tích.

## 2. Từ điển cột dữ liệu sạch

Kiểu dưới đây là kiểu logic Spark SQL/Hive, không phải mô tả kiểu vật lý bên trong footer Parquet. Ví dụ cột decimal có kiểu logic DECIMAL(18,2), không suy đoán cách mã hóa vật lý từ báo cáo schema.

| Cột | Kiểu Spark SQL / Hive | Ý nghĩa | Quy tắc ETL hiện tại | Null theo quy tắc dữ liệu sạch |
|---|---|---|---|---|
| event_time | TIMESTAMP | Thời điểm sự kiện, xử lý theo UTC | Trim; chỉ nhận chuỗi nguồn dạng yyyy-MM-dd HH:mm:ss UTC; parse thành timestamp; lỗi parse bị tách riêng | Không |
| event_type | STRING | Loại tương tác | Trim, lowercase; chỉ nhận view, cart, purchase, remove_from_cart | Không |
| product_id | BIGINT | Mã sản phẩm | Chuỗi chỉ chứa chữ số được cast thành long 64 bit; yêu cầu lớn hơn 0 | Không |
| category_id | BIGINT | Mã danh mục dạng số của nguồn | Chuỗi chỉ chứa chữ số được cast thành long; giá trị không hợp lệ hoặc không dương thành null; không loại cả dòng chỉ vì cột này | Có |
| category_code | STRING | Mã/đường dẫn phân cấp danh mục nguồn, ví dụ electronics.smartphone | Trim; chuỗi rỗng thành null; không tự suy ra từ category_id | Có |
| brand | STRING | Thương hiệu sản phẩm theo nguồn | Trim; chuỗi rỗng thành null; chưa chuyển thành unknown trong ETL hiện tại | Có |
| price | DECIMAL(18,2) | Giá sản phẩm ghi tại sự kiện | Cast số thập phân tối đa 18 chữ số, 2 chữ số phần lẻ; giá null, không cast được, tràn kiểu hoặc âm bị tách riêng; cast có thể làm tròn phần lẻ | Không |
| user_id | BIGINT | Mã người dùng | Chuỗi chỉ chứa chữ số được cast thành long 64 bit; yêu cầu lớn hơn 0 | Không |
| user_session | STRING | Mã phiên truy cập dùng gom sự kiện cho funnel | Trim; chuỗi rỗng thành null và bị tách riêng; không tự tạo mã thay thế; chưa kiểm tra định dạng UUID | Không |
| source_file | STRING | Đường dẫn file nguồn Spark đã đọc | Bổ sung bằng input_file_name(); có thể là URI local hoặc URI của hệ thống lưu trữ | Được sinh bởi ETL; chưa có quy tắc loại riêng và chưa có thống kê null trong báo cáo hiện tại |
| event_date | DATE | Ngày diễn ra sự kiện theo UTC | to_date(event_time) trong Spark session có timezone UTC; dùng làm partition | Không, suy ra từ event_time hợp lệ |

Các giá trị ví dụ chỉ giải thích định dạng, không đại diện cho khóa duy nhất. price chưa được pipeline gắn mã tiền tệ; cần xác nhận tài liệu nguồn trước khi báo cáo theo một đơn vị tiền cụ thể.

Metadata của schema sau khi đọc lại Parquet đang thể hiện nullable=true cho cả 11 cột. Đây là thuộc tính schema của bộ đọc, không phải khai báo rằng dữ liệu sạch được phép thiếu mọi trường. Các yêu cầu không null trong bảng trên được kiểm soát bằng logic ETL, chưa phải ràng buộc NOT NULL được Hive cưỡng chế.

## 3. Chất lượng dữ liệu đã đo

Mẫu số của thống kê null là 109.950.731 dòng sạch.

| Cột | Số null sau làm sạch | Ghi chú |
|---|---:|---|
| event_time | 0 | Trường bắt buộc |
| event_date | 0 | Cột phân vùng UTC |
| event_type | 0 | Trường bắt buộc |
| product_id | 0 | Trường bắt buộc |
| category_id | 0 | ETL vẫn cho phép null khi nguồn khác có lỗi/thiếu |
| category_code | 35.413.777 | Không loại dòng chỉ vì thiếu danh mục |
| brand | 15.331.241 | Không loại dòng chỉ vì thiếu thương hiệu |
| price | 0 | Trường bắt buộc |
| user_id | 0 | Trường bắt buộc |
| user_session | 0 | 12 dòng nguồn thiếu mã phiên được tách riêng |
| source_file | Chưa đo trong báo cáo null hiện tại | Không suy ra số null bằng 0 từ việc cột được sinh tự động |

Đối chiếu số lượng theo [summary](../data/processed/t003_full_run_01/quality/summary/):

```text
109.950.743 đầu vào = 109.950.731 sạch + 12 bị loại
Số dòng sạch đọc lại Parquet = số dòng sạch trước ghi
```

Theo [event_types](../data/processed/t003_full_run_01/quality/event_types/), dữ liệu sạch có view = 104.335.509, cart = 3.955.434, purchase = 1.659.788. remove_from_cart được phép theo schema nghiệp vụ nhưng không xuất hiện trong bảng thống kê của lần chạy này.

## 4. Bố trí Parquet và phân vùng

```text
events/
  event_date=2019-10-01/
    part-....snappy.parquet
  event_date=2019-10-02/
    part-....snappy.parquet
  ...
```

Với cách ghi partitionBy(event_date), giá trị event_date được mã hóa trong đường dẫn thư mục; không cần có cột này trong từng file dữ liệu. Đọc tại thư mục gốc events để Spark nhận diện partition. Trong Hive, khai báo event_date ở PARTITIONED BY, không khai báo trùng trong danh sách cột thường.

## 5. DDL tích hợp Hive

Đây là DDL tham chiếu khớp schema Parquet hiện tại, không phải bằng chứng bảng đã được tạo trên server. Thay LOCATION_PLACEHOLDER bằng URI HDFS đã xác nhận đủ dữ liệu trước khi chạy.

```sql
CREATE DATABASE IF NOT EXISTS ecommerce_db;

CREATE EXTERNAL TABLE ecommerce_db.fact_events_t003_v1 (
    event_time TIMESTAMP,
    event_type STRING,
    product_id BIGINT,
    category_id BIGINT,
    category_code STRING,
    brand STRING,
    price DECIMAL(18,2),
    user_id BIGINT,
    user_session STRING,
    source_file STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'LOCATION_PLACEHOLDER';

MSCK REPAIR TABLE ecommerce_db.fact_events_t003_v1;
```

Nếu bảng đã tồn tại, kiểm tra trước thay vì chạy CREATE lại:

```sql
SHOW TABLES IN ecommerce_db;
DESCRIBE FORMATTED ecommerce_db.fact_events_t003_v1;
SHOW PARTITIONS ecommerce_db.fact_events_t003_v1;
```

Khi đổi LOCATION cấp bảng, các partition có thể vẫn giữ LOCATION cũ. Cần kiểm tra metadata partition và cập nhật theo đường dẫn thực tế. Giữ timezone UTC ở các công cụ truy vấn timestamp; TIMESTAMP không mang một nhãn múi giờ riêng cho từng bản ghi.

## 6. Dữ liệu bị loại

Vùng local: [rejected_events](../data/processed/t003_full_run_01/rejected_events/), định dạng Parquet, không partitionBy(event_date) trong mã hiện tại.

Ngoài các cột đã chuẩn hóa của dòng sự kiện, vùng này giữ:

| Cột bổ sung | Kiểu logic | Ý nghĩa |
|---|---|---|
| _corrupt_record | STRING | Nội dung CSV mà Spark đánh dấu không đọc được theo schema |
| raw_record | STRUCT | Chín trường nguồn được giữ dưới dạng STRING trước bước trim/cast để truy vết |
| error_reason | STRING | Một hoặc nhiều nhãn lỗi, phân cách bằng dấu chấm phẩy |

Nhãn lỗi được sinh: malformed_csv, invalid_event_time, invalid_event_type, invalid_product_id, invalid_user_id, invalid_price, missing_user_session. Một dòng có thể có nhiều nhãn. Lần chạy hiện tại có 12 dòng bị loại với nhãn missing_user_session.

## 7. Quy ước dùng cho analytics và ML

- user_session là khóa gom nhóm phiên, không phải khóa duy nhất của bảng sự kiện. Một phiên có nhiều dòng, một người dùng có nhiều phiên.
- Chưa có event_id và chưa có bước khử trùng lặp trong pipeline hiện tại; không cam kết uniqueness cho một tổ hợp cột tùy ý.
- event_date là ngày của sự kiện; session_date là ngày UTC của sự kiện đầu tiên trong phiên và chưa có trong bảng này.
- Các trường first_view_time, first_cart_after_view_time, has_purchase_ordered, n_remove và các tỷ lệ chuyển đổi thuộc dữ liệu tổng hợp phía sau, chưa có trong bảng sự kiện.
- Không loại cart/purchase chỉ vì sự kiện không tuân thứ tự funnel. Kiểm tra thứ tự được thực hiện ở tầng phân tích phiên.
- Khi phân tích brand/category, chuẩn hóa nhóm thiếu thành unknown theo đặc tả; dữ liệu Parquet gốc hiện vẫn lưu null. Chốt category_id hay category_code làm chiều category trước khi so sánh kết quả.
- Khi tạo đặc trưng ML, chỉ dùng sự kiện sẵn có trước thời điểm dự đoán.
- BIGINT cho product_id/user_id/category_id là hợp đồng triển khai hiện tại. Nếu nhóm chọn STRING, phải đồng bộ ETL, Parquet, Hive và mã downstream; không chỉ sửa DDL để đọc dữ liệu cũ.

## 8. Checklist xác nhận với TV2 và TV4

1. Xác nhận tên bảng và LOCATION thực tế bằng DESCRIBE FORMATTED.
2. Xác nhận 61 partition và COUNT(*) = 109.950.731 trên đúng tập HDFS đầy đủ.
3. Chốt sử dụng category_id hoặc category_code cho từng báo cáo.
4. Chốt tầng chuẩn hóa unknown và kiểu của các mã định danh.
5. Lưu DDL đã điền LOCATION cùng kết quả kiểm tra để tái lập.

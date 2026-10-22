# BÁO CÁO THỰC HIỆN NHIỆM VỤ T002

## 1. Thông tin nhiệm vụ

| Nội dung | Thông tin |
|---|---|
| Mã việc | T002 |
| Tuần | Tuần 1 |
| Thời gian theo kế hoạch | Ngày 3–4 (19/10–20/10) |
| Thành viên | TV1 |
| Vai trò | Data Engineer / Storage |
| Phối hợp | TV2; TV4 |
| Ngày cập nhật báo cáo | 22/10/2026 |
| Sản phẩm đầu ra yêu cầu | Pipeline ETL bước đầu; quy tắc làm sạch và schema thống nhất |

**Nhiệm vụ được giao:** “Viết pipeline đọc dữ liệu bằng Spark; chuẩn hóa kiểu dữ liệu, xử lý giá trị thiếu/bản ghi không hợp lệ và tạo event_date theo ngày UTC.”

**Trạng thái kỹ thuật:** Đã triển khai pipeline và ghi dữ liệu tháng 11/2019 ra Parquet; đã đọc lại kết quả để đối chiếu số lượng và schema. Quy tắc và schema dưới đây phản ánh phiên bản triển khai hiện tại, cần TV2 và TV4 xác nhận trước khi coi là hợp đồng dữ liệu thống nhất của nhóm. Báo cáo không tự xác nhận nghiệm thu hoặc hoàn thành đúng hạn kế hoạch.

## 2. Dữ liệu đầu vào và phạm vi

- Nguồn: [eCommerce behavior data from multi category store – Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
- Tệp đã xử lý: [2019-Nov.csv](../data/raw/archive/2019-Nov.csv), dung lượng 9.006.762.395 byte, gồm 9 cột nguồn.
- Mã nguồn: [clean_events.py](../src/batch/etl/clean_events.py).
- Phạm vi nghiệm chứng: dữ liệu tháng 11/2019. Chưa báo cáo xử lý tháng 10 hoặc toàn bộ dataset.

Pipeline thực hiện luồng:

```text
CSV nguồn → đọc bằng Spark với schema tường minh
          → chuẩn hóa chuỗi và kiểu dữ liệu
          → tạo event_date theo UTC, kiểm tra quy tắc
          → tách bản ghi hợp lệ / bản ghi lỗi
          → ghi Parquet và kiểm tra đầu ra
```

## 3. Nội dung đã triển khai

### 3.1. Đọc dữ liệu bằng Spark

Sử dụng PySpark 3.5.9, đọc CSV có dòng tiêu đề bằng `spark.read.csv()`. Các cột nguồn được khai báo ban đầu là `StringType`, không dùng suy luận schema tự động. Cách này cho phép giữ giá trị đầu vào trước khi ép kiểu và truy vết dữ liệu lỗi.

Chế độ đọc `PERMISSIVE` và cột `_corrupt_record` phục vụ nhận diện bản ghi mà bộ đọc CSV xác định là lỗi cấu trúc. Pipeline bổ sung `source_file` để ghi nhận tệp nguồn và `raw_record` để giữ 9 giá trị gốc dưới dạng struct. Kiểm tra cấu trúc CSV không thay thế kiểm tra nội dung nghiệp vụ.

### 3.2. Quy tắc làm sạch hiện tại

| Đối tượng | Quy tắc và cách xử lý |
|---|---|
| Tất cả 9 cột nguồn | Xóa khoảng trắng ở hai đầu; chuỗi rỗng hoặc toàn khoảng trắng chuyển thành null. |
| `event_time` | Chấp nhận chuỗi dạng `yyyy-MM-dd HH:mm:ss UTC`; chuyển thành timestamp. Thiếu, sai mẫu hoặc thời gian không hợp lệ được phân loại lỗi. |
| `event_type` | Chuyển về chữ thường; hiện chấp nhận `view`, `cart`, `purchase`, `remove_from_cart`. Thiếu hoặc ngoài danh sách được phân loại lỗi. |
| `product_id`, `user_id` | Chỉ nhận chuỗi gồm chữ số, chuyển sang số nguyên 64 bit; yêu cầu lớn hơn 0. Thiếu, sai kiểu, vượt miền kiểu hoặc không dương được phân loại lỗi. |
| `category_id` | Chuyển thành số nguyên 64 bit; thiếu, sai kiểu hoặc không dương chuyển thành null; không loại toàn bộ sự kiện. |
| `category_code`, `brand` | Cho phép null sau chuẩn hóa, không tự suy đoán hoặc điền nhãn thay thế. |
| `price` | Chuyển thành decimal(18,2); thiếu, ép kiểu thất bại hoặc giá âm được phân loại lỗi. Chấp nhận giá bằng 0; không điền giá thiếu bằng 0. |
| `user_session` | Yêu cầu không null sau chuẩn hóa. Phiên bị thiếu được phân loại lỗi. Chưa kiểm tra định dạng UUID. |

Các bản ghi lỗi được tách sang đầu ra riêng, không bị xóa khỏi dữ liệu nguồn. Một bản ghi có thể có nhiều lý do lỗi, nối bằng dấu `;` trong `error_reason`.

Các nhãn lỗi: `malformed_csv`, `invalid_event_time`, `invalid_event_type`, `invalid_product_id`, `invalid_user_id`, `invalid_price`, `missing_user_session`.

Lưu ý phạm vi: pipeline hiện chưa khử trùng lặp, chưa thống kê tỷ lệ thiếu từng cột và chưa kiểm tra miền nghiệp vụ đầy đủ của danh mục sản phẩm. Việc chỉ có 10 bản ghi bị loại không đồng nghĩa chỉ có 10 giá trị thiếu trong nguồn. Các cột tùy chọn vẫn được phép null. Chuyển giá sang decimal(18,2) có thể làm tròn nguồn có nhiều hơn hai chữ số thập phân.

### 3.3. Tạo ngày theo UTC

Pipeline đặt múi giờ phiên Spark trước khi chuyển kiểu và tạo ngày:

```python
.config("spark.sql.session.timeZone", "UTC")
```

Sau khi phân tích `event_time` thành timestamp, tạo cột ngày:

```python
.withColumn("event_date", F.to_date("event_time"))
```

Hậu tố `UTC` trong mẫu đọc được xử lý như chuỗi cố định; múi giờ thực tế được xác định bởi cấu hình phiên Spark là UTC. Dữ liệu đã ở UTC nên không cộng 7 giờ. Ví dụ minh họa: `2019-11-01 23:30:00 UTC` phải có `event_date = 2019-11-01`, dù thời điểm đó đã sang ngày kế tiếp theo giờ Việt Nam. Ví dụ này giải thích quy tắc, không phải kết quả của một kiểm thử biên ngày riêng.

## 4. Schema đầu ra hiện tại

Schema sau đây đã được xác minh bằng cách đọc lại Parquet:

| Cột | Kiểu Spark | Điều kiện ở đầu ra hợp lệ |
|---|---|---|
| `event_time` | timestamp | Phân tích thành công; xử lý theo UTC |
| `event_type` | string | Thuộc tập sự kiện được chấp nhận |
| `product_id` | bigint / long | Không null, lớn hơn 0 |
| `category_id` | bigint / long | Cho phép null; nếu có thì lớn hơn 0 |
| `category_code` | string | Cho phép null |
| `brand` | string | Cho phép null |
| `price` | decimal(18,2) | Không null, không âm |
| `user_id` | bigint / long | Không null, lớn hơn 0 |
| `user_session` | string | Không null, không rỗng sau trim |
| `source_file` | string | Đường dẫn tệp nguồn |
| `event_date` | date | Ngày UTC của `event_time`; khóa phân vùng |

Các điều kiện trên do logic lọc thực thi, không có nghĩa metadata `nullable` của Spark được đổi thành false. Khi đọc toàn bộ thư mục Parquet, Spark khôi phục `event_date` từ tên thư mục phân vùng.

Đầu ra lỗi giữ các cột xử lý, đồng thời bổ sung `raw_record` (struct chứa 9 cột nguồn dạng string), `_corrupt_record` (string) và `error_reason` (string). Đầu ra hợp lệ bỏ ba cột kiểm tra này.

## 5. Kết quả thực hiện và kiểm chứng

| Chỉ tiêu | Kết quả | Bằng chứng |
|---|---:|---|
| Tổng số bản ghi đầu vào | 67.501.979 | Thống kê pipeline trong log |
| Bản ghi hợp lệ | 67.501.969 | Log và đếm lại Parquet |
| Bản ghi bị loại | 10 | Log và đếm lại Parquet |
| Lý do của 10 bản ghi bị loại | `missing_user_session` | Nhóm theo `error_reason` khi đọc lại Parquet |
| Số thư mục phân vùng ngày | 30 | Kiểm tra thư mục đầu ra hợp lệ |
| Dấu hoàn tất ghi | Có `_SUCCESS` ở cả hai đầu ra | Kiểm tra filesystem |

Đối soát số lượng:

```text
67.501.979 = 67.501.969 + 10
Đầu vào   = Hợp lệ     + Bị loại
```

Ngày 22/10/2026, thực hiện một phiên Spark riêng để đọc lại hai đầu ra, gọi `count()`, nhóm lỗi và in schema. Kết quả khớp bảng trên; lệnh kiểm chứng kết thúc với mã thoát 0. Điều này xác nhận đầu ra có thể đọc được và số bản ghi được bảo toàn qua bước phân loại. Chưa có bộ kiểm thử tự động riêng cho toàn bộ các nhánh lỗi hoặc kiểm tra mọi dòng đối với quy tắc UTC.

Đường dẫn sản phẩm:

- [Dữ liệu hợp lệ](../data/processed/nov_2019_run_01/events/): Parquet nén Snappy, phân vùng `event_date=YYYY-MM-DD`.
- [Dữ liệu lỗi](../data/processed/nov_2019_run_01/rejected_events/): Parquet nén Snappy, giữ lý do và dữ liệu gốc để truy vết.
- [Log chạy](../spark-parquet.log): số lượng, schema, mẫu ngày và thông tin thực thi.

Parquet được Spark ghi thành nhiều file `part-...parquet` trong các thư mục, không phải một file duy nhất. Các dấu `_SUCCESS` cùng kết quả đọc lại là bằng chứng ghi thành công; dòng thông báo tiến trình đã bị đóng không được dùng làm bằng chứng thành công.

## 6. Vướng mắc và biện pháp xử lý

| Vướng mắc | Cách xử lý / trạng thái |
|---|---|
| Đọc nhầm CSV rỗng ngoài thư mục giải nén | Đổi đầu vào sang `data/raw/archive/2019-Nov.csv`. |
| `JAVA_HOME` trỏ sai vị trí JDK | Cấu hình JDK 17 đã cài thực tế. |
| `OutOfMemoryError: Java heap space` | Cấp driver 2 GB; chạy `local[2]`; giảm `maxPartitionBytes` xuống 32 MB và batch cache xuống 1.000 dòng. Pipeline dùng cache `DISK_ONLY`. |
| Thiếu thư viện Hadoop cho Windows khi ghi Parquet | Bổ sung `winutils.exe`, `hadoop.dll` từ bản cộng đồng 3.3.5; đặt `HADOOP_HOME` và PATH. Đã thử ghi–đọc 10 dòng, sau đó xác minh đầu ra đầy đủ. |
| Cấu hình không giữ khi đổi terminal | Cần thiết lập biến môi trường trước khi chạy hoặc đóng gói thành script cấu hình dùng chung. |

Môi trường đã xác minh đầu ra: PySpark 3.5.9, JDK 17; môi trường ảo hiện tại vẫn dựa trên Python 3.13.7. Python 3.11.9 đã được cài riêng nhưng không tự thay phiên bản của môi trường ảo cũ. Thư viện Java Hadoop đi kèm PySpark là 3.3.4; thư viện native Windows bổ sung là bản cộng đồng 3.3.5. Việc ghi–đọc thành công xác nhận trường hợp sử dụng hiện tại, không phải chứng nhận tương thích mọi tính năng.

## 7. Bàn giao và nội dung cần thống nhất

| Sản phẩm yêu cầu | Sản phẩm đã có | Trạng thái |
|---|---|---|
| Pipeline ETL bước đầu | Mã PySpark đọc CSV, chuẩn hóa, phân loại và ghi Parquet | Đã triển khai và xác minh đầu ra tháng 11/2019 |
| Quy tắc làm sạch | Bảng quy tắc và nhãn lỗi ở mục 3 | Đã mô tả đúng code; chờ nhóm xác nhận |
| Schema thống nhất | Schema 11 cột hợp lệ và các cột truy vết lỗi ở mục 4 | Đã triển khai; chờ TV2/TV4 thống nhất sử dụng |

Các điểm cần phối hợp với TV2 và TV4:

1. Xác nhận loại sự kiện được chấp nhận, đặc biệt `remove_from_cart` đang có trong code.
2. Xác nhận chính sách loại bản ghi thiếu `user_session` hoặc `price`, và cho phép thiếu thuộc tính danh mục/thương hiệu.
3. Thống nhất kiểu `decimal(18,2)`, quy ước UTC, schema và tên đường dẫn đầu ra cho các bước tiếp theo.
4. Thống nhất cách chạy lại: hiện dùng `errorifexists`, nên phải dùng đường dẫn đầu ra mới hoặc có quy trình xử lý kết quả cũ; không tự ghi đè.

**Kết luận:** TV1 đã thực hiện phần kỹ thuật của T002 trên dữ liệu tháng 11/2019: đọc bằng Spark, chuẩn hóa kiểu, xử lý thiếu/sai theo quy tắc, tạo ngày UTC và xuất Parquet. Kết quả đầu ra đã được đọc lại để xác minh số lượng và schema. Bước bàn giao còn lại là nhóm xác nhận quy tắc và schema để dùng thống nhất; báo cáo không mở rộng kết luận sang các tháng chưa xử lý hoặc các kiểm thử chưa thực hiện.

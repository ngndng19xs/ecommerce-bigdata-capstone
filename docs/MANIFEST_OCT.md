# MANIFEST\_OCT.md

## Dataset Manifest – Curated October 2019

> \\\*\\\*Owner:\\\*\\\* Người A  
> \\\*\\\*Reviewer:\\\*\\\* Người B  
> \\\*\\\*Dataset:\\\*\\\* REES46 eCommerce Behavior Data  
> \\\*\\\*Phạm vi:\\\*\\\* 2019-10-01 đến 2019-10-31 (UTC)  
> \\\*\\\*Trạng thái:\\\*\\\* Bản ghi nhận kết quả hiện có; các mục checksum, Git và xác nhận CSV corrupt cần hoàn tất trước khi bàn giao chính thức.

\---

# 1\. Dataset Manifest

## 1.1. Thông tin chung

|Hạng mục|Giá trị|
|-|-|
|Dataset|REES46 eCommerce Behavior Data|
|Source file|`/data/raw/ecommerce\\\_events/2019-Oct.csv`|
|Source format|CSV|
|Curated output (partitioned)|`/data/curated/events/`|
|Curated output (cleaned, non-partitioned)|`/data/curated/events\\\_cleaned/`|
|Rejected output|`/data/curated/\\\_quarantine/invalid\\\_events`|
|Data scope|October 2019|
|Timezone|UTC|
|Output format bàn giao|Parquet|
|Compression|Snappy|
|Partition column|`event\\\_date`|
|Partition layout|`event\\\_date=YYYY-MM-DD`|
|Raw schema version|`raw-v1.0`|
|Curated schema version|`curated-v1.0`|

**Lưu ý về đường dẫn:** Hai thư mục Curated không đồng nhất:

* `/data/curated/events/` là đầu ra Parquet đã phân vùng theo `event\\\_date`; đây là bộ dữ liệu partitioned được chọn làm đầu ra bàn giao chính.
* `/data/curated/events\\\_cleaned/` là đầu ra Parquet đã làm sạch nhưng chưa phân vùng.
* Các thống kê partition/file trong báo cáo này áp dụng cho `/data/curated/events/`. Số dòng Curated trong audit hiện được ghi nhận từ kết quả kiểm tra bộ dữ liệu làm sạch; trước khi chốt bàn giao cần xác nhận lại số dòng trực tiếp trên `/data/curated/events/` để bảo đảm hai đầu ra tương ứng cùng một lần ETL.

## 1.2. Schema version

### Raw schema – `raw-v1.0`

Schema logic nguồn gồm 9 trường:

|#|Field|Kiểu dữ liệu logic|Nullable theo Data Contract|
|-:|-|-|-|
|1|`event\\\_time`|Timestamp (UTC)|Không\*|
|2|`event\\\_type`|String|Không\*|
|3|`product\\\_id`|Long|Không\*|
|4|`category\\\_id`|Long|Không\*|
|5|`category\\\_code`|String|Có|
|6|`brand`|String|Có|
|7|`price`|Double|Không\*|
|8|`user\\\_id`|Long|Không\*|
|9|`user\\\_session`|String|Có|

Raw CSV được giữ nguyên theo định dạng nguồn. Khi đọc vào Spark, các trường có thể được tiếp nhận dưới dạng String trước khi chuẩn hóa.

### Curated schema – `curated-v1.0`

Schema sau chuẩn hóa và ETL gồm 13 trường:

|#|Field|Kiểu dữ liệu|
|-:|-|-|
|1|`event\\\_time`|Timestamp (UTC)|
|2|`event\\\_type`|String|
|3|`product\\\_id`|String|
|4|`category\\\_id`|String|
|5|`category\\\_code`|String|
|6|`brand`|String|
|7|`price`|Double|
|8|`user\\\_id`|String|
|9|`user\\\_session`|String|
|10|`reject\\\_reason`|String|
|11|`event\\\_type\\\_rank`|Integer|
|12|`event\\\_sort\\\_key`|String|
|13|`event\\\_date`|Date|

Quy ước:

* `event\\\_time` và `event\\\_date` được xử lý theo UTC.
* `event\\\_date` được trích xuất từ `event\\\_time` và dùng làm partition column.
* `event\\\_type\\\_rank`: `view=1`, `cart=2`, `remove\\\_from\\\_cart=3`, `purchase=4`.
* `event\\\_sort\\\_key` là khóa SHA-256 có độ dài 64 ký tự, dùng để phân định thứ tự khi `event\\\_time` và `event\\\_type\\\_rank` trùng nhau.
* `reject\\\_reason` ghi nhận lý do từ chối đối với bản ghi không hợp lệ; trường này có thể tồn tại trong DataFrame đầu ra theo schema ETL.

## 1.3. Quy tắc chất lượng dữ liệu áp dụng

|Trường hợp|Quy tắc|
|-|-|
|`brand` NULL/rỗng|Chuẩn hóa thành `Unknown`, giữ trong Curated|
|`category\\\_code` NULL|Giữ NULL trong Curated|
|`user\\\_session` NULL|Giữ trong Curated; không dùng cho Funnel/ML|
|`event\\\_type` không hợp lệ|Reject|
|`event\\\_time` không parse được|Reject|
|`price` NULL|Reject|
|`price` âm|Reject|
|`price = 0`|Giữ trong Curated|
|`user\\\_id` NULL|Reject|
|CSV sai định dạng|Phải thống kê/ghi nhận riêng, không được bỏ sót âm thầm|

## 1.4. Quy mô và phân vùng Parquet

|Chỉ tiêu|Kết quả ghi nhận|
|-|-:|
|Số ngày/partition|31|
|Khoảng ngày|`2019-10-01` – `2019-10-31`|
|Số file Parquet|744|
|Số file mỗi partition|24|
|Dung lượng trung bình mỗi file|Khoảng 6,9 MB|
|Dung lượng mỗi partition|Khoảng 133,91–194,78 MB|
|Compression|Snappy (theo cấu hình ghi A303)|

Nhận xét: Dữ liệu có đủ 31 thư mục partition theo ngày trong tháng 10. Mỗi partition chứa 24 file Parquet, tổng cộng 744 file. Kích thước file tương đối nhỏ; có thể xem xét tối ưu small files ở giai đoạn sau, nhưng không tự ý gộp hoặc ghi lại dữ liệu trong bước audit này.

## 1.5. Checksum và mã nguồn

|Hạng mục|Giá trị|
|-|-|
|Checksum algorithm|SHA-256|
|Checksum manifest|`SHA256\\\_OCT.txt`|
|Git repository|Chưa cung cấp|
|Git commit hash|`TBD – điền sau khi commit`|
|Git tag|`handoff-curated-oct-v1` – chỉ tạo sau khi Người B review|
|ETL log|ETL\_OCT|
|Audit script/log|audit\_report\_oct\_md|

Không điền checksum hoặc commit hash bằng giá trị giả. SHA-256 cần được tính trên các file Parquet thực tế của `/data/curated/events/` và được Người B tính lại sau khi nhận dữ liệu.

\---

# 2\. Handoff Checklist

* \[ ] Xác nhận số dòng Curated trực tiếp từ `/data/curated/events/`.
* \[ ] Xác minh và ghi nhận số dòng CSV corrupt bằng cơ chế kiểm tra riêng.
* \[ ] Tạo `SHA256\\\_OCT.txt` từ toàn bộ file Parquet trong `/data/curated/events/`.
* \[ ] Lưu ETL log và audit log.
* \[ ] Điền Git commit hash.
* \[ ] Chuyển dữ liệu, manifest và checksum cho Người B.
* \[ ] Người B kiểm tra số dòng, schema, partition và checksum trên VM-B.
* \[ ] Người B xác nhận nghiệm thu.
* \[ ] Chỉ sau khi được duyệt mới tạo Git tag `handoff-curated-oct-v1`.

\---

# 3\. Thông tin xác nhận

|Vai trò|Người phụ trách|Trạng thái|
|-|-|-|
|Owner|Người A|Đã chuẩn bị manifest/audit|
|Reviewer|Người B|Chờ review|
|Ngày bàn giao|TBD|Điền khi bàn giao|
|Git commit|TBD|Điền sau commit|
|Git tag|`handoff-curated-oct-v1`|Chưa tạo – chờ duyệt|




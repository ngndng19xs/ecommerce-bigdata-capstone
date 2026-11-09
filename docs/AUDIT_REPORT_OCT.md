# AUDIT\_REPORT\_OCT.md

## Audit Report – Curated October 2019

> \\\*\\\*Owner:\\\*\\\* Người A  
> \\\*\\\*Reviewer:\\\*\\\* Người B  
> \\\*\\\*Dataset:\\\*\\\* REES46 eCommerce Behavior Data  
> \\\*\\\*Phạm vi:\\\*\\\* 2019-10-01 đến 2019-10-31 (UTC)  
> \\\*\\\*Trạng thái:\\\*\\\* Bản ghi nhận kết quả hiện có; các mục checksum, Git và xác nhận CSV corrupt cần hoàn tất trước khi bàn giao chính thức.

\---

# 1\. Mục tiêu

Đối chiếu số lượng bản ghi Raw, Curated, Rejected và CSV corrupt của tháng October để kiểm tra dữ liệu có bị mất trong quá trình ETL hay không.

Công thức nghiệm thu:

$$ N\_{Raw} = N\_{Curated} + N\_{Rejected} + N\_{Corrupt} $$

Trong đó:

* **Raw:** số bản ghi dữ liệu trong CSV, không tính header.
* **Curated:** số bản ghi hợp lệ đã được làm sạch.
* **Rejected:** số bản ghi đọc được nhưng không đạt điều kiện chất lượng.
* **Corrupt:** số dòng CSV không phân tích đúng cấu trúc, cần theo dõi riêng.

## 1.1. Kết quả đối chiếu số dòng

Các số liệu dưới đây được ghi nhận trong tài liệu kiểm tra A304 đã cung cấp:

|Thành phần|Số bản ghi|
|-|-:|
|Raw October (không tính header)|42.448.764|
|Curated hợp lệ|42.448.764|
|Rejected có cấu trúc|0|
|CSV corrupt được ghi nhận|0|
|Tổng đầu ra theo số liệu ghi nhận|42.448.764|
|Chênh lệch|0|

Phép tính:

$$ 42.448.764 - (42.448.764 + 0 + 0) = 0 $$

**Kết quả số học:** Chênh lệch bằng 0 theo các số liệu hiện có.

**Giới hạn cần xác nhận:** Tài liệu A304 nêu rõ CSV corrupt phải được kiểm tra riêng và không được mặc định rằng `invalid\\\_events` đã bao gồm các dòng corrupt. ETL code được cung cấp đang đọc CSV ở chế độ `PERMISSIVE`, nhưng schema đọc CSV chưa khai báo trường `\\\_corrupt\\\_record` và chưa thể hiện bước lưu/đếm riêng các dòng corrupt. Vì vậy, con số `CSV corrupt = 0` hiện chỉ là số liệu được ghi trong bảng audit, chưa đủ bằng chứng kỹ thuật để khẳng định không có dòng CSV lỗi định dạng. Cần kiểm tra bổ sung trước khi đánh dấu audit hoàn tất tuyệt đối.

## 1.2. Kiểm tra cấu trúc và chất lượng Curated

Theo tài liệu kiểm tra DataFrame:

* Đã đọc được Curated Parquet từ HDFS.
* Có đủ 9 trường dữ liệu gốc.
* Có các trường ETL cần thiết: `event\\\_date`, `event\\\_type\\\_rank`, `event\\\_sort\\\_key`.
* Số bản ghi Curated được ghi nhận: 42.448.764.
* Quy tắc rank được thiết lập cho cả 4 event type:

  * `view` → 1
  * `cart` → 2
  * `remove\\\_from\\\_cart` → 3
  * `purchase` → 4
* `remove\\\_from\\\_cart` không xuất hiện trong thống kê dữ liệu tháng 10; điều này không làm mất quy tắc rank 3.
* Tài liệu kiểm tra ghi nhận `event\\\_sort\\\_key` có độ dài 64 ký tự, phù hợp biểu diễn SHA-256.
* `event\\\_date` được sử dụng làm trường phân vùng theo ngày UTC.

## 1.3. Kiểm tra Parquet và partition

Phạm vi kiểm tra: `/data/curated/events/`

|Nội dung|Kết quả|
|-|-|
|Thư mục partition|Có|
|Số partition theo ngày|31|
|Phạm vi partition|2019-10-01 đến 2019-10-31|
|File Parquet|Có|
|Tổng số file Parquet|744|
|File mỗi partition|24|
|Partition rỗng/bất thường|Không ghi nhận trong kết quả kiểm tra|
|Kích thước file|Phần lớn nhỏ, trung bình khoảng 6,9 MB|
|Định dạng nén|Snappy theo cấu hình A303|

Nhận xét: Cấu trúc partition theo `event\\\_date` đã được tạo. Tuy nhiên, số lượng file nhỏ tương đối nhiều và có thể làm tăng chi phí quản lý metadata/đọc dữ liệu. Đây là đề xuất tối ưu hiệu năng, không phải bằng chứng dữ liệu sai hoặc bị mất.

## 1.4. Đánh giá audit

|Tiêu chí|Đánh giá|Ghi chú|
|-|-|-|
|Raw count|Đạt theo số liệu đã ghi nhận|42.448.764 dòng, không tính header|
|Curated count|Đạt theo số liệu đã ghi nhận|42.448.764 dòng|
|Rejected count|Đạt theo số liệu đã ghi nhận|0|
|Đối chiếu số học|Đạt|Chênh lệch = 0|
|Schema và trường ETL|Đạt theo tài liệu kiểm tra|Có các trường bổ sung cần thiết|
|Partition theo ngày|Đạt theo kết quả HDFS listing|31 partition|
|Parquet files|Đạt theo kết quả HDFS listing|744 file|
|CSV corrupt|Chưa bổ sung|ETL hiện chưa thể hiện rõ cơ chế đếm và lưu riêng các bản ghi CSV lỗi (corrupt records). Tuy nhiên, theo kết quả tính toán số lượng bản ghi hiện có, số bản ghi CSV corrupt được ghi nhận là 0. Giá trị này chưa được xác minh bằng cơ chế kiểm tra độc lập trong ETL.|
|Checksum SHA-256|Chưa hoàn tất trong tài liệu|Cần tạo `SHA256\\\_OCT.txt`|
|Reviewer approval|Chưa xác nhận|Người B kiểm tra trước khi gắn tag|

**Kết luận:** Các số liệu hiện có cho thấy Raw, Curated và Rejected cân bằng về mặt số học, với chênh lệch bằng 0. Cấu trúc Parquet partitioned theo ngày cũng đã được ghi nhận đầy đủ 31 partition và 744 file. Tuy nhiên, chưa nên tuyên bố audit hoàn tất tuyệt đối cho đến khi xác minh CSV corrupt, xác nhận số dòng trực tiếp trên đúng thư mục bàn giao `/data/curated/events/`, tạo checksum SHA-256 và được Người B review.


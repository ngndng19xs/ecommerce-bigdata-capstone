# Báo cáo nghiệm thu – P3-A-A5-01: Batch Conversion Funnel

## 1. Thông tin công việc

- **Task:** P3-A-A5-01 – Batch Conversion Funnel
- **Người thực hiện:** Người A
- **Nguồn dữ liệu:** `/data/curated/events`
- **Đầu ra:** `/data/analytics/conversion_funnel`
- **Thời gian dữ liệu:** 01/10/2019 – 31/10/2019
- **Múi giờ:** UTC
- **Công cụ:** PySpark, Spark SQL, HDFS
- **Định dạng đầu ra:** Parquet, phân vùng theo `funnel_date`

## 2. Nội dung thực hiện

Xây dựng chương trình Batch Conversion Funnel để phân tích hành trình chuyển đổi của khách hàng theo từng phiên `user_session`.

Ba giai đoạn chuyển đổi:

1. View – xem sản phẩm.
2. Cart – thêm sản phẩm vào giỏ sau View.
3. Purchase – mua hàng sau Cart.

Các sự kiện được sắp xếp theo thứ tự:

`event_time → event_type_rank → event_sort_key`

Mỗi phiên chỉ được tính một lần ở mỗi giai đoạn. Ngày Funnel được xác định theo ngày UTC của sự kiện View đầu tiên trong phiên.

## 3. Chỉ số đầu ra

Kết quả được tổng hợp theo ngày, gồm:

- `view_sessions`: số phiên có View.
- `cart_sessions`: số phiên có Cart sau View.
- `purchase_sessions`: số phiên có Purchase sau Cart.
- `view_to_cart_rate`: tỷ lệ Cart sessions / View sessions.
- `cart_to_purchase_rate`: tỷ lệ Purchase sessions / Cart sessions.
- `view_to_purchase_rate`: tỷ lệ Purchase sessions / View sessions.

Đầu ra có 31 bản ghi ngày, từ 01/10/2019 đến 31/10/2019.

## 4. Kiểm tra và xác minh

| Hạng mục | Kết quả |
|---|---|
| Đọc dữ liệu Curated | Thành công |
| Tính Funnel theo ngày | Thành công |
| Ghi Parquet lên HDFS | Thành công |
| File `_SUCCESS` | Có |
| Đọc lại Parquet | Thành công |
| Số bản ghi đầu ra | 31 |
| Ngày bị trùng | 0 |
| Bản ghi có chỉ số không hợp lệ | 0 |
| Xác minh độc lập View → Cart ngày 01/10 | Khớp |

Kết quả xác minh độc lập ngày 01/10/2019:

| Chỉ số | Kết quả |
|---|---:|
| View sessions | 268.628 |
| Cart sessions | 10.130 |
| View → Cart | 3,77% |

Kết quả SQL độc lập khớp với kết quả trong bảng Batch Funnel.

## 5. Lưu ý về dữ liệu

Trong bước khảo sát dữ liệu Curated, phát hiện 2 bản ghi `cart` thiếu `user_session`. Các bản ghi này không thể gắn với một phiên khách hàng cụ thể nên bị loại khỏi phép tính Funnel.

Dữ liệu đầu vào có các loại sự kiện `view`, `cart`, `purchase`. Không có sự kiện `remove_from_cart` trong kết quả khảo sát.

## 6. Danh sách tệp bàn giao

- `batch_conversion_funnel.py`: chương trình tính Funnel.
- `verify_funnel_independent.py`: chương trình xác minh độc lập.
- `validate_funnel_metrics.py`: chương trình kiểm tra chất lượng kết quả.
- `sql/verify_funnel_independent.sql`: truy vấn SQL xác minh.
- `logs/funnel_run.log`: log chạy chương trình chính.
- `logs/check_funnel_output.log`: log kiểm tra đầu ra.
- `logs/verify_funnel.log`: log xác minh độc lập.
- `logs/validate_funnel_metrics.log`: log kiểm tra chất lượng.

## 7. Kết luận

Chương trình Batch Conversion Funnel đã hoàn thành việc tính toán và lưu kết quả theo ngày cho toàn bộ tháng 10/2019.

Các bước đọc dữ liệu, tính toán, ghi Parquet, đọc lại, kiểm tra chất lượng và xác minh độc lập đều thực hiện thành công.

**Trạng thái đề xuất: Đạt các tiêu chí kỹ thuật đã kiểm tra của Task P3-A-A5-01.**


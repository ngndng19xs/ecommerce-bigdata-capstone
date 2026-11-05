## 1. Mục tiêu

**Task:** P3-B-B6-02 — Tạo nhãn mua hàng sau N và các đặc trưng chỉ từ prefix N.

**Người thực hiện:** Thành viên B
**Người nghiệm thu:** Thành viên A
**Giai đoạn:** P3 — ETL Oct + thử Funnel/ML trên mẫu.

Xây dựng dữ liệu Machine Learning cấp phiên (`session_features`) để dự đoán khả năng mua hàng sau N sự kiện đầu tiên.

Yêu cầu:
- Nhãn phản ánh hành vi mua hàng sau prediction point.
- Đặc trưng chỉ sử dụng thông tin đã quan sát đến sự kiện thứ N.
- Loại các phiên không đủ điều kiện và thống kê lý do loại không chồng nhau.
- Kiểm soát data leakage giữa dữ liệu đặc trưng và nhãn.
- Kiểm thử trên DataFrame nhỏ và mẫu REES46 thực tế.

## 2. Logic xử lý

### 2.1. Xác định prediction point

Kế thừa kết quả ranking B6-01, sự kiện trong mỗi `user_session` được sắp xếp theo chuẩn S2:

`event_time UTC ASC → event_type_rank ASC → event_sort_key ASC`

Prediction point nằm ngay sau sự kiện thứ N, bao gồm sự kiện thứ N trong dữ liệu quan sát.

N=2 được sử dụng để thử nghiệm trên mẫu. Giá trị N chính thức chỉ được chốt sau khi nhận phân bố độ dài phiên từ A6, bảo đảm còn đủ phiên và đủ nhãn dương.

### 2.2. Tạo nhãn mua hàng

- `label = 1`: Phiên có ít nhất một sự kiện `purchase` với `event_idx > N`.
- `label = 0`: Không có sự kiện `purchase` sau N trong phần phiên được quan sát.
- Các sự kiện trước hoặc tại N không được sử dụng làm nhãn tương lai.

### 2.3. Loại phiên không hợp lệ

Các trường hợp cần loại:

1. Bản ghi có `user_session` null hoặc rỗng.
2. Phiên có ít hơn N sự kiện.
3. Phiên đã phát sinh `purchase` trước hoặc tại sự kiện thứ N.

Mỗi phiên có định danh hợp lệ chỉ thuộc một nhóm trạng thái loại hoặc hợp lệ, bảo đảm số lượng không bị đếm chồng. Bản ghi thiếu định danh phiên được thống kê riêng.

### 2.4. Xây dựng đặc trưng

Chỉ tổng hợp từ N sự kiện đầu tiên của phiên:

- Số lượt `view`, `cart`, `remove_from_cart`.
- Số sản phẩm và danh mục khác nhau.
- Thời lượng từ sự kiện đầu tiên đến prediction point.
- Giá trung bình và giá lớn nhất.
- Thương hiệu đã quan sát.
- Loại sự kiện thứ N.

Mỗi phiên hợp lệ tạo đúng một bản ghi đặc trưng kèm nhãn.

## 3. Kiểm soát Data Leakage

Nguyên tắc bắt buộc:

- Đặc trưng chỉ được tính từ các sự kiện có `event_idx <= N`.
- Dữ liệu sau N chỉ phục vụ xác định nhãn.
- Không sử dụng tổng số sự kiện hoặc thời lượng toàn phiên làm đặc trưng.
- Không sử dụng hành vi Purchase tương lai hoặc các thống kê chỉ biết sau khi phiên kết thúc.
- Thay đổi sự kiện sau N không được làm thay đổi đặc trưng đã tính từ prefix N.
- Khi các bản ghi hoàn toàn giống nhau có cùng khóa sắp xếp, việc hoán đổi thứ tự không được làm thay đổi nội dung đặc trưng hoặc nhãn.

Việc loại phiên và tạo nhãn có thể xét toàn bộ phiên; tuy nhiên, dữ liệu dùng làm đặc trưng dự đoán phải được giới hạn nghiêm ngặt tại prediction point.

## 4. Tiêu chí nghiệm thu

| Tiêu chí | Yêu cầu |
|---|---|
| Thứ tự sự kiện | Tuân thủ chuẩn S2 |
| Prediction point | Ngay sau sự kiện thứ N |
| Tạo nhãn | Chỉ xét Purchase sau N |
| Loại phiên | Đúng điều kiện, không đếm chồng |
| Tạo đặc trưng | Chỉ từ prefix N, bao gồm sự kiện N |
| Data leakage | Không sử dụng thông tin tương lai |
| Tính ổn định | Feature không đổi khi suffix thay đổi |
| Dữ liệu đầu ra | Một dòng cho mỗi phiên hợp lệ |
| Kiểm thử | DataFrame nhỏ và mẫu REES46 thực tế |

## 5. Kết quả kiểm chứng

### 5.1. Kết quả nền tảng B6-01

B6-01 đã xác nhận:

- 6/6 unit tests cơ bản PASS.
- 3/3 edge-case tests PASS.
- Integration REES46 PASS.
- Thứ tự nghiệp vụ: View → Cart → Purchase.
- Prediction N=2: Prefix gồm View và Cart.

### 5.2. Kịch bản kiểm chứng B6-02

Với phiên REES46 đã xác minh ở B6-01:

`View → Cart → Purchase`

Tại N=2, kết quả B6-02 cần đạt:

- Prefix: View, Cart.
- `n_view = 1`, `n_cart = 1`.
- Thời lượng prefix: 14 giây.
- Sự kiện thứ N: Cart.
- Nhãn kỳ vọng: `label = 1`, vì Purchase xảy ra sau N.

Đồng thời cần kiểm chứng các phiên không mua, phiên ngắn hơn N, phiên mua trước/tại N và tình huống thay đổi dữ liệu tương lai.

**Trạng thái kiểm thử B6-02:** Chưa có log xác nhận unit tests và integration PASS. Các giá trị trên là kết quả kỳ vọng cần đối chiếu với thực nghiệm.

## 6. Kết luận

Logic P3-B-B6-02 được thiết kế theo đúng yêu cầu của phân hệ B6:

- Nhãn mua hàng chỉ xét các sự kiện sau prediction point.
- Đặc trưng chỉ sử dụng N sự kiện đầu tiên.
- Các phiên không đủ điều kiện được loại và thống kê rõ ràng.
- Có nguyên tắc kiểm chứng tính bất biến của đặc trưng và phòng tránh data leakage.

**Trạng thái:** Đã xác định logic và tiêu chí nghiệm thu; chờ kết quả kiểm thử thực tế B6-02 để kết luận PASS.

Trong P3, đầu ra yêu cầu là mẫu `session_features`. Việc chốt N và chạy dữ liệu đầy đủ tại `/data/ml/session_features` được thực hiện ở P5 sau khi nhận dữ liệu và thống kê từ thành viên A.

Ranh giới train/validation/test sẽ được chốt theo thời gian bắt đầu phiên và ghi nhận trong hợp đồng dữ liệu trước khi huấn luyện, không sử dụng tập test để lựa chọn N.

**Kết luận nghiệm thu chính thức:** Chờ log kiểm thử và xác nhận của reviewer A.

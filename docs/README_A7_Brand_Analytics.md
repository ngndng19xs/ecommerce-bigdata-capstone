# P6-A-A7 – Brand Analytics

## 1. Mục tiêu
Thực hiện phân tích dữ liệu theo thương hiệu trên dữ liệu eCommerce đã làm sạch:
- **A7-01:** tổng hợp số lượng sự kiện và session theo brand.
- **A7-02:** lọc và xếp hạng brand theo `view_sessions` với ngưỡng tối thiểu.

Công cụ: **PySpark**. Định dạng đầu ra: **Parquet trên HDFS**.

## 2. Input và output

| Nội dung | HDFS path |
|---|---|
| Dữ liệu đầu vào | `/data/curated/events` |
| Bảng tổng hợp A7-01 | `/data/analytics/brand_analysis/aggregate` |
| Bảng xếp hạng A7-02 | `/data/analytics/brand_analysis/top_brands_min_view_10000` |

Dữ liệu đầu vào là dữ liệu curated được phân vùng theo `event_date`.

## 3. Các chỉ số A7-01

`brand_analysis.py` tổng hợp theo `brand`:
- `total_events`: tổng số sự kiện.
- `view_events`, `cart_events`, `purchase_events`: số sự kiện tương ứng.
- `remove_from_cart_events`: số sự kiện remove from cart.
- `view_sessions`: số `user_session` duy nhất có sự kiện view.
- `purchase_sessions`: số `user_session` duy nhất có sự kiện purchase.
- `purchase_event_value`: tổng trường `price` của các sự kiện purchase.
- `session_conversion_rate`: `purchase_sessions / view_sessions * 100`.

**Quy ước:**
- Brand null hoặc chuỗi rỗng được chuẩn hóa thành `Unknown`; không loại bỏ nhóm này.
- `purchase_event_value` là tổng trường `price` của các sự kiện purchase, không mặc định đồng nghĩa với doanh thu.
- `purchase_events` là số sự kiện purchase, không phải số đơn hàng.
- `session_conversion_rate` là tỷ lệ giữa hai chỉ số session được tổng hợp theo brand; cần diễn giải đúng tử số và mẫu số, không mặc định là conversion của cùng một cohort.

## 4. Kết quả A7-01

- Tổng số nhóm brand: **4.304**, bao gồm `Unknown`.
- `Unknown`: **15.331.243 events**, **4.769.654 view_sessions**.
- Một số brand có `view_sessions` cao:
  - Samsung: 4.273.844
  - Apple: 3.612.630
  - Xiaomi: 2.386.962

## 5. A7-02 – Ngưỡng và xếp hạng

Ngưỡng được chọn: **`MIN_VIEW_SESSIONS = 10000`**.

Cơ sở lựa chọn:
- Phân vị 25%: 67 view sessions.
- Trung vị: 374 view sessions.
- Phân vị 75%: 2.008 view sessions.
- Phân vị 90%: 8.710 view sessions.
- Ngưỡng 10.000 cao hơn phân vị 90%, giúp hạn chế brand có lượng quan sát thấp.
- Ngưỡng 100.000 chỉ giữ lại 50/4.303 brand (1,16%), quá khắt khe cho mục tiêu xếp hạng rộng hơn.

Quy tắc:
- Không đưa `Unknown` vào bảng xếp hạng thương hiệu.
- Chỉ giữ brand có `view_sessions >= 10000`.
- Sắp xếp `view_sessions` giảm dần; nếu bằng nhau, sắp xếp theo tên brand.
- Vẫn giữ `Unknown` trong bảng tổng hợp A7-01.

Kết quả lần chạy:

| Chỉ tiêu | Giá trị |
|---|---:|
| Brand xác định được trước lọc | 4.303 |
| Ngưỡng | 10.000 view sessions |
| Brand được giữ lại | 427 |
| Brand bị loại | 3.876 |
| Tỷ lệ được giữ lại | 9,92% |

## 6. Cấu trúc mã nguồn

| File | Chức năng |
|---|---|
| `brand_analysis.py` | Tổng hợp chỉ số theo brand – A7-01 |
| `rank_top_brands.py` | Lọc và xếp hạng brand – A7-02 |
| `check_brand.py` | Kiểm tra Top 20, `Unknown` và tổng số brand |
| `profile_brand_threshold.py` | Khảo sát phân phối `view_sessions` và so sánh ngưỡng |

Hai file đầu là mã nguồn chính; hai file sau là script hỗ trợ kiểm tra và khảo sát.

## 7. Cách chạy trên VM

Yêu cầu: Hadoop/HDFS và PySpark đã được cấu hình; dữ liệu curated có tại `/data/curated/events`.

```bash
cd ~/capstone/analytics

# Kiểm tra cú pháp
python3 -m py_compile brand_analysis.py
python3 -m py_compile rank_top_brands.py

# Chạy A7-01
env -u PYSPARK_DRIVER_PYTHON PYSPARK_PYTHON=python3 spark-submit brand_analysis.py

# Chạy A7-02 sau khi A7-01 hoàn tất
env -u PYSPARK_DRIVER_PYTHON PYSPARK_PYTHON=python3 spark-submit rank_top_brands.py
```

Kiểm tra output:

```bash
hdfs dfs -ls /data/analytics/brand_analysis/aggregate
hdfs dfs -ls /data/analytics/brand_analysis/top_brands_min_view_10000
```

Output Parquet thành công có `_SUCCESS` và các file `part-*.parquet`.

## 8. Lấy mã nguồn từ VM qua VirtualBox Shared Folder

Shared Folder đang sử dụng trong VM:

`/media/sf_CapstoneData`

Trên VM CentOS, chạy:

```bash
mkdir -p /media/sf_CapstoneData/A7_brand_analytics

cp ~/capstone/analytics/brand_analysis.py /media/sf_CapstoneData/A7_brand_analytics/
cp ~/capstone/analytics/rank_top_brands.py /media/sf_CapstoneData/A7_brand_analytics/
cp ~/capstone/analytics/check_brand.py /media/sf_CapstoneData/A7_brand_analytics/
cp ~/capstone/analytics/profile_brand_threshold.py /media/sf_CapstoneData/A7_brand_analytics/
```

Sau đó trên Windows:
1. Mở thư mục Shared Folder `CapstoneData`, vào `A7_brand_analytics`.
2. Sao chép các file `.py` vào repository, ví dụ `ecommerce-bigdata-capstone/analytics/`.
3. Đặt README này trong thư mục `analytics/` và đổi tên thành `README.md` nếu cần.
4. Kiểm tra `git status`, sau đó commit lên nhánh `feature/A7-brand`.

Nếu `cp` báo `Permission denied`, kiểm tra quyền truy cập Shared Folder và nhóm `vboxsf` của tài khoản VM; không cần thay đổi quyền toàn hệ thống.

## 9. Lưu ý nghiệm thu

- A7-01 và A7-02 đọc dữ liệu curated trên HDFS, không đọc trực tiếp CSV raw.
- A7-02 sử dụng output aggregate của A7-01.
- `Unknown` được giữ trong aggregate nhưng loại khỏi ranking.
- Ranking phản ánh mức độ quan sát theo `view_sessions`, không phải đánh giá chất lượng hay doanh số thương hiệu.
- Khi bàn giao, cung cấp mã nguồn, README, đường dẫn HDFS output và ảnh chụp kết quả chạy/kiểm tra nếu nhóm yêu cầu.

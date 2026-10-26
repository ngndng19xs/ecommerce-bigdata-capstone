# Bộ truy vấn bản đầu và kịch bản benchmark (TV2)

Công việc: nối truy vấn với schema bảng, kiểm thử trên Hive, chuẩn bị benchmark ban đầu và các kỹ thuật tối ưu dự kiến.

**Trạng thái:** bộ này CHƯA được chạy trên Spark/Hive thật. Logic funnel đã được kiểm tra bằng SQLite trên `sample_20k.csv`, và cách dựng bảng bằng "1 phép gộp" cho kết quả trùng cách dùng 3 window trên 5.231 phiên. Script đã được thử luồng chạy bằng lệnh giả lập. Cú pháp Spark/Hive và mọi con số thời gian cần được đo trên máy ảo.

## 1. Gồm những gì

| File | Vai trò |
|---|---|
| `setup.sql` | Tạo database, bảng CSV, Parquet, Parquet phân vùng theo `event_date` đúng schema đặc tả 11.1 |
| `queries/q1..q6*.sql` | Bộ truy vấn bản đầu: funnel tổng thể, funnel theo ngày, số sự kiện mỗi phiên, thời lượng phiên, brand (Phương pháp A), quét theo brand |
| `queries/*_opt.sql` | Bản sau tối ưu của q1 đến q5 |
| `queries/q0a`, `q0b`, `q0c` | Dựng bảng `session_funnel` bằng 3 window và bằng 1 phép gộp, rồi đối chiếu hai bản |
| `queries/hive_check.sql` | Kiểm tra chéo Hive và Spark |
| `queries/adapter_real_schema.sql` | Mẫu nối với bảng thật của TV1 |
| `bench.sh` | Chạy toàn bộ: `setup`, `build`, `run`, `summary`, `sizes`, `hive` |

## 2. Chạy nhanh trên máy ảo

```bash
tar xzf ecom_bench.tar.gz && cd ecom_bench
export CSV_SRC=$HOME/sample_20k.csv
bash bench.sh setup      # tạo bảng
bash bench.sh build      # dựng session_funnel, kết quả cuối phải mismatched=0, missing_in_agg=0
bash bench.sh run        # đo, tự in bảng tổng hợp và lưu out/summary.md
bash bench.sh sizes      # dung lượng các bảng
bash bench.sh hive       # so Hive với Spark
```

- Lần đầu để mặc định (`REPLICAS=1`, 20.000 dòng) chỉ để kiểm tra mọi thứ chạy được. Thời gian ở quy mô này không có ý nghĩa.
- Để có số đo đáng tin: `export REPLICAS=50` (khoảng 1 triệu dòng, 50 ngày) rồi chạy lại `setup`, `build`, `run`. Dữ liệu nhân bản là dữ liệu giả (đổi `user_session`, lùi sang ngày sau), chỉ dùng để so sánh các phương án, không dùng để rút ra kết luận nghiệp vụ.
- Chỉ chạy một lệnh `spark-sql` mỗi lúc và luôn chạy `bench.sh` (script tự vào cùng một thư mục làm việc), vì Spark có thể đang dùng metastore Derby cục bộ.
- Có thể chỉnh `DRIVER_MEM`, `CORES`, `RUNS`, `D1`, `D2`. Máy ảo lúc đo chỉ còn khoảng 2 GB RAM trống nên đừng tăng `DRIVER_MEM` quá mức.

## 3. Kịch bản benchmark

| Kịch bản | Bảng đọc | Cấu hình Spark | Truy vấn |
|---|---|---|---|
| A: CSV | `events_csv` (view trên bảng văn bản) | mặc định (shuffle 200, AQE tắt) | q1 đến q6 bản đầu |
| B: Parquet | `events_pq` | mặc định | như A |
| C: Parquet + phân vùng | `events_pq_part` | mặc định | như A |
| D: sau tối ưu | `events_pq_part` và `session_funnel_pq` | shuffle 8, AQE bật | q1 đến q5 bản `_opt`, q6 như cũ |

Quy tắc đo:
- Mọi kịch bản dùng cùng khoảng ngày `D1` đến `D2`, cùng máy, không chạy việc khác.
- Mỗi ô chạy `RUNS=3` lần, lấy trung vị, trừ thời gian khởi động `spark-sql` (đo riêng bằng `SELECT 1`). Lần đầu có thể chậm hơn vì cache lạnh.
- Thời gian dựng `session_funnel` (một lần) ghi riêng ở `out/results_build.csv`, không tính vào q1 đến q4 của kịch bản D. Khi báo cáo, nêu cả hai.
- Kiểm tra kết quả: cột "Ket qua giong nhau" so mã băm đầu ra giữa các kịch bản. Kịch bản D lọc theo `session_date` còn A đến C lọc theo `event_date`, nên với dữ liệu thật có thể lệch nhẹ ở các phiên nằm sát biên khoảng ngày. Với dữ liệu nhân bản thì phải giống nhau.
- Số byte đọc: xem `Input Size` trong Spark UI (cổng 4040 khi truy vấn đang chạy), và dung lượng bảng bằng `bench.sh sizes`.
- Ghi vào báo cáo: số CPU, RAM, phiên bản Spark, `local[N]`, `DRIVER_MEM`, số dòng, số ngày.

## 4. Các kỹ thuật tối ưu dự kiến

| # | Kỹ thuật | Nơi áp dụng | Cách kiểm chứng |
|---|---|---|---|
| 1 | CSV sang Parquet (lưu theo cột, nén) | B so với A | thời gian và `sizes` |
| 2 | Phân vùng theo `event_date`, loại bỏ phân vùng không cần | C so với B | `EXPLAIN FORMATTED <truy vấn>` phải có `PartitionFilters`; thời gian |
| 3 | Dựng sẵn `session_funnel` thành bảng Parquet | q1 đến q4 kịch bản D | thời gian so với tính lại từ bảng sự kiện |
| 4 | Thay 3 lớp window bằng 1 phép gộp (`collect_list` và `filter`) | q0a so với q0b, q5 so với q5_opt | `q0c` phải cho `mismatched=0`; thời gian |
| 5 | Bật AQE, giảm `shuffle.partitions` từ 200 xuống 8 | toàn bộ kịch bản D | so cùng truy vấn |
| 6 | Chỉ đọc các cột cần thiết | mọi truy vấn | `EXPLAIN` thấy `ReadSchema` ngắn |

Dự kiến thử thêm nếu còn thời gian: `ANALYZE TABLE ... COMPUTE STATISTICS`, `CACHE TABLE` cho truy vấn lặp, chọn kích thước file Parquet và codec nén, bucketing theo `user_session`.

## 5. Bảng kết quả chạy thử (điền từ `out/summary.md`)

| Query | A: CSV (s) | B: Parquet (s) | C: Parquet + partition (s) | D: sau tối ưu (s) | Tăng tốc D/A | Kết quả giống nhau |
|---|---|---|---|---|---|---|
| q1 funnel tổng thể | chưa đo | chưa đo | chưa đo | chưa đo | | |
| q2 funnel theo ngày | chưa đo | chưa đo | chưa đo | chưa đo | | |
| q3 số sự kiện mỗi phiên | chưa đo | chưa đo | chưa đo | chưa đo | | |
| q4 thời lượng phiên | chưa đo | chưa đo | chưa đo | chưa đo | | |
| q5 brand (Phương pháp A) | chưa đo | chưa đo | chưa đo | chưa đo | | |
| q6 quét theo brand | chưa đo | chưa đo | chưa đo | chưa đo | | |

## 6. Nối với bảng thật của TV1

Khi TV1 giao bảng sự kiện đã làm sạch, hãy kiểm tra tên cột có đúng đặc tả 11.1 không: `event_time` (TIMESTAMP UTC), `event_date`, `user_session`, `user_id`, `event_type`, `product_id`, `category`, `brand`, `price`. Nếu khác tên, sửa `queries/adapter_real_schema.sql` để tạo view `events_real`, rồi chạy:

```bash
T_CSV=events_real T_PQ=events_real T_PQP=events_real D1=2019-10-01 D2=2019-10-07 bash bench.sh build
T_CSV=events_real T_PQ=events_real T_PQP=events_real D1=2019-10-01 D2=2019-10-07 bash bench.sh run
```

Với bảng thật, kịch bản A đến C trỏ cùng một bảng nên chỉ còn ý nghĩa so cấu hình. Muốn so định dạng thì đặt `T_CSV`, `T_PQ`, `T_PQP` về từng bảng tương ứng mà TV1 có.

## 7. Kiểm thử Hive

`bash bench.sh hive` chạy `queries/hive_check.sql` và q1 trên cả Hive lẫn Spark rồi so đầu ra.
- Nếu báo Hive không thấy database: Spark đang dùng metastore riêng. Cần TV1 hoặc TV5 chỉ cách trỏ Spark vào metastore của Hive. Đừng tự đổi cấu hình dùng chung.
- Nếu kết quả khác ở cột thời gian (lệch nhiều giờ): nghi do múi giờ. Máy ảo đang đặt múi giờ KST (UTC+9) trong khi quy ước nhóm là UTC. Ghi lại và báo TV1.

## 8. Rủi ro cần biết

- Bảng phân vùng tạo bằng `INSERT ... PARTITION` có thể sinh nhiều file nhỏ trên dữ liệu lớn.
- Truy vấn dùng `collect_list` cần bộ nhớ theo kích thước phiên. Phiên rất dài (hàng nghìn sự kiện) có thể chậm.
- Hàm cao cấp `filter` và `array_min` yêu cầu Spark 2.4 trở lên (máy ảo có Spark 3.1.2).

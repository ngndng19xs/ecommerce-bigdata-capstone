# ML Data Contract (Bản Đề Xuất DRAFT-S2-02)

> **Task:** P2-B-S2-02 – Prediction Point N, Label & Temporal Split<br>
> **Người đề xuất:** Người B (Streaming & Machine Learning)<br>
> **Người đánh giá (Reviewer):** Người A (Data Engineering & Analytics)<br>
> **Trạng thái:** 🟡 **BẢN ĐỀ XUẤT (DRAFT) – CHỜ NGƯỜI A PHÊ DUYỆT (H04)**<br>
> **Tài liệu tham chiếu:** [data_contract.md](./data_contract.md) | [kafka_contract_DRAFT.md](./kafka_contract_DRAFT.md)

---

## 1. Mục Tiêu

Tài liệu này xác lập quy chuẩn dữ liệu (Data Contract) cho bài toán Machine Learning thuộc dự án Capstone eCommerce Clickstream REES46:
- **Bài toán mô hình hóa:** Dự đoán khả năng khách hàng thực hiện hành vi mua hàng (**Purchase**) trong phần còn lại của phiên truy cập sau khi đã quan sát đúng $N$ sự kiện đầu tiên (*Session-level Purchase Prediction at Prefix $N$*).
- **Mục đích của hợp đồng:** Thiết lập cầu nối kỹ thuật thống nhất giữa tầng **Batch ETL / Analytics (Người A)** và tầng **Feature Engineering / ML (Người B)**, đảm bảo:
  1. Thống nhất thứ tự sự kiện chuẩn để đánh số thứ tự ($rn$).
  2. Định nghĩa nhãn mục tiêu nhị phân (`label`) rõ ràng, loại bỏ nhãn nhiễu.
  3. Áp dụng các quy tắc loại trừ phiên (Exclusion Rules) và cắt biên dữ liệu (Right-Censoring) hợp lý.
  4. Đề xuất và chuẩn hóa danh mục 15 đặc trưng tiền tố hiện có trong codebase, chờ A/B review trước khi chốt chính thức.
  5. Quy định cơ chế phân chia tập dữ liệu huấn luyện / kiểm định / đánh giá theo dòng thời gian (Temporal Split).

> ⚠️ **Lưu ý quan trọng:** Đây là bản dự thảo kỹ thuật do Người B đề xuất cho Task P2-B-S2-02 dựa trên kết quả rà soát codebase thực tế. Tài liệu này **KHÔNG thay thế** [data_contract.md](./data_contract.md) và sẽ được hoàn thiện để hợp nhất chính thức vào **Mục 9** của Data Contract sau khi hai bên rà soát tại mốc H04.

---

## 2. Nguồn Dữ Liệu

Tài liệu phân biệt rõ hai nguồn dữ liệu trong quá trình phát triển và vận hành:

### 2.1. Dữ liệu Production chính thức
- Toàn bộ pipeline huấn luyện và đánh giá mô hình chính thức sẽ đọc trực tiếp từ thư mục **Curated Parquet** (`/data/curated/events_cleaned` hoặc đường dẫn theo cấu hình) do Người A bàn giao từ pipeline PySpark ETL.
- Dữ liệu này đã được làm sạch, xử lý `brand = "Unknown"`, loại bỏ `price < 0` / NULL, chuẩn hóa kiểu dữ liệu (`event_time` dạng `TimestampType` UTC, `price` dạng `DoubleType`), và sẵn sàng cho việc trích xuất đặc trưng.

### 2.2. Tập mẫu khảo sát H02 (Môi trường phát triển & kiểm thử sơ bộ)
- Gồm 2 file CSV thô với 1.244.245 sự kiện phát sinh trong ngày `2019-10-01 UTC` tại thư mục chia sẻ `/media/sf_CapstoneSample/` trên VM-B.
- Qua khảo sát thực tế, tập dữ liệu này chứa **268.737 phiên** (`user_session`) phân biệt.
- **Giới hạn kỹ thuật quan trọng:** Do tập H02 chỉ bao phủ một ngày, chưa đủ cơ sở xác nhận tất cả session đều hoàn chỉnh; có nguy cơ thiếu sự kiện ở ranh giới thời gian (đầu ngày 00:00:00 UTC và cuối ngày 23:59:59 UTC). Do đó, tập H02 chỉ được sử dụng để kiểm tra luồng code, benchmark hiệu năng xử lý Spark và kiểm thử cấu trúc dữ liệu, **không sử dụng tập H02 để tạo nhãn ML chính thức** hoặc làm thước đo đánh giá chất lượng hội tụ cuối cùng của mô hình.

---

## 3. Thứ Tự Sự Kiện (Event Ordering)

Thứ tự xuất hiện của các sự kiện trong một phiên quyết định trực tiếp đến việc sự kiện nào thuộc tiền tố ($rn \le N$) và sự kiện nào thuộc phần đuôi ($rn > N$).

### 3.1. Quy tắc bắt buộc theo Data Contract của Người A (Mục 5.3 & 5.7)
Thứ tự sắp xếp sự kiện trong cùng một session bắt buộc phải tuân thủ bộ ba:
```
(event_time UTC ASC, event_type_rank ASC, event_sort_key ASC)
```
- `event_time`: Thời điểm phát sinh sự kiện theo chuẩn UTC.
- `event_type_rank`: Thứ tự ưu tiên sự kiện khi trùng giây:
  $$\text{view (1)} < \text{cart (2)} < \text{remove\_from\_cart (3)} < \text{purchase (4)}$$
- `event_sort_key`: Tiêu chí tie-breaker cuối cùng, là chuỗi mã băm SHA-256 chữ thường 64 ký tự được sinh từ 9 trường dữ liệu nguồn đã chuẩn hóa, nối bằng ký tự `||` (NULL thay bằng `<NULL>`).
- *Lưu ý:* `event_sort_key` là một tie-breaker tất định dựa trên giá trị dữ liệu, **không bảo đảm tính duy nhất tuyệt đối** nếu có các bản ghi trùng lặp hoàn toàn 100% cả 9 trường (nó không phải UUID).

### 3.2. Hiện trạng Codebase và Điểm chưa tương thích
- Trong mã nguồn [`build_session_dataset.py`](../src/ml/dataset/build_session_dataset.py) (dòng 74-78 & 175-179), cửa sổ sắp xếp hiện tại đang được định nghĩa là:
  ```python
  order_win = Window.partitionBy("user_session").orderBy(
      F.col("event_time").asc(),
      type_rank_col.asc(),
      F.col("product_id").asc(),  # <-- CHƯA TƯƠNG THÍCH
  )
  ```
- **Đánh giá:** Code ML hiện tại đang dùng **`product_id`** làm tiêu chí phân định thứ ba thay vì **`event_sort_key`**. Đây là điểm **chưa tương thích**.
- **Hai vấn đề cần Người A xác nhận:**
  1. Người A có lưu sẵn cột `event_sort_key` trong Curated Parquet bàn giao hay không, hay tầng ML phải tự tính lại bằng PySpark?
  2. Quy tắc chuẩn hóa ép kiểu chuỗi (cho Timestamp, Double, NULL) trước khi hash SHA-256 đã được hai bên thống nhất hoàn toàn bằng Test Vector chưa?

---

## 4. Prediction Point $N$

1. **Vị trí Prediction Point:** Thời điểm đưa ra dự đoán được ấn định **ngay sau khi sự kiện thứ $N$ của phiên truy cập diễn ra**.
2. **Quy tắc phân định Head / Tail:**
   - **Tập tiền tố (Head):** Gồm tất cả các sự kiện có số thứ tự $rn \le N$. Toàn bộ đặc trưng (features) của phiên **chỉ được phép trích xuất từ tập này**.
   - **Tập phần đuôi (Tail):** Gồm tất cả các sự kiện có số thứ tự $rn > N$. Tập này **chỉ dùng duy nhất cho việc xác định nhãn mục tiêu (`label`)**, tuyệt đối không được tham gia vào tính toán đặc trưng.
   - Sự kiện thứ $N$ (`rn == N`) nằm hoàn toàn trong tập Head và được đưa vào tính toán đặc trưng (ví dụ: `last_event_type`, `hour_of_day`, `interaction_duration`).
3. **Trạng thái giá trị $N$:**
   - Giá trị $N$ chính thức hiện tại vẫn là **TBD (Chưa chốt)**.
   - Trong codebase hiện tại ([`build_session_dataset.py`](../src/ml/dataset/build_session_dataset.py)), tham số đang được đặt tên là `k` với giá trị mặc định là `k=5` (`def build_session_dataset(events_df: DataFrame, k: int = 5)`).
   - **Lưu ý:** Giá trị $k=5$ chưa được A/B phê duyệt, chỉ là tham số mặc định phục vụ thử nghiệm code.
4. **Đối chiếu cấu hình hiện tại:**
   - Trong `configs/config.example.yaml`:
     - Dòng 113: `prediction_point_n: null` (thể hiện giá trị Contract chưa chốt).
     - Dòng 124: `k_events: 5` (thể hiện giá trị tham số dùng cho thử nghiệm).
   - Đây là sự khác biệt có chủ ý giữa giá trị Contract chính thức đang chờ chốt và giá trị tham số thực thi ban đầu. Tham số Python sẽ được giữ nguyên trong bước này.

---

## 5. Nhãn Mục Tiêu (Label Contract)

Mô hình giải bài toán phân loại nhị phân (Binary Classification):

$$\text{label} \in \{0, 1\}$$

### 5.1. Quy tắc gán nhãn
1. **`label = 1` (Positive Class):**
   - Phiên có **ít nhất một sự kiện `purchase`** xảy ra ở phần đuôi ($rn > N$).
   - Nếu trong phần đuôi có nhiều hơn 1 sự kiện `purchase`, nhãn vẫn giữ nguyên là `1`.
2. **`label = 0` (Negative Class):**
   - Phiên **không có bất kỳ sự kiện `purchase` nào** ở phần đuôi ($rn > N$), đồng thời thỏa mãn đầy đủ các điều kiện phiên hoàn chỉnh (không bị cắt biên dữ liệu).
3. **Lưu ý quan trọng:** Không được mặc định một session không có Purchase trong tập H02 một ngày là `label=0` chính thức nếu chưa kiểm chứng khả năng phiên bị cắt biên ở ranh giới dữ liệu.

### 5.2. Quy tắc loại bỏ phiên có Purchase sớm (Early Purchase Exclusion)
- Nếu một phiên có bất kỳ sự kiện `purchase` nào xuất hiện tại hoặc trước Prediction Point ($rn \le N$), phiên đó **bắt buộc bị loại bỏ hoàn toàn khỏi bộ dữ liệu huấn luyện ML** (`has_purchase_head == 1` $\rightarrow$ Drop).
- *Cơ sở nghiệp vụ:* Mục tiêu cốt lõi của bài toán là dự đoán ý định mua sắm của khách hàng *chưa chuyển đổi*. Nếu khách hàng đã mua hàng ngay trong $N$ tương tác đầu tiên, việc dự đoán hành vi sau đó không còn mang ý nghĩa bài toán chuyển đổi ban đầu và sẽ làm méo mó phân phối đặc trưng.

---

## 6. Các Quy Tắc Loại Trừ Phiên (Exclusion Rules)

Để bảo đảm tính hợp lệ của dữ liệu huấn luyện và tránh nhiễu mô hình, một phiên sẽ bị đánh giá và loại bỏ theo các trường hợp sau:

### 6.1. Phân loại theo độ dài phiên
1. **Trường hợp 1: $n\_events < N$**
   - Không đủ số lượng sự kiện để hình thành tập tiền tố (Head), bắt buộc phải loại bỏ.
2. **Trường hợp 2: $n\_events = N$**
   - Đủ sự kiện để trích xuất Head nhưng không có phần đuôi (Tail) nào được quan sát.
   - Code hiện tại đang loại bỏ trường hợp này vì áp dụng điều kiện ngặt `n_events > N`.
   - *Điểm thảo luận:* Không tự ý kết luận các phiên này là `label = 0`. Chính sách xử lý trường hợp có đúng $N$ sự kiện cần được Người A và B thống nhất chính thức tại mốc H04.
3. **Trường hợp 3: $n\_events > N$**
   - Có phần đuôi quan sát được, nhưng vẫn cần đánh giá tính hoàn chỉnh của phiên (kiểm tra Purchase sớm và cắt biên dữ liệu) trước khi gán nhãn âm chính thức.

### 6.2. Các quy tắc loại trừ khác
- **`user_session` không hợp lệ:** Loại bỏ các bản ghi có `user_session` là NULL, chuỗi rỗng (`""`), hoặc chuỗi chỉ chứa khoảng trắng (`trim(user_session) == ""`).
- **Session có Purchase sớm ($rn \le N$):** Đã quy định tại Mục 5.2.
- **Session bị cắt biên dữ liệu:** Được quy định chi tiết tại Mục 7.

*Nguyên tắc thống kê:* Khi chạy thống kê dữ liệu (EDA), việc tính toán số lượng phiên bị loại phải được thực hiện theo cấu trúc phân cấp tuần tự (hierarchical filtering) để đảm bảo các nhóm bị loại là **rời rạc (disjoint), không bị đếm trùng lặp**.

---

## 7. Quy Tắc Cắt Biên Dữ Liệu (Right-Censoring)

### 7.1. Mô tả logic hiện có trong code
- Trong [`build_session_dataset.py`](../src/ml/dataset/build_session_dataset.py) (dòng 59-62, 100), code hiện tại đang tính:
  $$\text{max\_dataset\_date} = \max(\text{to\_date}(event\_time))$$
  và loại bỏ toàn bộ các phiên có `session_start_date == max_dataset_date`.
- **Đánh giá:** Đây là một biện pháp heuristic hiện có, không bảo đảm phát hiện đầy đủ mọi session bị cắt biên (ví dụ: một session bắt đầu lúc 23:55 ngày hôm trước kéo dài sang ngày cuối vẫn có nguy cơ bị cắt dở dang).

### 7.2. Giới hạn khi chạy trên tập mẫu H02
- Hai file CSV của tập H02 chỉ bao phủ duy nhất một ngày (`2019-10-01`).
- Nếu áp dụng logic heuristic trên, tất cả các phiên đều có `session_start_date` bằng `max_dataset_date`, dẫn đến việc **100% các phiên bị loại bỏ** (kết quả rỗng).
- Do đó, tập H02 không phù hợp để tạo tập dữ liệu huấn luyện chính thức theo logic hiện tại.

### 7.3. Chế độ kiểm thử phát triển
- Trong tương lai, có thể đề xuất bổ sung một chế độ kỹ thuật (ví dụ: tham số cấu hình) cho phép kiểm thử luồng pipeline với tập mẫu ngắn ngày như H02.
- **Quy định bắt buộc:** *Việc bỏ qua right-censoring trong chế độ kiểm thử chỉ được phép phục vụ kiểm tra luồng xử lý và cấu trúc dữ liệu. Các nhãn tạo ra trong chế độ này không được dùng để huấn luyện, đánh giá hoặc báo cáo kết quả mô hình ML chính thức.*
- *Lưu ý:* Cờ cấu hình này hiện **chưa tồn tại trong code**, đây là đề xuất kỹ thuật cần xem xét ở các bước tiếp theo.

### 7.4. Điều kiện nghiệm thu chính thức
- Quy tắc hoàn chỉnh phiên, cửa sổ thời gian quan sát nhãn (label observation window) và chính sách xử lý Left/Right-Censoring ở cả hai đầu thời gian phải được Người A và B chốt chính thức. Không tự coi việc loại bỏ ngày cuối cùng là quy tắc chính thức đầy đủ.

---

## 8. Danh Mục Đặc Trưng (Feature Contract)

Dưới đây là **15 đặc trưng tiền tố hiện có trong codebase**, đề xuất đưa vào ML Contract (chờ A/B review, chưa nghiệm thu chính thức). Toàn bộ được trích xuất từ [`build_session_dataset.py`](../src/ml/dataset/build_session_dataset.py) (dòng 106-128) và tính toán trên tập tiền tố $rn \in [1, N]$:

| # | Tên đặc trưng | Kiểu dữ liệu | Phạm vi tính toán | Diễn giải nghiệp vụ |
|---|---|---|---|---|
| 1 | `view_count` | `LongType` | $rn \le N$ | Tổng số lần xem sản phẩm trong $N$ sự kiện đầu. |
| 2 | `cart_count` | `LongType` | $rn \le N$ | Tổng số lần thêm vào giỏ hàng trong $N$ sự kiện đầu. |
| 3 | `remove_count` | `LongType` | $rn \le N$ | Tổng số lần xóa khỏi giỏ hàng trong $N$ sự kiện đầu. |
| 4 | `has_cart` | `IntegerType` | $rn \le N$ | Cờ nhị phân: `1` nếu `cart_count > 0`, ngược lại `0`. |
| 5 | `unique_product_count` | `IntegerType` | $rn \le N$ | Số lượng `product_id` phân biệt tương tác trong $N$ sự kiện đầu. |
| 6 | `unique_category_count`| `IntegerType` | $rn \le N$ | Số lượng `category_code` phân biệt tương tác trong $N$ sự kiện đầu. |
| 7 | `unique_brand_count` | `IntegerType` | $rn \le N$ | Số lượng `brand` phân biệt tương tác trong $N$ sự kiện đầu. |
| 8 | `avg_price` | `DoubleType` | $rn \le N$ | Giá trung bình của các sản phẩm tương tác trong $N$ sự kiện đầu. |
| 9 | `max_price` | `DoubleType` | $rn \le N$ | Giá cao nhất của sản phẩm tương tác trong $N$ sự kiện đầu. |
| 10| `min_price` | `DoubleType` | $rn \le N$ | Giá thấp nhất của sản phẩm tương tác trong $N$ sự kiện đầu. |
| 11| `interaction_duration` | `LongType` | $rn \le N$ | Thời lượng tương tác tính bằng giây: $\text{time}(rn=N) - \text{time}(rn=1)$. |
| 12| `hour_of_day` | `IntegerType` | $rn = N$ | Giờ trong ngày (0 đến 23 theo UTC) tại sự kiện thứ $N$. |
| 13| `day_of_week` | `IntegerType` | $rn = N$ | Ngày trong tuần (1 = Chủ nhật, 7 = Thứ bảy) tại sự kiện thứ $N$. |
| 14| `last_event_type` | `StringType` | $rn = N$ | Loại tương tác tại đúng sự kiện thứ $N$ (`view`, `cart`, `remove_from_cart`). |
| 15| `prev_event_type` | `StringType` | $rn = N - 1$ | Loại tương tác tại sự kiện liền trước $N$ (bằng `null` nếu $N=1$). |

### Nguyên tắc nghiêm ngặt về đặc trưng:
- Mọi feature chỉ được tính từ tiền tố $rn \in [1, N]$.
- **Không dùng thông tin toàn phiên:** Cấm dùng tổng số sự kiện của cả phiên (`n_events`), tổng thời lượng cả phiên, hoặc thời điểm kết thúc phiên.
- **Không dùng dữ liệu tương lai:** Cấm mọi dữ liệu phát sinh sau Prediction Point ($rn > N$).
- **Vai trò của định danh:** Các trường định danh (`user_session`, `user_id`) có thể được sử dụng cho các thao tác gom nhóm (`groupBy`), phân vùng (`Window`), hoặc chia tập (`split`), nhưng **tuyệt đối không được đưa vào vector đặc trưng (`features`) của mô hình**.

---

## 9. Phân Chia Tập Dữ Liệu Theo Thời Gian (Temporal Split)

### 9.1. Nguyên tắc cốt lõi
- Tuyệt đối **KHÔNG sử dụng Random Split** (chia ngẫu nhiên). Trong dữ liệu chuỗi thời gian người dùng, chia ngẫu nhiên sẽ làm mô hình học trước hành vi tương lai để dự đoán quá khứ, gây sai số nghiêm trọng khi triển khai thực tế (Lookahead Bias).
- Việc phân chia bắt buộc phải dựa trên dòng thời gian: Toàn bộ dữ liệu **Train** xảy ra trước dữ liệu **Validation**, và toàn bộ dữ liệu **Validation** xảy ra trước dữ liệu **Test**.

### 9.2. Hiện trạng Codebase (`src/ml/dataset/split.py`)
- Code hiện tại sử dụng trường **`session_start_date`** (`DateType` theo UTC) làm căn cứ phân chia.
- Thuật toán sắp xếp danh sách các ngày phân biệt tăng dần và chia theo tỷ lệ ngày động:
  - **Train:** 70% số ngày đầu tiên.
  - **Validation:** 15% số ngày tiếp theo.
  - **Test:** 15% số ngày cuối cùng.
- Bảo đảm tính rời rạc: $\text{train\_dates} \cap \text{val\_dates} = \emptyset$ và $\text{val\_dates} \cap \text{test\_dates} = \emptyset$. Mỗi session chỉ xuất hiện trong duy nhất một tập.

### 9.3. Đề xuất hợp đồng chính thức
- Hợp đồng production chính thức đề xuất sử dụng **các mốc ngày cố định (Fixed Cutoff Dates)**:
  - `train_end_utc` (YYYY-MM-DD): Ngày kết thúc của tập Train.
  - `validation_end_utc` (YYYY-MM-DD): Ngày kết thúc của tập Validation.
  - Tập Test: Bắt đầu từ ngày kế tiếp sau `validation_end_utc` đến hết phạm vi dữ liệu đã loại trừ right-censoring.
- **Trạng thái:** Các mốc ngày này hiện vẫn là **TBD (Chưa chốt)** trong cấu hình `configs/config.example.yaml` (dòng 115-116) và Mục 9 của `docs/data_contract.md`.

### 9.4. Trường hợp session vượt qua ranh giới tập dữ liệu
- *Vấn đề kỹ thuật:* Nếu một session bắt đầu vào cuối giai đoạn Train nhưng các sự kiện ở Tail lại kéo dài sang giai đoạn Validation. Việc chia theo `session_start_date` chỉ bảo đảm phiên thuộc về tập Train, nhưng thông tin dùng để gán nhãn lại diễn ra trong tương lai của tập Validation.
- *Yêu cầu xem xét:*
  1. Thống nhất quy định về cửa sổ thời gian kết thúc quan sát nhãn (label observation window).
  2. Xác định cơ chế xử lý: có cần loại bỏ hoặc giới hạn các phiên kéo qua ranh giới split hay không.
  3. Bảo đảm Spark Session luôn chạy với múi giờ UTC (`spark.sql.session.timeZone = "UTC"`) khi chuyển đổi `event_time` sang `session_start_date`.
- Chính sách này cần được Người A và B thống nhất, không tự ý chọn khi chưa có phê duyệt.

### 9.5. Định nghĩa ngày của Session
- **Trong Funnel Analytics (Mục 5.6 của A):** Ngày của phiên là ngày UTC của sự kiện `view` đầu tiên.
- **Trong Codebase ML hiện tại (`build_session_dataset.py`, dòng 84):** Đang lấy `min(to_date(event_time))` (ngày của sự kiện đầu tiên bất kể loại tương tác).
- Không mặc định hai định nghĩa này phải giống nhau. Vấn đề này được đưa vào danh sách cần Người A xác nhận (Mục 12).

---

## 10. Ngăn Chặn Rò Rỉ Dữ Liệu (Data Leakage Prevention)

Hợp đồng quy định 5 ranh giới chống rò rỉ dữ liệu nghiêm ngặt:

1. **Ranh giới Feature - Label (Head/Tail Separation):**
   - Mọi feature chỉ được hình thành từ $rn \le N$.
   - Mọi tính toán liên quan đến nhãn chỉ được hình thành từ $rn > N$.
   - *Bằng chứng từ codebase:* Codebase đã có Unit Test `test_tail_mutation_does_not_affect_features` để kiểm chứng việc thay đổi Tail không ảnh hưởng đến Head features. Kết quả thực thi trên VM B sẽ được xác minh ở bước kiểm thử riêng.
2. **Ranh giới phân vùng Session (Session Disjointness):**
   - Mỗi session gắn chặt với duy nhất một `session_start_date` và chỉ xuất hiện trong đúng một tập (Train, Validation hoặc Test).
3. **Ranh giới thời gian (Temporal Causality):**
   - Mô hình học từ quá khứ để dự đoán tương lai, không dùng dữ liệu tương lai để suy luận quá khứ.
4. **Ranh giới tiền xử lý đặc trưng (Pipeline Fit Separation):**
   - Tất cả các phép biến đổi đặc trưng có tham số (tính mean/std của giá trị số, xây dựng từ điển nhãn của `StringIndexer`, OneHotEncoder) **chỉ được phép thực hiện `fit()` trên tập Train**.
   - Tập Validation và Test chỉ được áp dụng phép `transform()` từ mô hình tiền xử lý đã fit trên Train.
5. **Ranh giới tối ưu siêu tham số và đánh giá (Evaluation Integrity):**
   - Giá trị $N$, lựa chọn mô hình và ngưỡng phân loại tối ưu (classification threshold) **chỉ được lựa chọn bằng dữ liệu Train và Validation**.
   - Tập Test được giữ hoàn toàn độc lập chỉ phục vụ đánh giá cuối cùng.
   - Tuyệt đối không sử dụng nhãn từ dữ liệu bị cắt biên để đánh giá mô hình.

---

## 11. Bảng Đối Chiếu Hiện Trạng Codebase (Implementation Audit)

Bảng tổng hợp đối chiếu giữa quy chuẩn hợp đồng và mã nguồn thực tế sau khi hoàn thành Static Code Review Bước 1:

| Hạng mục / Quy tắc | Hiện trạng trong Codebase | Đánh giá | Hành động dự kiến ở các bước sau |
|---|---|---|---|
| **Tham số Prediction Point $N$** | Đang đặt tên biến là `k`, mặc định `k=5` trong [`build_session_dataset.py`](../src/ml/dataset/build_session_dataset.py). | **PHÙ HỢP TỪ STATIC REVIEW** về mặt tham số hóa;<br>**CHƯA TƯƠNG THÍCH** thuật ngữ hợp đồng. | Chuẩn hóa tài liệu gọi là $N$. Giữ alias tham số trong code để tương thích ngược. Chờ chốt $N$ chính thức. |
| **Không rò rỉ dữ liệu (Anti-Leakage)** | Chỉ lấy đặc trưng từ $rn \le k$. Sự kiện thứ $k$ được đưa vào feature. Không dùng tail. | **PHÙ HỢP TỪ STATIC REVIEW** (Đã có unit test cấu trúc). | Giữ nguyên kiến trúc Head/Tail hiện tại. |
| **Thứ tự sự kiện (Ordering Tie-breaker)** | Đang sắp xếp theo `(event_time, type_rank, product_id)`. | ❌ **CHƯA TƯƠNG THÍCH** với Data Contract của A (Mục 5.3 quy định dùng `event_sort_key`). | Cập nhật `orderBy` sang `event_sort_key` sau khi A xác nhận nguồn cột và quy tắc hash. |
| **Định nghĩa Label nhị phân** | `label=1` nếu có purchase tại $rn > k$, ngược lại `label=0`. | **PHÙ HỢP TỪ STATIC REVIEW** với quy tắc Head/Tail, nhưng phụ thuộc vào chính sách censoring. | Giữ nguyên logic cốt lõi; bổ sung cơ chế kiểm soát biên dữ liệu. |
| **Loại bỏ Purchase sớm** | Session có purchase tại $rn \le k$ bị loại bỏ (`has_purchase_head == 0`). | **PHÙ HỢP TỪ STATIC REVIEW** | Giữ nguyên quy tắc logic. |
| **Loại bỏ Session ngắn** | Loại bỏ session có $n \le k$ sự kiện (`n_events > k`). | **PHÙ HỢP TỪ STATIC REVIEW** | Thống nhất với A về trường hợp $n = N$. |
| **Lọc `user_session` không hợp lệ** | Lọc `user_session.isNotNull()`. | ⚠️ **CHƯA XỬ LÝ ĐỦ** (chưa chặn chuỗi rỗng `""` và khoảng trắng). | Bổ sung kiểm tra `trim(user_session) != ""` trong bước làm sạch. |
| **Thống kê loại trừ không chồng lấn** | Lọc phân cấp tuần tự từng điều kiện loại trừ trong hàm EDA. | **PHÙ HỢP TỪ STATIC REVIEW** | Giữ nguyên để phục vụ báo cáo chất lượng dữ liệu. |
| **Right-Censoring (Cắt biên dữ liệu)** | Loại bỏ toàn bộ session có `session_start_date == max_date`. | ❌ **CHƯA TƯƠNG THÍCH** khi chạy mẫu 1 ngày H02 (gây drop 100% dữ liệu); heuristic chưa đủ bảo đảm. | Thống nhất chính sách tổng quát với A; bổ sung chế độ test mẫu nhỏ. |
| **Trật tự Temporal Split** | Chia theo `session_start_date` tăng dần. $\text{Train} < \text{Val} < \text{Test}$. Không random. | **PHÙ HỢP TỪ STATIC REVIEW** trong [`split.py`](../src/ml/dataset/split.py). | Giữ nguyên nguyên lý chia theo thời gian. |
| **Cơ chế ranh giới Split** | Chia theo tỷ lệ phần trăm ngày động (70/15/15). | ❌ **CHƯA TƯƠNG THÍCH** với Data Contract dự kiến (Fixed Cutoff Dates). | Bổ sung chế độ chia theo ngày cố định khi Người A chốt các mốc ngày. |
| **Định nghĩa ngày của Session** | Code ML dùng `min(to_date(event_time))`; Funnel của A dùng ngày View đầu tiên. | ❓ **CHƯA XÁC MINH** | Chờ Người A xác nhận tính phù hợp. |

---

## 12. Những Điểm Cần Người A và Người B Thống Nhất

| # | Hạng mục cần chốt | Hiện trạng kỹ thuật | Điểm cần Người A và Người B thống nhất |
|---|---|---|---|
| 1 | **Giá trị $N$ chính thức** | Đang dùng tạm $k=5$ trong code. | Người A cung cấp kết quả phân tích phân phối độ dài phiên từ Task A6 / A2 để hai bên chốt giá trị $N$ chính thức (ví dụ: $N=3, 5$ hoặc $10$). |
| 2 | **Nguồn và quy cách cột `event_sort_key`** | Code ML đang dùng `product_id`. | Xác nhận Người A sẽ lưu sẵn cột `event_sort_key` trong Curated Parquet hay tầng ML tự tính bằng SHA-256; thống nhất quy cách ép kiểu chuỗi 9 trường. |
| 3 | **Định nghĩa ngày bắt đầu phiên cho ML** | ML dùng `min(event_time)`, Funnel dùng ngày `view` đầu tiên. | Xác nhận ML có bắt buộc phải dùng ngày View đầu tiên hay dùng thời điểm bắt đầu phiên thực tế. |
| 4 | **Chính sách hoàn chỉnh phiên và Cắt biên dữ liệu** | Code đang loại bỏ ngày cuối cùng theo heuristic. | Thống nhất chính sách hoàn chỉnh phiên và xử lý Left/Right-Censoring tổng quát ở cả hai đầu thời gian; cho phép chế độ test mẫu nhỏ H02. |
| 5 | **Quy tắc đối với phiên có đúng $N$ sự kiện** | Code hiện loại bỏ ($n\_events > N$). | Xác nhận phiên có đúng $N$ sự kiện và không mua hàng có được coi là `label=0` hay bắt buộc loại bỏ vì thiếu quan sát ở đuôi. |
| 6 | **Mốc thời gian Split chính thức** | Cấu hình đang để `null / TBD`. | Người A và B chốt 02 mốc ngày cụ thể: `train_end_utc` và `validation_end_utc`. |
| 7 | **Session vượt ranh giới Split & Cửa sổ nhãn** | Chia theo ngày bắt đầu có thể kéo đuôi sang tập sau. | Thống nhất quy định về cửa sổ kết thúc quan sát nhãn và chính sách xử lý phiên kéo qua ranh giới split. |
| 8 | **Đồng bộ tham số cấu hình** | `config.yaml` có cả `prediction_point_n: null` và `k_events: 5`. | Thống nhất chuẩn hóa tên key trong `config.yaml` khi chốt Contract. |
| 9 | **Phê duyệt và hợp nhất tài liệu Data Contract** | Bản đề xuất hiện nằm riêng tại `docs/ml_contract_DRAFT.md`. | Quy trình review của Người A để sáp nhập chính thức vào **Mục 9** của [`docs/data_contract.md`](./data_contract.md) tại mốc H04. |

---

## 13. Checklist Nghiệm Thu Tiêu Chí H04

- [x] Đã hoàn thành Static Code Review đối chiếu logic dữ liệu ML hiện có.
- [x] Đã soạn thảo tài liệu đề xuất ML Data Contract DRAFT (`docs/ml_contract_DRAFT.md`) đầy đủ 13 mục.
- [x] Đã rà soát và đối chiếu danh mục 15 đặc trưng tiền tố hiện có trong codebase.
- [x] Đã xác định rõ các điểm chưa tương thích (Event ordering dùng `product_id`, Heuristic right-censoring trên H02, Fixed Cutoff Dates).
- [x] Đã hoàn thiện bản DRAFT theo các phản hồi nghiệp vụ và định dạng kỹ thuật chuẩn.
- [ ] Người A và B review và thống nhất 9 điểm mở tại Mục 12.
- [ ] Chốt giá trị $N$ chính thức và mốc thời gian Temporal Split.
- [ ] Cập nhật mã nguồn `build_session_dataset.py` (sửa `orderBy` sang `event_sort_key`, xử lý right-censoring, lọc khoảng trắng).
- [ ] Cập nhật mã nguồn `split.py` (hỗ trợ phân chia theo mốc ngày cố định).
- [ ] Bổ sung Unit Tests kiểm chứng toàn bộ các trường hợp biên của ML Contract.
- [ ] Chạy kiểm thử thực tế và xác minh kết quả trên máy ảo VM B.
- [ ] Hai bên nghiệm thu và hợp nhất nội dung vào Mục 9 của `docs/data_contract.md`.

# P5-B-B6-01 — Prediction point chính thức N=3 và phân tích ứng viên N

**Trạng thái:**
- **Prediction point chính thức: N=3** (quyết định thiết kế của thành viên B, khai báo `selected_n: 3` trong `configs/n_candidate_analysis.example.yaml`).
- **H11 = PENDING_A_REVIEW.** Thành viên A chưa phê duyệt. Chưa có phê duyệt S2 chung.
- Triển khai và test trên Windows hoàn tất. **Chưa chạy trên VM B.**

## 1. Những gì không được hiểu nhầm

- N=3 là **quyết định thiết kế** của B, dựa trên đề xuất P5-A-S2-01 của A và phân bố session trong H10. Không phải kết quả tối ưu hóa N.
- Không dùng validation hoặc test để chọn N. Cờ `validation_or_test_used_for_selection: false` và `test_labels_used_for_selection: false` được kiểm tra bằng test.
- Chế độ `--analysis-scope acceptance` (mặc định) chỉ phân tích `selected_n`. Chế độ `--analysis-scope reference` phân tích N=2/3/5 để đối chiếu, không đổi N chính thức.
- Không có tham số ghi đè N trên CLI (`--n` bị từ chối).
- Bảng N=2/3/5 chỉ là bằng chứng so sánh. Không phải căn cứ để đổi N.
- Cửa sổ thời gian H11 là **đề xuất, đang chờ A review**. Chưa có phê duyệt S2 chung.
- Các con số H10 của A (23.016.650 sessions, 6,10% có purchase…) là thống kê mô tả trên toàn bộ dữ liệu, dùng `has_purchase` bất kỳ trong phiên. **Không phải nhãn sau N** và không so khớp trực tiếp với kết quả ở đây, vì cửa sổ H11 loại trừ test range và censor các session qua ranh giới.
- Không dùng test range để chọn N hoặc threshold. Script không đọc nhãn test.

## 2. Logic (tóm tắt)

| Thành phần | Quy tắc |
|---|---|
| Thứ tự sự kiện | `event_time` UTC ASC → `event_type_rank` ASC → `event_sort_key` ASC (tái sử dụng `ml.features.rank_session_events`) |
| Phân loại session | `too_short` (ít hơn N event) → `early_purchase` (purchase tại hoặc trước N) → `eligible`. Không chồng lấp, cùng thứ tự với B6 đã nghiệm thu |
| Label | `1` nếu có purchase tại `event_idx > N` trong cùng session; `0` nếu không |
| Gán split | Session thuộc train/validation **chỉ khi toàn bộ event** nằm trong khoảng đó (`session_end < cuối cửa sổ`). Session bắt đầu trong cửa sổ nhưng kết thúc sau ranh giới bị **censor**, đếm riêng, không gán nhãn |
| Test range | Chỉ đếm theo thời điểm bắt đầu. Không ranking, không nhãn |
| Session NULL/blank | Đếm theo **số event**, không gọi là số session |

Bất biến QA (kiểm tra từng lần chạy):
- `in_scope = too_short + early_purchase + eligible`
- `eligible = label_0 + label_1`
- Không event candidate nào nằm ở hoặc sau cuối split của chính nó
- Không session nào ngoài train/validation được ranking

## 3. Test

| File | Số test | Nội dung |
|---|---:|---|
| `tests/unit/test_p5_b6_01_n_selection.py` | 23 | N=2/3/5 kỳ vọng tính tay; partition và label-sum; coverage; invalid session; censor; test range không ảnh hưởng (adversarial); suffix không đổi prefix B6; khớp audit B6 (oracle); repeatability; out-of-order và repartition; timestamp trùng; biên window; QA; CLI từ chối |

Kết quả Windows (PySpark 4.0.1, Python 3.11.9): **23 passed**, exit 0.

Kiểm chứng test có khả năng bắt lỗi: mutant tắt censor làm 4 assertion thất bại (t03, t04, t08, t10).

## 4. Chạy trên Windows (kiểm tra cục bộ, dữ liệu synthetic)

```powershell
cd E:\Data_SamSung\Github\ecommerce-bigdata-capstone
python scripts\run\run_p5_b6_01_n_analysis.py --output-dir reports\P5_B_B6_01\<RUN_ID> --run-id <RUN_ID>
```
Lệnh trên dùng `configs/n_candidate_analysis.example.yaml`. Trên dữ liệu synthetic tháng 11 ngày 1–6 (không nằm trong H11), **validation rỗng nên QA FAIL có chủ đích** (đã kiểm: `reports/P5_B_B6_01/win_synthetic_h11_windows/`, exit 1).

Cấu hình khớp dữ liệu synthetic cho kiểm tra PASS: `reports/P5_B_B6_01/win_synthetic_windowed/` (exit 0). Đây **không phải dữ liệu REES46 thật**.

## 5. Chạy trên VM B (cần thực hiện để nghiệm thu)

Cấu hình VM: Python 3.8.11, Spark/PySpark 3.1.2, Java 8, 2 CPU, RAM ~4,6 GB. Runner dùng:
```bash
export CAPSTONE_SPARK_MASTER='local[2]'
export CAPSTONE_DRIVER_MEMORY='1g'
export CAPSTONE_SHUFFLE_PARTITIONS=4
export PYSPARK_SUBMIT_ARGS='--driver-memory 1g pyspark-shell'
```

Bước 1 — chạy chế độ kiểm thử (mẫu 0,1% session, không full):
```bash
cd /media/sf_ecommerce-bigdata-capstone
RUN_ID=p5_vm_sample_1 bash scripts/run/run_p5_b6_01_vm_b.sh
```

Runner chạy:
- **E5** (acceptance): chỉ N=3 (`sample/`).
- **E5b** (reference): N=2/3/5 để đối chiếu (`reference/`).

Bước 2 — kiểm tra kết quả E1–E5b trong `reports/P5_B_B6_01/p5_vm_sample_1/vm_b_summary.txt` và `sample/run_manifest.json` (`analysis_scope: acceptance`, `selected_n: 3`, `qa_pass: true`) trước khi cân nhắc bước full.

Bước 3 (chỉ khi đã duyệt, và RAM còn đủ theo `free -m`) — full Curated:
```bash
FULL_RUN=1 CONFIRM_FULL=1 RUN_ID=p5_vm_full_1 bash scripts/run/run_p5_b6_01_vm_b.sh
```
Lưu ý: full quét toàn bộ 109.950.743 event. Runner không có watchdog thời gian/RAM. Theo dõi `free -m` và swap trong khi chạy.

Runner không ghi đè `RUN_ID` đã tồn tại, không ghi vào HDFS, không chạm Curated, không chạm B7-01/B7-02 output.

## 6. Output và bằng chứng

Trong `reports/P5_B_B6_01/<RUN_ID>/`:

| File | Nội dung |
|---|---|
| `vm_b_summary.txt` | `OVERALL`, trạng thái từng bước |
| `vm_b_run.log` | log đầy đủ |
| `environment.json` | Python, PySpark, Java |
| `source_integrity.json` | SHA-256 của 13 file P5 |
| `e3_preflight.json` | Curated metadata (số file, schema, không quét dòng) |
| `p5_junit.xml`, `p5_pytest.log` | 23 test P5 |
| `e4b_*.log` | Regression B6 |
| `sample/n_candidate_result.json` | Bảng đầy đủ: N, split, exclusion, label, coverage, positive rate, boundary QA, leakage QA |
| `sample/n_candidate_table.md` | Bảng so sánh N ở dạng đọc nhanh |
| `sample/run_manifest.json` | Config snapshot, Spark/Python, cửa sổ UTC, `qa_pass`, `qa_problems` |

## 7. Điều kiện PASS/FAIL/BLOCKED

- **PASS** (`OVERALL=PASS`, exit 0): E2, E3, E4a, E4b, E5 đều PASS và `qa_pass=true` trong `run_manifest.json`.
- **FAIL** (exit 1): có bước FAIL, hoặc QA invariant không thỏa (partition, label-sum, split rỗng, train không có nhãn dương, event candidate ở sau cuối split).
- **BLOCKED** (exit 2): thiếu file bắt buộc, không đọc được manifest, Curated không tồn tại, hoặc thiếu môi trường.

PASS trên Windows **không** đồng nghĩa PASS trên VM B.

## 8. Việc còn lại trước khi chọn N

1. Chạy E1–E5 trên VM B và đưa `vm_b_summary.txt` cùng `n_candidate_table.md` vào review.
2. Đối chiếu tổng bucket với H10 (session hợp lệ = train + validation + censored + test range + gap + before train). Đây là bước reconciliation, không phải so sánh bằng nhau.
3. A review H11. Nếu cửa sổ thay đổi, sửa `configs/n_candidate_analysis.example.yaml` và tạo manifest mới.
4. N=3 đã được B chốt là quyết định thiết kế. Nếu A không phê duyệt H11 hoặc bằng chứng VM B cho thấy N=3 không khả thi (ví dụ validation rỗng, train không có nhãn dương, coverage quá thấp), phải quay lại quyết định với A/B. Không đổi N bằng cách xem test.
5. Chỉ sau đó mới chuyển sang B6-02 Full Features (không thuộc task này).

## 9. Điều tra gap sessions (136 session trên mẫu 0,1%)

**Kết quả VM B đã biết (run `p5_n3_vm_b_sample_v1`, không bị ghi đè):** train 13.900, validation 5.471, test_range 3.402, censored_train 5, censored_validation 9, **gap 136** (tổng 22.923 = số session mẫu).

**Nguyên nhân có thể (giả thuyết, CHƯA xác nhận):** trước khi sửa, `classify_sessions` gán mọi session không khớp nhánh nào vào `gap` (`.otherwise`). Hai nhóm có thể nằm trong đó:
- session có `session_start` NULL, tức mọi event của session có `event_time` NULL;
- session bắt đầu từ `test_end` (2019-12-01 UTC) trở đi, vì code không có nhánh cho trường hợp này.

Session có **một phần** event `event_time` NULL cũng bị xử lý sai trước đây: `rank_session_events` sắp NULL lên đầu, nên thứ tự event không đáng tin mà không báo lỗi.

**Đã thay đổi trên code (Windows):**
- Bucket `excluded_null_event_time`: session có bất kỳ event `event_time` NULL bị loại trước khi ranking và được đếm riêng.
- Bucket `after_test_end`: session bắt đầu từ `test_end` trở đi, đếm riêng, không ranking.
- Bucket `gap` chỉ còn cho session không nhóm nào giải thích được. **QA FAIL** nếu `gap > 0`. Không có session nào bị bỏ qua âm thầm.
- Cảnh báo (không làm FAIL): số session loại vì NULL time, số session sau test_end, số event NULL trong session hợp lệ. Được ghi trong `run_manifest.json` mục `qa_warnings`.

**Chế độ diagnostic (không ranking, không nhãn):**
```bash
RUN_ID=p5_gap_diag_1 bash scripts/run/run_p5_b6_01_vm_b.sh        # E5c chạy cùng mẫu 0,1%
# hoặc trực tiếp:
python3 scripts/run/run_p5_b6_01_n_analysis.py --mode sample --sample-percent 0.1 \
  --diagnose-only --output-dir reports/P5_B_B6_01/p5_gap_diag_1/diagnostic --run-id p5_gap_diag_1_diagnostic
```
Kết quả: `gap_diagnostic.json` với số session theo bucket, ngày bắt đầu min/max của từng bucket, số session không có start, số event NULL time, phân bố ngày của session sau test_end, và `unexplained_gap_sessions`.

**Điều kiện trước khi chạy Full:**
1. `unexplained_gap_sessions = 0` trên cùng mẫu 0,1%.
2. Đối chiếu tổng: 136 gap cũ = `excluded_null_event_time` + `after_test_end` + phần còn lại. Nếu phần còn lại > 0, dừng và điều tra.
3. Đã xem `qa_warnings` và chấp nhận từng nhóm loại trừ.
4. A review nhóm `excluded_null_event_time` và `after_test_end`.
5. Không thay đổi N=3, thứ tự S2, định nghĩa label, prefix features, ranh giới split hay trạng thái H11 để làm QA PASS.

## 10. Engine staged, so sánh nghiệp vụ và bảo vệ tài nguyên

**Trạng thái:** code và test trên Windows hoàn tất. **Chưa chạy trên VM B. Chưa có số đo benchmark. Chưa chạy Full.**

### 10.1 Hai engine
- `--engine legacy`: implementation đã nghiệm thu, dùng làm **oracle**. Không xóa, không sửa.
- `--engine staged`: Phase A (tóm tắt session trên toàn bộ event, một action) → gate độ dài nhóm window → Phase B (ranking S2 đầy đủ với `event_sort_key`, join ở mức session, một action).
  - `--ranking-input semi_join`: chỉ ranking event của session train/validation (LEFT SEMI JOIN với Phase A).
  - `--ranking-input time_filter`: ranking event có `event_time < validation_end`.
  - `--materialize disk_persist` (DISK_ONLY) hoặc `staging` (Parquet trong thư mục mới).
  - `--max-window-group N`: chặn Phase B (BLOCKED, exit 2) nếu nhóm lớn nhất vượt N. Mặc định chỉ ghi nhận.
  - `--aqe keep|on|off`: Spark 3.1 mặc định tắt AQE; giá trị thực tế được ghi vào `run_manifest.json`.
- Physical plan của Phase A và Phase B được ghi vào `plan_phase_a.txt`, `plan_phase_b.txt`. Danh sách action được ghi vào `execution.spark_actions`.

### 10.2 So sánh giá trị nghiệp vụ
`scripts/run/compare_p5_results.py --baseline A.json --candidate B.json --out cmp.json` so sánh **giá trị**, không so JSON theo byte. So sánh: số event, số session theo bucket, in_scope, too_short, early_purchase, eligible, label_0, label_1, len_equals_n cho train/validation/train+validation, và QA leakage phải bằng 0. Bỏ qua run_id, thời gian, plan, config, thứ tự trường. Exit 1 nếu có khác biệt.

### 10.3 Bảo vệ tài nguyên
- `scripts/run/p5_resource_preflight.py`: RAM available (mặc định ≥ 1200 MB), swap (cảnh báo), đĩa của `spark.local.dir` (≥ 30 GB), HDFS (≥ 5 GB), không có SparkSubmit khác, thư mục output phải mới. Không đạt → BLOCKED (exit 2).
- `scripts/run/p5_eventlog_metrics.py`: tổng hợp **số đo thật** từ event log Spark (shuffle đọc/ghi, spill memory/disk, peak execution memory, thời gian). Không ước lượng.
- Runner ghi `resource_monitor.log` (RAM, swap, đĩa, dung lượng `spark_local`) trong suốt run.
- Spark local dir, event log và staging nằm dưới `$HOME/p5_b6_01_work/<RUN_ID>`, **không** trong Shared Folder.

### 10.4 Các bước của runner VM B
| Bước | Nội dung |
|---|---|
| E5 | legacy, acceptance N=3, mẫu 0,1% (oracle) |
| E5b | legacy, reference N=2/3/5 (chỉ so sánh) |
| E5c | diagnostic gap, không ranking, không nhãn |
| E5s / E5t / E5p | staged: semi_join+disk_persist / time_filter+disk_persist / semi_join+staging |
| E5r | staged, reference N=2/3/5 |
| E5k | staged với `TZ=Asia/Seoul`: kết quả phải không đổi |
| E5cmp | so sánh E5s/E5t/E5p/E5k với baseline UTC đã nghiệm thu; E5r với E5b |
| E6p | preflight tài nguyên cho Full |
| E6 | Full N=3, **chỉ khi** `FULL_RUN=1` và `CONFIRM_FULL=1` và mọi bước trước PASS |

### 10.5 Lệnh chạy (chưa chạy)
```bash
cd /media/sf_ecommerce-bigdata-capstone
# Bước 4: so sánh 0,1% (không Full)
RUN_ID=p5_staged_cmp_1 bash scripts/run/run_p5_b6_01_vm_b.sh
# Bước 5: benchmark 1% (không Full)
RUN_ID=p5_bench_1pct_1 SAMPLE_PCT=1 bash scripts/run/run_p5_b6_01_vm_b.sh
```
Full chỉ được xem xét sau khi bước 4 và 5 đạt GO. Lệnh Full không nằm trong tài liệu này để tránh chạy nhầm.

### 10.6 Kết quả kiểm thử Windows (PySpark 4.0.1, Python 3.11.9)
- `tests/unit/test_p5_b6_01_n_selection.py`: **49 passed, 1 skipped**, exit 0. Test skipped là t34 (chạy KST trên Linux).
- Mutant tắt containment (censor): 6 test thất bại (t35, t36).
- Mutant cho phép test range vào ranking: 1 trong 2 biến thể t37 thất bại.
- CLI staged trên synthetic cùng cửa sổ với bản legacy: **EQUAL** theo `compare_p5_results.py`.
- Chưa có kiểm chứng trên Spark 3.1.2. Các API mới đã đối chiếu với tài liệu, cần chạy trên VM để xác nhận.

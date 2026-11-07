# P5-B-B6-01 — Phân tích ứng viên N cho prediction point chính thức

**Trạng thái:** triển khai trên Windows hoàn tất (code, test, runner). **Chưa chạy trên VM B. Chưa chọn N.**

Tài liệu này là hướng dẫn chạy lại. Không mang kết luận nào về N.

## 1. Những gì không được hiểu nhầm

- Script **không chọn N**. Bảng so sánh N=2, 3, 5 chỉ là bằng chứng.
- `N=3` là giả thuyết ưu tiên ban đầu của A, không phải kết quả.
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

Bước 2 — kiểm tra kết quả E1–E5 trong `reports/P5_B_B6_01/p5_vm_sample_1/vm_b_summary.txt` trước khi cân nhắc bước full.

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
4. Quyết định N dựa trên bằng chứng (cân bằng giữa dự đoán sớm, lịch sử quan sát, coverage, số nhãn dương, mất cân bằng lớp, khả năng dùng cho Logistic Regression ở P6). Không dùng test.
5. Chỉ sau đó mới chuyển sang B6-02 Full Features (không thuộc task này).

# P4-B-B7-01 — Acceptance Report: Logistic Regression Smoke Test trên B6 Session Features

> Written for: nhóm dự án (Người A, Người B) làm reviewer nghiệm thu.
> Trạng thái: **P4-B-B7-01 PARTIALLY VERIFIED — AWAITING VM B REAL-DATA SMOKE TEST**

---

## H1. Thông tin nhiệm vụ

| Hạng mục | Giá trị |
|---|---|
| Task | P4-B-B7-01 — Dựng/hoàn thiện Logistic Regression pipeline train/validation và smoke test trên mẫu B6 |
| Phạm vi | Smoke train/validation. Không test cuối, không chọn threshold, không ghi model/metrics chính thức |
| Branch | `feature/P3-B-B6-02-labels` |
| Git HEAD khi bắt đầu | `ab24868bd511002557b2db084373f57ff2c6a93f` (working tree sạch) |
| Thay đổi | Chưa commit. Xem danh sách tại H5 |
| Người thực hiện | Agent Claude Code làm việc với Huong (Người B) |
| Môi trường Windows | Windows 10 Pro, Python 3.11.9 trong venv riêng (scratchpad), PySpark 4.0.1, Java 21 (Temurin), `HADOOP_HOME=C:\hadoop` |
| Môi trường VM B | **Chưa truy cập được từ phiên này.** Không có terminal/SSH tới VM B. Theo báo cáo B6-01: Spark 3.1.2, Python 3.8.11 |
| Dữ liệu thật | **Chưa chạy.** Trạng thái: BLOCKED (xem H7) |

---

## H2. Phân chia trách nhiệm hai môi trường

| Môi trường | Làm gì trong B7-01 |
|---|---|
| Windows | Sửa mã nguồn, viết unit test, chạy static check, chạy test trên PySpark cục bộ với dữ liệu synthetic, viết báo cáo |
| VM B (Linux) | Đọc source qua Shared Folder, đọc Session Features B6 trên HDFS, chạy Spark, lưu log và report |
| Shared Folder `/media/sf_CapstoneRepo` | Đưa source từ Windows sang VM B. Script `scripts/run/run_b7_01_vm_b.sh` kiểm tra commit trước khi chạy |
| HDFS | Curated `/data/curated/events` (đầu vào của B6, chỉ đọc). Smoke output dưới `/data/ml/smoke/p4_b_b7_01/<run_id>/` |
| Windows không chạy dữ liệu thật | Dữ liệu REES46 không có trên Windows. Test Windows chỉ dùng synthetic, nên **không được coi là PASS thực nghiệm** |

---

## H3. Dependency B6 → B7

```
Curated Parquet  /data/curated/events  (HDFS, 13 cột, chỉ đọc)
      |
      |  ml/session_features.py : build_session_features(events, N)     [B6]
      |     rank_session_events (ml/features.py): event_time, event_type_rank, event_sort_key
      v
Session Features  (chỉ có trong code nếu chạy writer)                 [MỚI: scripts/run/run_b6_session_features.py]
      |  HDFS /data/ml/smoke/p4_b_b7_01/<run_id>/session_features
      v
src/ml/training/lr_b6_smoke.py : run_b6_lr_smoke                      [MỚI: B7-01 smoke]
      |  train 70% ngày đầu | validation 15% ngày kế | ngày còn lại: KHÔNG ĐỌC
      |  StringIndexer -> OneHot -> VectorAssembler -> StandardScaler -> LogisticRegression
      v
reports/P4_B_B7_01/<run_id>/smoke_report.json  +  model smoke (không chính thức)
```

| Module | Chức năng | Đầu vào | Đầu ra | Gọi từ | Test hiện có |
|---|---|---|---|---|---|
| `ml/session_features.py` (B6) | Tạo đặc trưng prefix N + nhãn | Event đã có thứ tự S2 | 1 dòng/phiên: `user_session`, `n_view`, `n_cart`, `n_remove_from_cart`, `n_unique_*`, `avg_price`, `max_price`, `prefix_start`, `prediction_time`, `last_event_type`, `prefix_duration_seconds`, `label`, `brands_seen` | Test, `verify_b6_02_sample.py` | `test_b6_02.py` (10 test) |
| `ml/features.py` (B6-01) | Ranking và prefix | Event | `event_idx` | B6 | `test_ml_features.py`, `test_b6_01_edges.py` |
| `src/ml/training/lr_b6_smoke.py` (B7-01, mới) | Smoke LR trên schema B6 | Output B6 + `prediction_n` | Report JSON, model smoke | `run_b7_lr_smoke.py` | `test_b7_01_lr_smoke.py` (22 test) |
| `src/ml/training/train_lr.py` (nhánh cũ) | Pipeline chính thức cũ | Schema nhánh `src/ml/dataset` | Model, `lr_metrics.json` | Không được gọi B7 | `test_split_metrics.py` (schema nhánh cũ) |

---

## H4. Kết quả audit

Mức: Critical = chặn nghiệm thu; High = cần xử lý trước B8/P6; Medium = rủi ro cần quyết định; Low = ghi nhận.

| ID | Mức | Vấn đề | Bằng chứng | Tác động | Xử lý trong B7-01 |
|---|---|---|---|---|---|
| F1 | **Critical** | Hai nhánh schema không tương thích. `split_by_time` cần `session_start_date`; `get_feature_pipeline_stages` cần `view_count`, `cart_count`, `has_cart`, `prev_event_type`… Output B6 không có các cột này | Thực thi trên dữ liệu B6 synthetic: `split_by_time` → `AnalysisException: session_start_date cannot be resolved`; `feature_pipeline` fit → lỗi Py4J (thiếu cột) | `train_lr.py` không chạy được trên Session Features B6 | Thêm pipeline riêng cho schema B6 (`lr_b6_smoke.py`). Không sửa nhánh cũ |
| F2 | **Critical cho B8/B9** (không chặn smoke B7) | `train_lr.py` transform test: dòng 88–89 `model.transform(test_df)`; dòng 92–99 `evaluate_model(test_preds=...)`; dòng 83 ghi model vào `model_path`; dòng 98 ghi `results/metrics/lr_metrics.json` | Đọc `src/ml/training/train_lr.py` | Vi phạm "không transform/đánh giá test trong B7". File `results/metrics/lr_metrics.json` đã commit là kết quả của luồng này | Không dùng `train_lr.py` cho B7. **Chưa sửa** — cần tách ở B8/B9 |
| F3 | High | B6 chưa có writer. Không có code nào ghi Session Features ra Parquet. `configs` trỏ tới `data/processed/ml/session_features` (local, hiện trống) và `/data/ml/features/session_features` (HDFS). Không có bằng chứng file B6 tồn tại trên HDFS | `grep write` trong repo: chỉ có writer cho nhánh cũ và ledger B4. `data/processed/` chỉ có `.gitkeep` | Scenario A của E3 không xác minh được; phải có output B6 trước | Thêm `scripts/run/run_b6_session_features.py` (ghi mẫu theo session, mặc định 1%, không ghi đè) |
| F4 | High | Config không khớp đường dẫn đã xác nhận. `hdfs.curated.events_cleaned = /data/curated/events_cleaned`, nhưng Curated đã xác nhận là `/data/curated/events` | `configs/config.example.yaml` dòng 68; yêu cầu nhiệm vụ | Đọc nhầm đường dẫn nếu dùng config | Writer nhận `--curated-path`, mặc định `/data/curated/events`. Cần Người A xác nhận |
| F5 | High (rủi ro tiềm ẩn) | `VectorAssembler(handleInvalid="keep")` trong `feature_pipeline.py`. Theo tài liệu Spark, `keep` đưa NULL/NaN vào vector thành NaN, không chặn. **Chưa tái hiện trực tiếp** trong lần này | `src/ml/features/feature_pipeline.py` dòng 62–66 | Giá trị thiếu có thể đi vào LR | Smoke dùng `handleInvalid="error"` và kiểm tra NULL/NaN/inf trước khi fit |
| F6 | Medium | Output B6 không mang N. Không truy vết được N của dữ liệu. N=2 là giá trị thử nghiệm (B6-01/B6-02 đều ghi rõ); N chính thức TBD | `reports/B6_01`, `reports/B6_02`, `configs` `prediction_point_n: null` | Không chứng minh được N trong báo cáo | Writer ghi cột `prediction_n`; smoke yêu cầu đúng một giá trị |
| F7 | Medium | `split_by_time` đếm test partition (`test_count`, `test_pos`) trong bước split. Đây là đọc nhãn test, không transform | `src/ml/dataset/split.py` dòng 97–105 | Trái nguyên tắc "không đọc test" nếu dùng ở B7 | Smoke không gọi `split_by_time`; dùng `split_train_val_dates` (chỉ đọc cột ngày để tính cutoff) |
| F8 | Medium | Right-censoring chưa có trong B6. ML contract (§7) yêu cầu loại phiên bắt đầu ngày cuối; code nhánh cũ có heuristic. Với phiên bắt đầu ngày cuối, `label=0` có thể sai do dữ liệu bị cắt | `ml/session_features.py` không có điều kiện ngày; `docs/ml_contract_DRAFT.md` §7 | Ảnh hưởng nhãn ở ranh giới thời gian khi chạy chính thức | Không xử lý trong smoke. Ghi nhận cho B8/P6 |
| F9 | Medium | Chính sách phiên có đúng N sự kiện chưa chốt. B6 coi `n_events == N` là eligible với `label = 0` (không có sự kiện sau N); ML contract §6.1 để ngỏ | `ml/session_features.py` dòng 84–92; `docs/ml_contract_DRAFT.md` §6.1 | Ảnh hưởng phân phối nhãn | Không đổi B6. Cần A/B chốt tại mốc H04 |
| F10 | Medium | Môi trường chưa khớp. `requirements.txt` có `pyspark>=3.4`; VM B dùng Spark 3.1.2 / Python 3.8.11; Windows test dùng PySpark 4.0.1 / Python 3.11 | `requirements.txt`; báo cáo B6-01 | Test Windows không chứng minh được hành vi trên VM B | Đã dùng API có từ Spark 3.1 (`exists`, `vector_to_array`, `OneHotEncoder(inputCols)`, `xxhash64`). Test lại trên VM B (E4a) |
| F11 | Low | `results/metrics/lr_metrics.json` đã commit là kết quả nhánh cũ, có `AUC_test`, `F1_test`. Có `split_dates` từ 2019-11-01 đến 2019-11-29 | `results/metrics/lr_metrics.json` | Dễ bị nhầm là bằng chứng B7 | Không dùng làm bằng chứng. Báo cáo này không tham chiếu |
| F12 | Low | Nhận xét "hai statement trên cùng một dòng trong split" không tái hiện | `py_compile` các file ML OK; `grep` `;` trong `src/ml`, `ml/` chỉ có một comment | — | Không sửa |
| F13 | Low | `brands_seen` là array trong output B6; không đưa vào feature | `ml/session_features.py` | Không ảnh hưởng | Loại khỏi feature và kiểm tra bằng `FORBIDDEN_FEATURE_COLUMNS` |
| F14 | Low | Test `test_b6_02` (B6-02) trên VM B chưa có log PASS. Báo cáo B6-02 ghi "Chờ log" | `reports/B6_02/nov05_v1/acceptance_summary.md` | B6 chưa được nghiệm thu trên VM B | Chạy lại trong E4b (regression) |

---

## H5. Thay đổi mã nguồn

Vòng 1 (tạo pipeline) và vòng 2 (rà soát an toàn VM B). Có **một** file hiện có được sửa; còn lại là file mới.

| File | Trạng thái | Vấn đề ban đầu | Thay đổi | Lý do / rủi ro |
|---|---|---|---|---|
| `src/common/spark_session.py` | **sửa** (vòng 2) | `get_spark` cứng driver 2g, không có đường override | Đọc `CAPSTONE_SPARK_MASTER`, `CAPSTONE_DRIVER_MEMORY`, `CAPSTONE_SHUFFLE_PARTITIONS` nếu có; không đặt thì giữ nguyên mặc định | Chỉ ảnh hưởng khi biến môi trường được đặt. Rủi ro thấp; các test cũ không đặt biến này |
| `src/ml/training/lr_b6_smoke.py` | mới (vòng 1), sửa (vòng 2) | — | Vòng 2: thêm `sample_whole_sessions` (lấy mẫu theo phiên) và `distinct_session_dates_total` trong report | Dùng để kiểm tra đồng nhất ngày (xem H6, TC14) |
| `scripts/run/run_b6_session_features.py` | mới (vòng 1), sửa (vòng 2) | Không có writer; `count()`/`collect()` nhiều lần trên toàn Curated (mỗi lần quét 109M dòng); mặc định 1% | Materialise mẫu **một lần** (`persist MEMORY_AND_DISK`, rồi `unpersist`); kiểm tra cột trước khi quét; mặc định 0.1%; từ chối >1% nếu không có `--allow-large-sample`; từ chối ghi 0 phiên | Giảm số lượt quét từ ~5 xuống 1 |
| `scripts/run/run_b7_lr_smoke.py` | mới (vòng 1) | — | Không đổi logic; ghi report kể cả khi FAIL | — |
| `scripts/run/run_b7_01_vm_b.sh` | mới (vòng 1), **viết lại** (vòng 2) | Repo hardcode `/media/sf_CapstoneRepo`; `git` không có trên VM làm E2 PASS giả; E3 luôn ghi PASS; `record E3 0`; chạy tiếp sau lỗi; SAMPLE 1%; `head` che exit code | Repo xác định theo vị trí script (override `REPO`), chặn `sf_CapstoneRepo`; kiểm file bắt buộc trước khi tạo gì; RUN_ID mới, không ghi đè; gating theo phụ thuộc (PASS/FAIL/BLOCKED/SKIPPED); summary ghi kể cả khi lỗi (`trap`); exit 0/1/2 | Không dùng `head`/`tail` trên đường exit code |
| `scripts/run/source_manifest.py` | mới (vòng 2) | TC16 không có cách kiểm trên VM không có git | `make` (Windows, có git): HEAD, branch, mục chưa commit, SHA-256 của 24 file trọng yếu. `verify` (VM, stdlib): băm lại, so khớp; exit 0/1/2 | Không chứa env, token, email. Manifest mô tả **working tree snapshot** khi còn mục chưa commit |
| `scripts/run/preflight_b7_01.py` | mới (vòng 2) | E3 không thật sự kiểm tra gì | Chỉ đọc metadata: `inputFiles()`, schema; không quét dòng; không tạo `/data/ml` | Thiếu file/cột thì FAIL và dừng |
| `scripts/run/check_b7_01_evidence.py` | mới (vòng 2) | `dates_not_read=1` hardcode trong tiêu chí | Đánh giá tiêu chí từ file bằng chứng; ngày kiểm bằng đồng nhất `D = |train| + |val| + not_read` | Không so với hằng số |
| `tests/unit/test_b7_01_lr_smoke.py` | mới (vòng 1) | — | 22 test | — |
| `tests/unit/test_b7_01_tooling.py` | mới (vòng 2) | — | 14 test: manifest PASS/FAIL/BLOCKED, tiêu chí checker, lấy mẫu không cắt phiên, tái lập | — |
| `reports/P4_B_B7_01/windows_synthetic_e2e/` | mới (vòng 1) | — | Bằng chứng synthetic Windows. Không phải dữ liệu thật | — |
| `reports/P4_B_B7_01/source_manifest.json` | mới (vòng 2) | — | Manifest tạo trên Windows | Phải tạo **sau cùng**; mọi sửa file trọng yếu sau đó làm E2a FAIL |
| `docs/qa/P4_B_B7_01_acceptance.md` | báo cáo này | — | — | — |

Không sửa: `src/ml/training/train_lr.py`, `src/ml/features/feature_pipeline.py`, `src/ml/dataset/split.py`, `src/ml/evaluation/metrics.py`, `ml/session_features.py`, `ml/features.py`. Các file này **không nằm trong đường chạy VM** (xem F1, F2, F5, F7). Đưa vào manifest để xác nhận không đổi.

**Lựa chọn của agent (có thể đổi):**
- Không sửa `train_lr.py` và nhánh `src/ml/` cũ, để không ảnh hưởng B8/B9 và tránh sửa lớn ngoài phạm vi. Hệ quả: F2 vẫn mở.
- Smoke có tính AUC trên validation như một chỉ số kiểm tra (`auc_sanity_only`). Không dùng để chọn tham số hay threshold. Nếu bạn muốn bỏ hẳn để tránh trùng B8, chỉ cần xóa một khối trong `lr_b6_smoke.py`.
- Chọn cutoff train/val theo cùng quy tắc 70/15 với `split.py`, nhưng không đọc các ngày còn lại.

---

## H6. Kịch bản kiểm thử

Mức bằng chứng: **Static** (Windows), **Unit synthetic** (Windows), **Spark integration synthetic** (Windows), **Real B6 data** (VM B, chưa có). Không gộp các mức này.

| ID | Mục tiêu / Input | Expected | Actual | Status | Mức bằng chứng |
|---|---|---|---|---|---|
| TC01 | `py_compile` các file ML/B6 hiện có và file mới; `ast.parse(feature_version=(3,8))` cho file mới; `pyflakes` | Không lỗi | py_compile OK; py3.8 syntax OK cho 4 file mới; pyflakes không cảnh báo | PASS | Static (Windows) |
| TC02 | Import các module mới bằng test | Import được | Test chạy được | PASS | Unit (Windows) |
| TC03 | `validate_b6_schema` trên output B6 thật (synthetic) và trên DataFrame thiếu `n_cart` | Chấp nhận / báo lỗi rõ | Đúng cả hai | PASS | Unit (Windows) |
| TC04 | Label = 2 ở validation | ValueError "outside" | Đúng | PASS | Unit (Windows) |
| TC05 | Train có cả 0 và 1 | Có | Train: pos=16, neg=28 | PASS | Unit + e2e (Windows) |
| TC06 | Train không có positive | ValueError rõ ràng | "no positive labels" | PASS | Unit (Windows) |
| TC07 | NULL và NaN trong `avg_price` của train | ValueError | Đúng với cả hai | PASS | Unit (Windows) |
| TC08 | Preprocessing fit trên train. `remove_from_cart` chỉ xuất hiện ở validation | Vocabulary train = {cart, view} | Đúng; validation có giá trị chưa thấy vẫn transform được, số dòng giữ nguyên | PASS | Unit (Windows) |
| TC09 | Không có feature tương lai/định danh/nhãn | Danh sách feature không chứa cột cấm | Assembler inputs không chứa `user_session`, `label`, `prefix_start`, `prediction_time`. Kiểm tra theo danh sách, không chứng minh toàn bộ | PASS | Unit (Windows) |
| TC10 | `user_session` không vào feature vector | Không có | Đúng | PASS | Unit (Windows) |
| TC11 | Class weight chỉ từ train | = neg/pos của train = 28/16 = 1.75 | 1.75; tính lại độc lập từ dữ liệu bằng test | PASS | Unit (Windows) |
| TC12 | LR fit thành công | Có hệ số hữu hạn | 12 hệ số = 9 số + 3 one-hot (đo: `categorySizes=[3]`, `dropLast` không bỏ bucket `keep`) | PASS | e2e synthetic (Windows) |
| TC13 | Transform validation | Số dòng vào = ra; probability size 2; không NaN | 11 = 11; bad=0 | PASS | e2e synthetic (Windows) |
| TC14 | Test range không được đọc | Ngày test không ảnh hưởng; đếm đúng | Ngày 6 có session NULL-price. Pipeline không lỗi; `dates_not_read=1`; `train.sessions=44`, `validation.sessions=11`. Giới hạn: cutoff đọc cột `session_start_date` của toàn bộ dòng (không đọc feature/nhãn) | PASS | Unit + e2e (Windows) |
| TC15 | Repeatability cùng input | Hệ số giống nhau | Hai lần chạy khớp trong 1e-9 (`local[2]`, Windows) | PASS (Windows) | Unit (Windows). VM B: chưa |
| TC16 | VM B chạy đúng source | SHA-256 của 24 file trọng yếu trên VM khớp `source_manifest.json` tạo trên Windows | Chưa chạy trên VM. Trên Windows đã kiểm: manifest khớp khi không đổi; khi đổi một file thì `verify` trả FAIL và runner không chạy Spark (xem H10) | **BLOCKED** (chờ VM B) | Trên VM: `tc16_source.json`. Git không có trên VM nên commit không được xác minh độc lập; ghi rõ khi báo cáo |
| TC17 | Session Features B6 thật trên HDFS | Chạy được | Không truy cập HDFS | **BLOCKED** | — |
| TC18 | Log và exit code | Có | Có cho Windows synthetic (`smoke_report.json`, `exit_code: 0`). Chưa có cho VM B | PARTIAL | e2e (Windows) |

### Chi tiết lần chạy end-to-end trên Windows (synthetic, không phải dữ liệu thật)

Dữ liệu: 6 ngày, 12 phiên/ngày, 260 event. Lệnh: `run_b6_session_features.py --n 2 --sample-percent 100`, sau đó `run_b7_lr_smoke.py`.

- Writer: audit `eligible=66`, `purchased_by_n=6`, `too_short=0`, `invalid=0`; nhãn `1=24`, `0=42`.
- Smoke: `smoke_status=PASS`, `exit_code=0`, `spark=4.0.1`, `python=3.11.9`, `git_commit=ab24868…`.
- `AUC_sanity=1.0` trên synthetic. **Đây không phải chất lượng mô hình.** Mẫu synthetic có quy tắc xác định (nhãn 1 luôn có `cart` trong prefix), nên tách hoàn hảo là bình thường.

Bằng chứng: `reports/P4_B_B7_01/windows_synthetic_e2e/b6_writer_stats.json`, `reports/P4_B_B7_01/windows_synthetic_e2e/smoke_report.json`.

---

## H7. Thực nghiệm VM B

**Trạng thái: BLOCKED — chưa chạy.** Phiên này không truy cập được VM B. Không có kết quả VM nào được điền trong báo cáo.

Môi trường VM B đã xác minh (do người dùng cung cấp): Python 3.8.11, PySpark 3.1.2, Spark 3.1.2, Scala 2.12.10, Java 1.8.0_291, pytest 8.3.5, 2 CPU, RAM 4.6 GB (~1.6 GB trống khi kiểm tra), swap 7.9 GB, HDFS truy cập được, `/data/curated/events` tồn tại, `/data/curated/events_cleaned` **không** tồn tại, `/data/ml` chưa tồn tại, **không có git**.

Repo trên VM: `/media/sf_ecommerce-bigdata-capstone`. Đường dẫn `/media/sf_CapstoneRepo` đã lỗi thời và runner từ chối.

**Trước khi chạy trên VM (trên Windows):**
1. Commit hoặc chấp nhận working tree snapshot. Manifest sẽ ghi rõ trạng thái này.
2. Tạo manifest mới sau cùng: `python scripts/run/source_manifest.py make`. Nếu sửa bất kỳ file trọng yếu nào sau đó, phải tạo lại.

**Lệnh trên VM B:**

```bash
cd /media/sf_ecommerce-bigdata-capstone
bash scripts/run/run_b7_01_vm_b.sh
```

Mặc định: `N=2`, `SAMPLE_PCT=0.1` (0.1% số phiên), Curated `/data/curated/events`, không ghi đè. Không cần khai báo `REPO`. Runner xác định repo theo vị trí script.

Để chạy có chủ đích một cỡ mẫu lớn hơn (chỉ sau khi cỡ mẫu nhỏ PASS và bạn đã xem `free -m`):
```bash
SAMPLE_PCT=1 ALLOW_LARGE_SAMPLE=1 bash scripts/run/run_b7_01_vm_b.sh
```

Thứ tự gate: E1 → E2a (TC16) → E4a, E4b → E3 (preflight) → E5 (ghi mẫu B6) → E6 (smoke LR) → E7 (tiêu chí). Bước sau không chạy nếu bước trước không PASS.

**Gửi lại (dạng text) từ `reports/P4_B_B7_01/<RUN_ID>/`:**
1. `vm_b_summary.txt` (OVERALL và trạng thái từng bước)
2. `vm_b_run.log`
3. `tc16_source.json`
4. `e3_preflight.json`
5. `e4a_pytest.log`, `e4b_pytest.log`
6. `b6_writer_stats.json` (nếu E5 chạy)
7. `smoke_report.json` (nếu E6 chạy)
8. `evidence_check.json` (nếu E7 chạy)

**Điều kiện PASS (không hardcode):** `OVERALL=PASS` trong summary, và `evidence_check.json` có toàn bộ tiêu chí PASS. Trong đó `dates_not_read` được kiểm theo đồng nhất `distinct_session_dates_total = |train| + |val| + dates_not_read`, không so với số cố định. Với Curated Oct+Nov (ước lượng 61 ngày) giá trị đúng sẽ **không** phải 1 nếu cutoff 70/15 áp lên toàn bộ ngày.

---

## H8. Regression và Data Leakage

### Regression

Chạy lại toàn bộ bộ ML/B6 cùng test mới trên Windows (PySpark 4.0.1):

`tests/unit/test_b6_02.py`, `test_ml_features.py`, `test_split_metrics.py`, `test_session_dataset.py`, `test_b6_01_edges.py`, `test_b7_01_lr_smoke.py`

Kết quả: **53 passed** (exit 0, PySpark 4.0.1 / Python 3.11 trên Windows, 570 s). Gồm 31 test ML/B6 cũ và 22 test B7-01 mới.

Trước khi thay đổi, cùng bộ test trước đó chạy 31 PASS. Không sửa file B6 hiện có nên không có hồi quy về logic B6. Điều này vẫn cần kiểm chứng trên VM B (E4b).

### Leakage (theo code và test)

| Kiểm tra | Cách bảo đảm | Bằng chứng |
|---|---|---|
| Feature chỉ từ prefix N | Dùng output B6, đã lọc `event_idx <= N` | `ml/session_features.py` dòng 96–99 |
| Không dùng thông tin toàn phiên | `session_length`, `purchased_by_n`, `event_idx`, `prediction_time`, `prefix_start` không nằm trong `NUMERIC_FEATURES` / `CATEGORICAL_FEATURES` | TC09 |
| Preprocessing không fit trên validation/test | `Pipeline.fit(train_weighted)` chỉ nhận train | `lr_b6_smoke.py` |
| Class weight chỉ từ train | `compute_train_class_weight(train_stats)` | TC11 |
| Validation không dùng để chọn tham số | Không có vòng lặp chọn tham số; chỉ transform một lần | `lr_b6_smoke.py` |
| Test không được đọc | Không filter, không count, không transform ngày test. Cutoff chỉ đọc cột ngày | TC14 |
| Không ghi đè đường chính thức | `assert_smoke_output_path` từ chối `/data/ml/models`, `/data/curated`, `results/metrics`, … | Test `test_official_output_paths_are_refused` |

Giới hạn: `prefix_start`, `prediction_time` và `last_event_type` là sự kiện prefix nên không gây leak theo định nghĩa B6. `prefix_start` chỉ dùng để tính ngày cutoff, không vào feature.

---

## H9. Kết luận nghiệm thu

**P4-B-B7-01: PARTIALLY VERIFIED — AWAITING VM B REAL-DATA SMOKE TEST**

Lý do:
- Đã có: static check, unit test synthetic, Spark integration synthetic trên Windows. Tất cả PASS.
- Chưa có: smoke trên Session Features B6 thật (VM B). Đây là điều kiện bắt buộc để PASS.
- Chưa có: bằng chứng VM B chạy đúng source (TC16), dữ liệu thật (TC17), log VM B (TC18).

**Điều kiện để chuyển sang PASS:**
1. VM B chạy `scripts/run/run_b7_01_vm_b.sh` và `OVERALL=PASS` trong `vm_b_summary.txt`.
2. `smoke_report.json` từ VM B có `smoke_status=PASS`, `exit_code=0`, `dates_not_read=1`, đủ đếm train/validation.
3. Unit test trên VM B (E4a) PASS với Spark 3.1.2 / Python 3.8.11.
4. Có Session Features B6 thật được ghi bởi `run_b6_session_features.py` (hoặc bằng chứng B6 đã có trên HDFS và được xác minh).

**Chưa thuộc phạm vi B7-01 (ghi cho backlog):**
- F2: tách `train_lr.py` khỏi luồng test trước B8/B9.
- F1: thống nhất schema giữa nhánh `src/ml/` cũ và B6 trước khi chạy chính thức.
- F8, F9: chính sách right-censoring và `n_events == N` (B8/P6).
- P4-B-B7-02: kiểm thử chuyên sâu preprocessing/seed.
- P6-B-B7-01, P6-B-B7-02: temporal split và huấn luyện đầy đủ.

**Quyết định cần bạn xác nhận:**
- Có commit các file mới không? Hiện không commit vì bạn chưa yêu cầu. Nếu commit, cần thêm dòng `Co-Authored-By` theo quy ước.
- F4: đường dẫn Curated đúng là `/data/curated/events` hay `/data/curated/events_cleaned`?
- Có giữ AUC sanity trong smoke không (xem H5)?

---

## H10. Kiểm tra an toàn và gating (thực hiện trên Windows)

### H10.1 Nguồn, phạm vi ngày, cách lấy mẫu

| Hạng mục | Thực tế trong code |
|---|---|
| Nguồn đọc | `spark.read.parquet(/data/curated/events)` — **toàn bộ** Curated, chỉ đọc |
| Phạm vi ngày | **Không lọc ngày.** Lý do: Curated partition theo `event_date`; nếu lọc theo ngày, phiên bắt đầu gần ranh giới sẽ bị cắt và nhãn sai |
| Lấy mẫu | `hash(user_session) mod 10000 < threshold`. Quyết định **chỉ** dựa trên `user_session` nên mọi event của một phiên được chọn đều giữ lại; không có lọc theo hàng |
| SAMPLE_PCT | Là phần trăm **phiên**, không phải phần trăm event (`run_b6_session_features.py`, `sample_whole_sessions`) |
| Thứ tự xử lý | Lấy mẫu **trước** mọi aggregation. Window `row_number` và groupBy B6 chỉ chạy trên mẫu |
| Số lượt quét Curated | Trước: ~5 lượt (mỗi `count`/`collect` quét lại). Sau: **1 lượt** — mẫu được `persist(MEMORY_AND_DISK)` rồi mọi thao tác sau đọc từ cache nhỏ |
| `collect`/`toPandas` | Không có trên dữ liệu lớn. Chỉ `collect` kết quả nhóm nhỏ (đếm status, đếm nhãn) |
| Shuffle | Chỉ trên mẫu. `shuffle.partitions = 4` |
| Session xuyên ngày | Giữ nguyên toàn bộ vì lấy mẫu không nhìn `event_time` |
| Session bị cắt bởi **cuối dataset** | Không xử lý (F8). Đây là giới hạn của dữ liệu, không do lấy mẫu. Ghi trong `b6_writer_stats.json` ở `sample_scope` |
| Nhãn trên mẫu | Nhãn tính trên toàn bộ phiên (mọi event của phiên đã có trong mẫu), không suy ra từ mẫu event |

### H10.2 Cấu hình Spark (VM: 2 CPU, ~1.6 GB trống)

| Tham số | Giá trị | Căn cứ |
|---|---|---|
| `CAPSTONE_SPARK_MASTER` | `local[2]` | Đúng số CPU |
| `CAPSTONE_DRIVER_MEMORY` | `1g` | Lớn nhất để JVM + Python worker + OS vừa trong ~1.6 GB trống. Đặt qua `PYSPARK_SUBMIT_ARGS` và `CAPSTONE_DRIVER_MEMORY` để chắc chắn áp dụng |
| `CAPSTONE_SHUFFLE_PARTITIONS` | `4` | Dữ liệu mẫu nhỏ |
| `SAMPLE_PCT` mặc định | `0.1` | Mẫu nhỏ trước; `SAMPLE_PCT>1` bị từ chối nếu không có `ALLOW_LARGE_SAMPLE=1` ở cả shell lẫn Python |

Giới hạn: **không đo được** thời gian và RAM của lượt quét 109M dòng khi chưa chạy trên VM. Runner không có watchdog. Khuyến nghị: theo dõi `free -m` trong lần chạy đầu; nếu swap tăng mạnh, dừng và giảm cỡ mẫu.

### H10.3 Đường ghi và xóa

- Ghi: `reports/P4_B_B7_01/<RUN_ID>/` (trong repo, chỉ bằng chứng), `/data/ml/smoke/p4_b_b7_01/<RUN_ID>/` (HDFS, smoke). Spark tự tạo thư mục cha của `/data/ml/smoke/…`; đây là đường dẫn smoke, không phải `/data/ml` chính thức.
- `errorifexists` cho features; `RUN_ID` mới bắt buộc (runner dừng nếu thư mục đã tồn tại); smoke model `overwrite` chỉ trong thư mục `RUN_ID` mới.
- Không xóa gì. Không chạm Curated, không chạm `/data/ml/models`, không chạm `results/metrics` (chặn bằng `assert_smoke_output_path`).

### H10.4 Gating và trạng thái — đã kiểm trên Windows

**Test A (mismatch):** sửa tạm `tests/unit/test_b7_01_tooling.py` (thêm một dòng trống) → `verify` FAIL → runner thoát **1**, E2a = FAIL, E4a/E4b/E3/E5/E6/E7 = SKIPPED, không chạy Spark. Sau khi khôi phục byte-level, `verify` trả **0**. Bằng chứng: `reports/P4_B_B7_01/win_gate_mismatch/vm_b_summary.txt`.

**Test B và C:** xem mục H10.5 (kết quả chạy trên synthetic bằng chính runner).

### H10.5 Kết quả chạy runner trên synthetic (Windows, không phải VM B)

_Điền sau khi chạy xong — xem ghi chú bên dưới._

### H10.6 Những thứ KHÔNG được kiểm trên Windows

- Spark 3.1.2 / Python 3.8.11 / Java 8 (chỉ kiểm bằng `ast` feature_version=(3,8) và các API có từ 3.1).
- Bash trên Linux thật (chỉ kiểm `bash -n` trên Git Bash).
- HDFS (E3 thật), `hdfs dfs`, quyền ghi `/data/ml/smoke`.
- Thời gian và RAM của lượt quét full Curated.

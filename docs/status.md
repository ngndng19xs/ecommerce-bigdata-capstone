# Bảng Tiến Độ Dự Án

> Cập nhật thường xuyên bởi cả Người A và Người B.
>
> **Status:** Not Started | In Progress | Blocked | In Review | Done

---

## Shared Tasks (S)

| Task | Mô tả | Owner | Reviewer | Status | Phụ thuộc | Bằng chứng | Ghi chú |
|------|-------|-------|----------|--------|-----------|-------------|---------|
| S1 | Cài đặt và kiểm tra môi trường VM | B (lead), A | Chéo | Not Started | – | `docs/env_check.md` | |
| S2 | Khảo sát dữ liệu, hợp đồng dữ liệu | A (lead), B | Chéo | Not Started | S1 | `docs/data_contract.md` | |
| S3 | Chuyển dữ liệu Parquet A→B | A, B | Chéo | Not Started | A3 | Manifest | |
| S4 | Git Repository & quy ước mã nguồn | B | A | In Progress | – | PR vào main | |
| S5 | Kiểm thử & QA | A (lead), B | Chéo | Not Started | A3–A6, B1–B8 | `docs/issues.md` | |
| S6 | Báo cáo cuối kỳ & bảo vệ | A, B | – | Not Started | All | `reports/` | |

---

## Người A Tasks (Data & Analytics)

| Task | Mô tả | Owner | Reviewer | Status | Phụ thuộc | Bằng chứng | Ghi chú |
|------|-------|-------|----------|--------|-----------|-------------|---------|
| A1 | Tải dữ liệu REES46 | A | B | Not Started | S1 | Dữ liệu trên HDFS | |
| A2 | Khảo sát EDA (thống kê, chất lượng) | A | B | Not Started | A1 | Notebook / report | |
| A3 | ETL: Ingestion → Curated Parquet | A | B | Not Started | A2 | Code + Parquet trên HDFS | |
| A4 | Hive DDL & External Tables | A | B | Not Started | A3 | SQL files + Hive query OK | |
| A5 | Batch Analytics (funnel, session, brand) | A | B | Not Started | A4 | SQL files + kết quả | |
| A6 | Benchmark (CSV vs Parquet) | A | B | Not Started | A3 | `results/benchmark/` | |
| A7 | Data Quality checks | A | B | Not Started | A3 | Test results | |
| A8 | Quarantine invalid events | A | B | Not Started | A3 | HDFS quarantine path | |
| A9 | Metadata & manifests | A | B | Not Started | A3 | `docs/data_contract.md` | |
| A10 | Visualization batch results | A | B | Not Started | A5 | `results/figures/` | |

---

## Người B Tasks (Streaming & ML)

| Task | Mô tả | Owner | Reviewer | Status | Phụ thuộc | Bằng chứng | Ghi chú |
|------|-------|-------|----------|--------|-----------|-------------|---------|
| B1 | Kafka setup & topic creation | B | A | Not Started | S1 | Kafka topic active | |
| B2 | Kafka Producer (history replay) | B | A | Not Started | S3, B1 | Code + producer log | |
| B3 | Spark Structured Streaming consumer | B | A | Not Started | B2 | Code + streaming output | |
| B4 | Streaming aggregation (window) | B | A | Not Started | B3 | HDFS streaming results | |
| B5 | Session dataset & labeling | B | A | Not Started | S3 | Code + dataset trên HDFS | Có code trên nhánh `ml-model` |
| B6 | Feature engineering | B | A | Not Started | B5 | Code + features trên HDFS | Có code trên nhánh `ml-model` |
| B7 | ML training (Spark MLlib) | B | A | Not Started | B6 | Code + model trên HDFS | Có code trên nhánh `ml-model` |
| B8 | ML evaluation & metrics | B | A | Not Started | B7 | `results/metrics/` | Có code trên nhánh `ml-model` |
| B9 | Demo end-to-end script | B | A | Not Started | B3, B8 | `scripts/demo/` | |


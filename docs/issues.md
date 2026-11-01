# Quản Lý Lỗi và Vấn Đề Kỹ Thuật (Issue Tracking)

> **Owner:** Người A – QA (Task S5).  
> **Mục tiêu:** Ghi nhận, phân loại và theo dõi việc xử lý các lỗi phát sinh trong suốt vòng đời dự án Big Data Capstone.

---

## 1. Quy Trình Xử Lý Lỗi

1. **Phát hiện & Ghi nhận**: Bất kỳ thành viên nào phát hiện lỗi đều tạo một mục mới trong bảng bên dưới với trạng thái `Open`.
2. **Phân loại & Gán Owner**: Xác định mức độ ảnh hưởng (Severity) và phân công thành viên phụ trách sửa (Assignee).
3. **Sửa lỗi & Tạo nhánh**: Sử dụng quy ước nhánh `bugfix/<TaskID>-<mô-tả>`.
4. **Kiểm thử lại**: Người phát hiện hoặc QA kiểm tra lại kết quả trên VM thực tế.
5. **Đóng lỗi**: Cập nhật trạng thái `Resolved` hoặc `Closed` kèm mã commit/PR khắc phục.

---

## 2. Bảng Theo Dõi Lỗi (Issue Log)

| Mã lỗi | Task liên quan | Người phát hiện | Mô tả lỗi ngắn gọn | Mức độ ảnh hưởng | Người sửa (Assignee) | Trạng thái | Bằng chứng / PR khắc phục |
|--------|----------------|-----------------|--------------------|-------------------|----------------------|------------|---------------------------|
| *Chưa có lỗi nào được ghi nhận* | | | | | | | |

---

## 3. Mẫu Mô Tả Lỗi Chi Tiết (Issue Template)

Khi phát hiện lỗi phức tạp cần tài liệu hóa chi tiết, tạo mục theo mẫu sau:

```markdown
### [ISSUE-001] Mô tả ngắn gọn về lỗi

- **Mã lỗi:** ISSUE-001
- **Task liên quan:** (ví dụ: A3, B2, S2,...)
- **Người phát hiện:** Người A / Người B
- **Ngày phát hiện:** YYYY-MM-DD
- **Mức độ ảnh hưởng:** Critical / High / Medium / Low
- **Người chịu trách nhiệm sửa:** Người A / Người B
- **Môi trường phát sinh:** VM-A / VM-B / Local Windows / HDFS / Spark

#### Mô tả chi tiết:
[Mô tả hiện tượng xảy ra, sự khác biệt giữa kết quả mong đợi và kết quả thực tế]

#### Cách tái hiện lỗi:
1. Chạy lệnh: `...`
2. Đọc file tại: `...`
3. Quan sát thông báo lỗi: `...`

#### Log / Bằng chứng lỗi:
```text
[Dán stack trace hoặc log lỗi tại đây]
```

#### Phương án khắc phục & Kết quả kiểm thử lại:
- **Giải pháp:** [Tóm tắt cách sửa]
- **PR khắc phục:** [Liên kết PR hoặc commit hash]
- **Kết quả kiểm thử lại:** Đạt / Không đạt (Ghi nhận bởi QA)
```


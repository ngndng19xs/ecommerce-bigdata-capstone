# H10 – Session Distribution Summary

**Owner:** Người A  
**Receiver / Reviewer:** Người B  
**Data scope:** October–November 2019  
**Timezone:** UTC  
**Source:** `/data/curated/events`  
**Schema version:** Theo Data Contract của dự án

## 1. Tổng quan dữ liệu session

| Chỉ tiêu | Giá trị |
|---|---:|
| Tổng số session | 23,016,650 |
| Session có mua hàng | 1,402,758 |
| Session không mua hàng | 21,613,892 |
| Tỷ lệ session có mua hàng | 6.10% |
| Tỷ lệ session không mua hàng | 93.90% |
| Số event trung bình/session | 4.78 |
| Thời lượng trung bình/session (giây) | 1,100.39 |

## 2. So sánh hai nhóm session

| Chỉ tiêu | Không mua hàng | Có mua hàng |
|---|---:|---:|
| Số session | 21,613,892 | 1,402,758 |
| Thời lượng trung bình (giây) | 1,125.95 | 706.57 |
| Thời lượng trung vị (giây) | 49 | 257 |
| Thời lượng P90 (giây) | 729 | 1,206 |
| Số event trung bình/session | 4.56 | 8.19 |
| Số event trung vị/session | 2 | 5 |
| Số event P90/session | 11 | 16 |
| Số lượt view trung bình | 4.45 | 5.81 |
| Số lượt cart trung bình | 0.11 | 1.19 |
| Số lượt purchase trung bình | 0.00 | 1.18 |

**Nhận xét:** Nhóm có mua hàng có số event trung bình và trung vị cao hơn nhóm không mua hàng. Tuy nhiên, thời lượng trung bình không mua hàng cao hơn do phân phối có đuôi dài; vì vậy nên tham khảo cả median và P90, không chỉ dùng mean.

## 3. Phân phối số event/session

Tỷ lệ phần trăm được tính trong từng nhóm session.

| Số event/session | Không mua hàng | Có mua hàng |
|---|---:|---:|
| 1 event | 38.23% | 0.27% |
| 2–3 events | 27.07% | 23.73% |
| 4–5 events | 12.03% | 28.06% |
| 6–10 events | 12.61% | 27.33% |
| 11–20 events | 6.94% | 14.19% |
| Trên 20 events | 3.11% | 6.41% |
| **Tổng** | **99.99%*** | **99.99%*** |

*Chênh lệch nhỏ do làm tròn phần trăm.*

## 4. Gợi ý cho Người B khi lựa chọn ngưỡng N

Dựa trên phân phối quan sát được:

- Nhóm không mua hàng tập trung nhiều ở session ngắn: 65.30% có không quá 3 events.
- Nhóm có mua hàng tập trung nhiều hơn ở khoảng 4–10 events: 55.39%.
- P90 số event/session là 11 ở nhóm không mua hàng và 16 ở nhóm có mua hàng.

Người B có thể sử dụng các phân vị và phân phối trên để cân nhắc ngưỡng N phù hợp với thiết kế bài toán. Cần xác định rõ N là số event đầu tiên được dùng làm lịch sử quan sát của session.

**Lưu ý quan trọng:**
1. Không chọn N dựa trên nhãn mua hàng của tập test.
2. Không dùng `has_purchase`, `purchase_count`, tổng số event cuối session hoặc các thống kê chỉ biết sau khi session kết thúc làm đặc trưng dự đoán tại thời điểm quan sát.
3. Việc chọn N cần dựa trên dữ liệu huấn luyện/khảo sát được phép sử dụng và phải bảo đảm không rò rỉ thông tin tương lai.
4. Người B phản hồi lại N được chọn và lý do lựa chọn để hai thành viên thống nhất trước bước tiếp theo.

## 5. Bàn giao

- A6-01 output và A6-02 output: google docs (đã gán link sheet ở cột ghi chú)
- Source code: `session_analytics.py`, `session_comparison.py`
- Tình trạng: Hai chương trình đã chạy thành công; output đã được xác nhận trên HDFS.

**Người B xác nhận:** đã nhận dữ liệu, thống nhất phạm vi và xác nhận ngưỡng N được lựa chọn.
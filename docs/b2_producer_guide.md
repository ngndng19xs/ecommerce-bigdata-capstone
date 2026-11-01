
# P2-B-B2-01 — Event Replay Producer

## 1. Mục tiêu

Triển khai chương trình đọc mẫu CSV REES46/H02 theo
chunk, có khả năng điều chỉnh tốc độ, lọc ngày UTC,
giới hạn event và ghi log kết quả xử lý.

Đầu ra theo bảng phân công: streaming/producer.py.
Vị trí trong kiến trúc repo thực tế:
src/streaming/producer/replay.py.

## 2. Phạm vi

- Đọc từng chunk CSV, không nạp toàn bộ dataset vào RAM.
- Kiểm tra đúng 9 cột H02.
- Lọc ngày UTC có tính cả ngày đầu và ngày cuối.
- Giới hạn số sự kiện được chọn bằng --limit.
- Điều chỉnh tốc độ mục tiêu bằng --rate.
- Thống kê scanned, selected, sent_ack, send_failed,
  malformed, filtered_date và elapsed.
- Hỗ trợ dry-run mặc định.
- Có thể gửi thử vào Kafka test topic riêng.

JSON phát sinh có nhãn:
draft-b2-01-not-approved-s2

Đây không phải JSON chính thức của S2.
Không sử dụng nó để nghiệm thu B2-02.

## 3. Môi trường

Repo Windows qua VirtualBox Shared Folder:
    /media/sf_CapstoneRepo

Dữ liệu mẫu H02:
    /media/sf_CapstoneSample

Thư mục lưu log trên VM B:
    /home/student/capstone_runtime/logs/B2

Dry-run yêu cầu Python 3.6+ và thư viện chuẩn.
Kafka test yêu cầu thêm thư viện kafka-python
tương thích với môi trường Python thực tế trên VM.

## 4. Kiểm tra dữ liệu đầu vào

    ls -lh /media/sf_CapstoneSample/part-*.csv

Schema CSV gồm 9 cột:

    event_time,event_type,product_id,category_id,
    category_code,brand,price,user_id,user_session

Timestamp CSV mẫu:
    2019-10-01 00:00:00 UTC

Timestamp JSON nháp:
    2019-10-01T00:00:00Z

## 5. Dry-run 10 event

    cd /media/sf_CapstoneRepo

    python3 scripts/run/run_b2_producer.py \
      --input /media/sf_CapstoneSample \
      --limit 10 \
      --rate 50 \
      --preview 2

Mặc định KHÔNG gửi vào Kafka.

## 6. Dry-run 100 event và lưu log

    mkdir -p ~/capstone_runtime/logs/B2
    cd /media/sf_CapstoneRepo

    python3 scripts/run/run_b2_producer.py \
      --input /media/sf_CapstoneSample \
      --start-date 2019-10-01 \
      --end-date 2019-10-01 \
      --chunk-size 500 \
      --limit 100 \
      --rate 100 \
      2>&1 | tee \
      ~/capstone_runtime/logs/B2/B2-01_dryrun_100.log

Nếu 100 bản ghi được chọn đều hợp lệ,
kỳ vọng selected=100, sent_ack=0.

sent_ack=0 là đúng với dry-run.

## 7. Unit test

    cd /media/sf_CapstoneRepo

    python3 -m unittest discover \
      -s tests/unit \
      -p 'test_b2_replay.py' -v

## 8. Tham số CLI

--input
    CSV file hoặc thư mục part-*.csv.

--chunk-size
    Số dòng giữ trong RAM mỗi chunk. Mặc định 1000.

--start-date / --end-date
    Ngày UTC, YYYY-MM-DD, bao gồm hai đầu mút.

--limit
    Giới hạn số event hợp lệ sau lọc.
    Mặc định 10. Giá trị 0 là không giới hạn
    và chỉ cho phép ở chế độ dry-run.

--rate
    Tốc độ mục tiêu, event/giây. Mặc định 10.
    Tốc độ thực tế có thể thấp hơn.

--preview
    Số JSON nháp in trong dry-run. Mặc định 2.

--send-test
    Bật gửi thật vào Kafka topic thử nghiệm.

--topic
    Topic thử nghiệm phải bắt đầu bằng b2_test_.

--bootstrap-servers
    Địa chỉ broker Kafka của VM B.

## 9. Kafka test tùy chọn

CHỈ thực hiện khi:
- Đã xác nhận broker VM B hoạt động.
- Đã có kafka-python trong Python environment.
- Đã xác nhận topic test tồn tại.
- Không dùng topic replay chính thức.

Ví dụ:

    python3 scripts/run/run_b2_producer.py \
      --input /media/sf_CapstoneSample \
      --limit 10 \
      --rate 5 \
      --send-test \
      --topic b2_test_rees46 \
      --bootstrap-servers 127.0.0.1:9092

Địa chỉ broker ở trên chỉ là ví dụ.
Phải dùng đúng broker đã cấu hình trên VM B.

Khi nhận ACK, log có sent_ack và một số
partition/offset để kiểm tra kỹ thuật.

Không xem bước này là nghiệm thu B2-02.

## 10. Git và bàn giao

Branch:
    feature/P2-B-B2-01-producer

Commit code, unit test và CLI README.
Không commit CSV mẫu H02, checkpoint hoặc log lớn.

Gửi A review:
- Code đọc CSV và các tham số CLI.
- Kết quả unit test.
- Log dry-run 10/100 event.
- Giải thích schema JSON hiện chỉ là DRAFT.

B2-02 chỉ thực hiện chính thức sau khi
S2 Kafka schema được phê duyệt.

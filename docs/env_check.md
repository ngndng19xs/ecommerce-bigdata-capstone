# Kiểm Tra Môi Trường VM

> **Owner:** Người B – Task S1. Người A kiểm tra VM-A.
>
> Tất cả ô "Chưa kiểm tra" phải được cập nhật bằng giá trị thực khi kiểm tra trên VM.

---

## VM-A (Data & Analytics)

| Hạng mục | Giá trị | Trạng thái |
|----------|---------|------------|
| OS | Chưa kiểm tra | ⬜ |
| CPU | Chưa kiểm tra | ⬜ |
| RAM | Chưa kiểm tra | ⬜ |
| Disk | Chưa kiểm tra | ⬜ |
| Python version | Chưa kiểm tra | ⬜ |
| Java version | Chưa kiểm tra | ⬜ |
| Spark version | Chưa kiểm tra | ⬜ |
| Hadoop version | Chưa kiểm tra | ⬜ |
| Kafka version | Chưa kiểm tra | ⬜ |
| Hive version | Chưa kiểm tra | ⬜ |
| Impala availability | Chưa kiểm tra | ⬜ |
| HDFS status | Chưa kiểm tra | ⬜ |
| Git version | Chưa kiểm tra | ⬜ |
| Kết nối mạng | Chưa kiểm tra | ⬜ |
| Quyền tài khoản (`student`, `hadoop`, `kafka`) | Chưa kiểm tra | ⬜ |
| Spark–Kafka connector | Chưa kiểm tra | ⬜ |

---

## VM-B (Streaming & ML)

| Hạng mục | Giá trị | Trạng thái |
|----------|---------|------------|
| OS | Chưa kiểm tra | ⬜ |
| CPU | Chưa kiểm tra | ⬜ |
| RAM | Chưa kiểm tra | ⬜ |
| Disk | Chưa kiểm tra | ⬜ |
| Python version | Chưa kiểm tra | ⬜ |
| Java version | Chưa kiểm tra | ⬜ |
| Spark version | Chưa kiểm tra | ⬜ |
| Hadoop version | Chưa kiểm tra | ⬜ |
| Kafka version | Chưa kiểm tra | ⬜ |
| Hive version | Chưa kiểm tra | ⬜ |
| Impala availability | Chưa kiểm tra | ⬜ |
| HDFS status | Chưa kiểm tra | ⬜ |
| Git version | Chưa kiểm tra | ⬜ |
| Kết nối mạng | Chưa kiểm tra | ⬜ |
| Quyền tài khoản (`student`, `hadoop`, `kafka`) | Chưa kiểm tra | ⬜ |
| Spark–Kafka connector | Chưa kiểm tra | ⬜ |

---

## Lệnh Kiểm Tra Tham Khảo

```bash
# Tài nguyên hệ thống
free -h
nproc
df -h

# Java & Hadoop ecosystem
java -version
hadoop version
spark-submit --version
hive --version
kafka-topics.sh --version   # hoặc kafka-topics --version

# Python
python3 --version
pip3 list | grep -i pyspark

# HDFS
hdfs dfs -ls /
hdfs dfsadmin -report

# Kiểm tra các daemon
jps

# Git
git --version
```


# Pipeline Spark Streaming Dự Đoán Doanh Thu Salon

## 1. Giới thiệu

Dự án này triển khai pipeline Spark Structured Streaming nhằm xử lý và dự đoán doanh thu salon theo thời gian thực từ Kafka, sau đó gửi kết quả dự đoán vào topic Kafka khác. Pipeline chạy trên cụm Spark Standalone.

## 2. Môi trường & Yêu cầu hệ thống

* OS: Ubuntu/Linux
* JDK: 8+
* Python: 3.8/3.9
* Apache Spark: 3.2.1
* Apache Kafka + Zookeeper (multi-broker: kafka1, kafka2, kafka3)
* Docker & Docker Compose

## 3. Thiết lập môi trường Python và Spark

```bash
sudo apt install python3.9 python3.9-venv
python3 -m venv /home/tienlee/dataanalytis/venv
source /home/tienlee/dataanalytis/venv/bin/activate
pip install pyspark pandas confluent-kafka scikit-learn pyarrow setuptools cloudpickle==2.0.0
```

## 4. Cấu hình Spark Standalone

### 4.1 spark-env.sh

```bash
export SPARK_MASTER_HOST=192.168.162.130
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

export SPARK_WORKER_INSTANCES=3
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=2G

export SPARK_WORKER_DIR=/tmp/spark-tienlee/workdir_multis
export SPARK_LOG_DIR=/opt/spark/logs/multis_workers

export SPARK_LOCAL_IP=192.168.162.130
export PYSPARK_PYTHON=/home/tienlee/dataanalytis/venv/bin/python
export PYSPARK_DRIVER_PYTHON=/home/tienlee/dataanalytis/venv/bin/python
```

### 4.2 workers

```
192.168.162.130
```

### 4.3 Start Spark Cluster

```bash
chmod +x ./start-master-worker.sh
./start-master-worker.sh
# Hoặc:
/opt/spark/sbin/start-all.sh
```

## 5. Kafka Cluster (3 Brokers)

### 5.1 Khởi động Kafka

```bash
cd /path/to/docker-compose
docker-compose down -v
docker-compose up -d
```

### 5.2 Tạo Kafka Topics

```bash
docker exec -it dataanalytisreport-kafka1-1 kafka-topics --create --topic salon-input \
  --bootstrap-server dataanalytisreport-kafka1-1:9092,dataanalytisreport-kafka2-1:9093,dataanalytisreport-kafka3-1:9094 \
  --partitions 3 --replication-factor 3

docker exec -it dataanalytisreport-kafka1-1 kafka-topics --create --topic salon-output \
  --bootstrap-server dataanalytisreport-kafka1-1:9092,dataanalytisreport-kafka2-1:9093,dataanalytisreport-kafka3-1:9094 \
  --partitions 3 --replication-factor 3

# Kiểm tra danh sách topics

docker exec -it dataanalytisreport-kafka1-1 kafka-topics --bootstrap-server kafka1:9092,kafka2:9093,kafka3:9094 --list
```

## 6. Triển khai Pipeline

### 6.1 Chạy Kafka Producer

```bash
source /home/tienlee/dataanalytis/venv/bin/activate
python3 /home/tienlee/DataAnalytisReport/producer.py
```

### 6.2 Chạy Spark Streaming (spark-streaming-pipeline.py)

```bash
spark-submit --master spark://192.168.162.130:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
  --conf "spark.pyspark.python=/home/tienlee/dataanalytis/venv/bin/python" \
  --conf "spark.pyspark.driver.python=/home/tienlee/dataanalytis/venv/bin/python" \
  /home/tienlee/DataAnalytisReport/spark-streaming-pipeline.py
```

* Script `spark-streaming-pipeline.py` sẽ:

  * Đọc dữ liệu JSON từ topic Kafka `salon-input`
  * Parse schema và nhóm theo `Id_salon`, `create_date`
  * Áp dụng mô hình dự đoán (real/fake) qua `predict_revenue()` sử dụng Pandas UDF
  * Gửi kết quả về topic Kafka `salon-output` qua `foreachBatch()`

### 6.3 Chạy Streamlit UI (tùy chọn)

```bash
streamlit run ui.py
```

* Giao diện để xem hoặc trực quan hóa dữ liệu salon theo thời gian.

### 6.4 Kafka Consumer (console output)

```bash
docker exec -it dataanalytisreport-kafka1-1 bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic salon-output --from-beginning
```

* Bạn sẽ thấy dữ liệu JSON được in ra console:

```json
{"Id_salon": "SALON123", "date_predict": "2025-07-01", "revenue_month": 5230.5, "revenue_week": 1210.2}
```

## 7. Dọn dẹp - Stop Services

```bash
/opt/spark/sbin/stop-all.sh
kill -9 $(ps -ef | grep spark | grep -v grep | awk '{print $2}')
rm -rf /opt/spark/logs/*
```

## 8. Lưu ý

* Đảm bảo venv đã được active trước khi chạy producer.py hoặc spark-submit.
* Tất cả worker và driver phải dùng chung interpreter Python và cloudpickle.
* Dùng Web UI: [http://192.168.162.130:8080](http://192.168.162.130:8080) để theo dõi cluster.

## 9. Các bước tổng quát

1. Thiết lập venv + cài thư viện
2. Cấu hình Spark env + workers
3. Khởi động Spark cluster
4. Khởi động Kafka + topics
5. Chạy producer.py
6. spark-submit pipeline
7. consumer xem kết quả trên console
8. Giao diện UI bằng streamlit (nếu có)
9. Stop và clear log

from confluent_kafka import Producer
import json
import time
from faker import Faker
import random
from datetime import datetime, timedelta

# Khởi tạo Faker
fake = Faker()

# Khởi tạo Kafka Producer
#producer = Producer({'bootstrap.servers': 'kafka1:9092,kafka2:9093,kafka3:9094'})
producer = Producer({'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094'})

# Danh sách ID salon mẫu
salon_ids = [f"salon_{str(i).zfill(3)}" for i in range(1, 11)]  # salon_001 đến salon_010

def generate_fake_data():
    # Tạo thời gian ngẫu nhiên trong vòng 7 ngày gần đây
    random_date = datetime.now() - timedelta(days=random.randint(0, 7), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    # Tạo dữ liệu giả
    data = {
        "Id_salon": random.choice(salon_ids),
        "create_date": random_date.strftime("%Y-%m-%d %H:%M:%S"),
        "total": round(random.uniform(500000, 5000000), 2)  # Doanh thu từ 500k đến 5M VNĐ
    }
    return data

while True:
    # Tạo dữ liệu giả
    fake_data = generate_fake_data()
    # Gửi dữ liệu vào Kafka
    producer.produce('salon-input', value=json.dumps(fake_data).encode('utf-8'))
    producer.flush()
    print(f"Sent: {fake_data}")  # In dữ liệu để kiểm tra
    time.sleep(10)  # Gửi mỗi giây

import streamlit as st
from confluent_kafka import Consumer
import json
import pandas as pd

st.title("Báo cáo doanh thu salon theo thời gian thực")

# Kết nối Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': 'kafka1:9092,kafka2:9093,kafka3:9094',
    'group.id': 'ui-consumer',
    'auto.offset.reset': 'latest'
})
consumer.subscribe(['prediction-results'])

# Tạo placeholder cho dữ liệu
data = []
placeholder = st.empty()

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    # Parse dữ liệu từ Kafka
    prediction = json.loads(msg.value().decode('utf-8'))
    data.append(prediction)
    # Hiển thị bảng
    df = pd.DataFrame(data)
    with placeholder.container():
        st.write("Dữ liệu dự đoán doanh thu:")
        st.dataframe(df)
        # Vẽ biểu đồ
        st.line_chart(df[["revenue_month", "revenue_week"]])

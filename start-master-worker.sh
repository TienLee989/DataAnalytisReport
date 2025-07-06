#!/bin/bash

# Dừng các tiến trình Spark cũ (nếu có)
echo "Dừng các tiến trình Spark cũ..."
/opt/spark/sbin/stop-all.sh
sleep 5

# Xóa các file PID và working directory cũ...
echo "Xóa các file PID và working directory cũ..."
# Đảm bảo đường dẫn này đúng với SPARK_WORKER_DIR bạn đã cấu hình trong spark-env.sh
# Nếu bạn dùng /tmp/spark-tienlee/workdir_multis, hãy xóa nó
rm -rf /tmp/spark-tienlee/*
sleep 2

# Đặt biến môi trường để Spark bind vào IP đúng
export SPARK_LOCAL_IP=192.168.162.130

# Khởi chạy Master và tất cả Workers bằng start-all.sh
echo "Khởi chạy Spark Master và Workers..."
/opt/spark/sbin/start-all.sh
sleep 20 # Đợi đủ lâu để tất cả các Worker khởi động

echo "Spark Master và 3 Worker đã khởi động thành công."
echo "==========================================================================="
echo "Kiểm tra lại các tiến trình Spark đang chạy:"
ps -ef | grep spark | grep -v grep
echo "==========================================================================="

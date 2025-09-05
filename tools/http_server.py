import json
import os

from datetime import datetime
from flask import Flask, request

# Khởi tạo một ứng dụng Flask
app = Flask(__name__)

def append_to_json_log(new_data, filename: str):
    """
    Hàm này đọc file log JSON, thêm dữ liệu mới vào và ghi lại.
    """
    records = []

    # 1. Đọc dữ liệu cũ nếu file tồn tại và không rỗng
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                records = json.load(f)

            # Đảm bảo records là một list
            if not isinstance(records, list):
                records = [records]
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Nếu file bị lỗi, bắt đầu với một danh sách rỗng
            print(f"Cảnh báo: File '{filename}' bị lỗi hoặc không phải JSON. Sẽ tạo file mới.")
            records = []

    # 2. Thêm dấu thời gian và dữ liệu mới vào danh sách
    record_to_add = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data': new_data
    }
    records.append(record_to_add)

    # 3. Ghi lại toàn bộ danh sách đã cập nhật vào file
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=4)
        print(f"✅ Đã ghi bản ghi mới vào file '{filename}'")
    except Exception as e:
        print(f"❌ Lỗi khi ghi file log: {e}")


# Tạo một endpoint để nhận dữ liệu.
# Đường dẫn phải là '/data' và phương thức là 'POST' để khớp với tín hiệu của thiết bị.
@app.route('/data', methods=['POST'])
def receive_data():
    print("\n====== Đã nhận được tín hiệu! ======")

    # Lấy ngày hiện tại theo định dạng YYYY-MM-DD
    today_str = datetime.now().strftime("%Y-%m-%d")
    # Tạo tên file dựa trên ngày hiện tại
    filename = f"logs/data_log_{today_str}.json"

    # Lấy dữ liệu được gửi đến. Dữ liệu này thường ở dạng JSON hoặc form-data.
    try:
        # Thử lấy dữ liệu dạng JSON trước
        data = request.get_json()
        if not data:
            print("Dữ liệu nhận được rỗng.")
            return "No data received", 400

        print(f"Dữ liệu nhận được (sẽ lưu vào {filename}):")
        print(json.dumps(data, indent=2))

        # Gọi hàm ghi log với tên file động
        append_to_json_log(data, filename)
    except Exception as e:
        print(f"Lỗi khi xử lý request: {e}")
        return "Invalid request", 400

    return "OK", 200

if __name__ == '__main__':
    # Chạy server trên tất cả các địa chỉ IP của máy tính (0.0.0.0)
    # và ở cổng 5432 (bạn có thể đổi cổng nếu cần)
    PORT = 5432
    print(f"▶️  Server đang lắng nghe tại http://0.0.0.0:{PORT}/data")
    print("▶️  Hãy cấu hình thiết bị IoT để gửi dữ liệu tới IP máy tính này.")
    print("▶️  Dữ liệu sẽ được tự động lưu vào file theo ngày (VD: data_log_YYYY-MM-DD.json).")

    app.run(host='0.0.0.0', port=PORT)









# Dữ liệu nhận được (JSON):
# {
#   "SID": "F150E71E79C9B2A3D52D9AA035BD43AB",
#   "Num": 2,
#   "Data": [
#     {
#       "D": 450,
#       "I": 4775,
#       "O": 19488,
#       "S": 0,
#       "T": 2507221350
#     },
#     {
#       "D": 450,
#       "I": 18095,
#       "O": 9283,
#       "S": 0,
#       "T": 2507221350
#     }
#   ]
# }
# 192.168.10.250 - - [22/Jul/2025 12:54:25] "POST /data HTTP/1.1" 200 -
# 192.168.10.250 - - [22/Jul/2025 12:55:48] "POST /err HTTP/1.1" 404 -
# ====== Đã nhận được tín hiệu! ======
# Dữ liệu nhận được (JSON):
# {
#   "SID": "F150E71E79C9B2A3D52D9AA035BD43AB",
#   "Num": 5,
#   "Data": [
#     {
#       "D": 450,
#       "I": 2,
#       "O": 1,
#       "S": 0,
#       "T": 2507221354
#     },
#     {
#       "D": 450,
#       "I": 1,
#       "O": 0,
#       "S": 0,
#       "T": 2507221353
#     },
#     {
#       "D": 450,
#       "I": 0,
#       "O": 0,
#       "S": 8,
#       "T": 2507221353
#     },
#     {
#       "D": 450,
#       "I": 4776,
#       "O": 19489,
#       "S": 0,
#       "T": 2507221352
#     },
#     {
#       "D": 450,
#       "I": 18095,
#       "O": 9282,
#       "S": 0,
#       "T": 2507221352
#     }
#   ]
# }
# 192.168.10.250 - - [22/Jul/2025 12:56:26] "POST /data HTTP/1.1" 200 -
# ====== Đã nhận được tín hiệu! ======
# Dữ liệu nhận được (JSON):
# {
#   "SID": "F150E71E79C9B2A3D52D9AA035BD43AB",
#   "Num": 6,
#   "Data": [
#     {
#       "D": 450,
#       "I": 1,
#       "O": 0,
#       "S": 0,
#       "T": 2507221356
#     },
#     {
#       "D": 450,
#       "I": 0,
#       "O": 0,
#       "S": 8,
#       "T": 2507221355
#     },
#     {
#       "D": 450,
#       "I": 3,
#       "O": 0,
#       "S": 0,
#       "T": 2507221355
#     },
#     {
#       "D": 453,
#       "I": 17,
#       "O": 3,
#       "S": 0,
#       "T": 2507221354
#     },
#     {
#       "D": 450,
#       "I": 4779,
#       "O": 19490,
#       "S": 8,
#       "T": 2507221354
#     },
#     {
#       "D": 450,
#       "I": 18095,
#       "O": 9284,
#       "S": 0,
#       "T": 2507221354
#     }
#   ]
# }
# 192.168.10.250 - - [22/Jul/2025 12:58:28] "POST /data HTTP/1.1" 200 -

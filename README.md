# iCount People Analytics
Hệ thống thống kê và phân tích lưu lượng người ra vào trung tâm thương mại, được xây dựng bằng FastAPI và DuckDB.

## Hướng dẫn cài đặt và chạy dự án
### 1. Yêu cầu
- Python 3.8+
- Dữ liệu Parquet của bạn được đặt trong thư mục `data/` theo đúng cấu trúc đã mô tả.

### 2. Các bước thiết lập
#### a. Sao chép dự án
Tạo một thư mục cho dự án và sao chép tất cả các file và thư mục đã được cung cấp ở trên vào đó.

#### b. Tạo môi trường ảo (Khuyến khích)
```
python -m venv venv
source venv/bin/activate  # Trên Windows: venv\Scripts\activate
```

#### c. Cài đặt các thư viện cần thiết
```
pip install -r requirements.txt
```

#### d. Chuẩn bị dữ liệu
Đảm bảo rằng bạn có thư mục `data/` ở cùng cấp với thư mục `app/` và chứa dữ liệu Parquet của bạn theo cấu trúc:
```
iCount-People/
├── app/
├── data/
│   ├── crowd_counts/
│   │   └── year=.../*.parquet
│   └── error_logs/
│       └── year=.../*.parquet
├── .env
├── requirements.txt
└── ...
```

#### e. Chuẩn bị file tĩnh
Tạo các file `logo.png` và `favicon.ico` trong thư mục `app/static/` để logo và icon của trang web hiển thị.

### 3. Chạy ứng dụng
Sử dụng Uvicorn để khởi động server. Từ thư mục gốc của dự án (`iCount-People/`), chạy lệnh sau:
```
uvicorn app.main:app --reload
```
- `app.main:app`: Chỉ cho Uvicorn tìm đối tượng `app` trong file `app/main.py`.
- `--reload`: Tự động khởi động lại server mỗi khi có thay đổi trong code.

### 4. Truy cập ứng dụng
Sau khi server khởi động, bạn có thể:
- Truy cập Dashboard: Mở trình duyệt và đi đến http://127.0.0.1:8000
- Xem tài liệu API: Truy cập http://127.0.0.1:8000/docs để xem giao diện Swagger UI tương tác.

Chúc bạn thành công với dự án! Nếu có bất kỳ câu hỏi nào khác, đừng ngần ngại hỏi nhé.

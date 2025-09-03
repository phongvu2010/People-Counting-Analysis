# Hệ thống Phân tích Dữ liệu Đếm người (People Counting Analysis)
Đây là một ứng dụng web dashboard được xây dựng bằng Streamlit để trực quan hóa và phân tích dữ liệu lượt người ra vào các cửa hàng. Ứng dụng kết nối trực tiếp đến cơ sở dữ liệu của hệ thống máy đếm, cung cấp các báo cáo thống kê linh hoạt và một giao diện quản trị thân thiện.

## ✨ Tính năng nổi bật
* **📈 Trực quan hóa dữ liệu:** Sử dụng biểu đồ cột (Bar Chart) của Plotly để hiển thị dữ liệu một cách trực quan và dễ hiểu.
* **🔐 Xác thực người dùng:** Tích hợp hệ thống đăng nhập, đăng xuất an toàn cho người dùng (admin, user) bằng `streamlit-authenticator`.
* **⚙️ Bộ lọc đa dạng:** Cho phép lọc và phân tích dữ liệu theo nhiều chiều:
  * **Cửa hàng**
  * **Khung thời gian:** Ngày, Tuần, Tháng, Quý, Năm.
  * **Chu kỳ trong ngày:** 15 phút, 30 phút, 1 giờ.
* **📊 Báo cáo chi tiết:** Hiển thị bảng dữ liệu thống kê chi tiết với các chỉ số như Số lượng (Quantity), Tỷ lệ phần trăm (Percentage), và Tỷ lệ tương đối (Relative Ratio).
* **🛠️ Quản trị hệ thống:** Trang quản trị viên có thể xem nhật ký lỗi của các thiết bị đếm để kịp thời xử lý sự cố.
* **🚀 Tối ưu hiệu năng:** Sử dụng cơ chế cache của Streamlit để tăng tốc độ tải dữ liệu và giảm tải cho cơ sở dữ liệu.

## 🛠️ Công nghệ sử dụng
* **Backend & Frontend:** [Streamlit](http://streamlit.com/)
* **Phân tích và xử lý dữ liệu:** [Pandas](https://pandas.pydata.org/)
* **Vẽ biểu đồ:** [Plotly](https://plotly.com/python/)
* **Tương tác Cơ sở dữ liệu:** [SQLAlchemy](https://www.sqlalchemy.org/), [pyodbc](https://github.com/mkleehammer/pyodbc) (cho MS SQL Server)
* **Xác thực người dùng:** [streamlit-authenticator](https://github.com/mkhorasani/Streamlit-Authenticator)

## 🚀 Hướng dẫn cài đặt và chạy ứng dụng
### 1. Yêu cầu
* Python 3.8+
* Microsoft ODBC Driver for SQL Server

### 2. Cài đặt môi trường
Đầu tiên, sao chép (clone) repository này về máy của bạn.
```Bash
git clone https://github.com/phongvu2010/people-counting-analysis.git
cd people-counting-analysis
```

Tạo và kích hoạt môi trường ảo (virtual environment). Hướng dẫn có trong file requirements.txt. Ví dụ trên Windows:
```Bash
# Tạo môi trường ảo
python -m venv .venv

# Kích hoạt môi trường ảo
.venv\Scripts\activate.bat
```

### 3. Cài đặt các thư viện cần thiết
```Bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Cấu hình
#### a. Cấu hình kết nối Database:
Tạo một file tên là `secrets.toml` trong thư mục `.streamlit/`. Nội dung file phải tuân theo mẫu sau, thay thế bằng thông tin CSDL của bạn:
```TOML
[development]
DB_HOST="your_database_host"
DB_PORT=1433
DB_NAME="your_database_name"
DB_USER="your_username"
DB_PASS="your_password"
```

#### b. Cấu hình tài khoản người dùng:
Mở file `config.yaml` để quản lý thông tin người dùng. Mật khẩu phải được hash trước khi đưa vào file. Bạn có thể tạo mật khẩu đã hash bằng cách chạy một script riêng sử dụng `stauth.Hasher(['your_password']).generate()`.
```YAML
credentials:
  usernames:
    admin:
      email: admin@example.com
      name: Admin
      password: "$2b$12$hashed_password_for_admin"
    user:
      email: user@example.com
      name: User
      password: "$2b$12$hashed_password_for_user"
```

### 5. Chạy ứng dụng
Sử dụng lệnh của Streamlit:
```Bash
streamlit run main.py
```

Hoặc trên Windows, bạn có thể chạy file `run.bat`.

## 📁 Cấu trúc dự án
```
.
├── .streamlit/
│   ├── config.toml       # Cấu hình giao diện và server của Streamlit
│   └── secrets.toml      # (Cần tự tạo) Chứa thông tin nhạy cảm như mật khẩu CSDL
├── .gitignore            # Các file và thư mục được Git bỏ qua
├── config.yaml           # Cấu hình tài khoản người dùng cho streamlit-authenticator
├── database.py           # Module xử lý kết nối và truy vấn CSDL
├── main.py               # File chính chạy ứng dụng Streamlit
├── models.py             # Định nghĩa các model SQLAlchemy ORM
├── requirements.txt      # Danh sách các thư viện Python cần thiết
├── run.bat               # Script để chạy nhanh ứng dụng trên Windows
├── style.css             # File CSS tùy chỉnh giao diện
└── logo.png              # Logo của ứng dụng
```

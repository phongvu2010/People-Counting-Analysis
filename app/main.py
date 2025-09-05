"""
Điểm khởi đầu (Entrypoint) cho ứng dụng web FastAPI.

Tệp này chịu trách nhiệm:
- Khởi tạo đối tượng ứng dụng FastAPI.
- Cấu hình Middleware (ví dụ: CORS để cho phép frontend giao tiếp).
- Tích hợp các routers từ các module khác vào ứng dụng chính.
- Phục vụ các tệp tĩnh (CSS, JS) và template HTML cho giao diện.
"""
 
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .core.config import settings
from .routers import router as api_router

# --- 1. Khởi tạo ứng dụng FastAPI ---
# Lấy các thông tin cơ bản từ tệp cấu hình để khởi tạo.
api_app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version="2.1.0",
)

# --- 2. Cấu hình Middleware ---
# Cấu hình CORS (Cross-Origin Resource Sharing) để cho phép trình duyệt
# ở các domain khác (ví dụ: http://localhost:3000) có thể gọi đến API này.
if settings.BACKEND_CORS_ORIGINS:
    origins = [str(origin).rstrip("/") for origin in settings.BACKEND_CORS_ORIGINS]
    api_app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],  # Cho phép tất cả các phương thức (GET, POST, etc.)
        allow_headers=["*"],  # Cho phép tất cả các header.
    )

# --- 3. Tích hợp Routers ---
# "Gắn" tất cả các endpoint được định nghĩa trong `app/routers.py` vào ứng dụng chính.
api_app.include_router(api_router)

# --- 4. Cấu hình Template và Tệp tĩnh ---
templates = Jinja2Templates(directory="template")
# Mount thư mục `statics` để phục vụ các tệp tĩnh (CSS, JS, images).
# URL '/static' sẽ trỏ đến thư mục 'template/statics' trên server.
api_app.mount(
    "/static", StaticFiles(directory="template/statics"), name="static"
)


# --- 5. Định nghĩa các Endpoint gốc ---
@api_app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def show_dashboard(request: Request):
    """
    Endpoint gốc (`/`), phục vụ trang dashboard HTML cho người dùng.

    `include_in_schema=False` để không hiển thị endpoint này trong tài liệu API
    tự động (Swagger UI) vì nó chỉ dành cho giao diện người dùng.
    """
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "project_name": settings.PROJECT_NAME,
            "description": settings.DESCRIPTION,
        },
    )


@api_app.get("/health", tags=["Health Check"])
def health_check():
    """
    Endpoint để kiểm tra "sức khỏe" (health status) của ứng dụng.

    Thường được sử dụng bởi các hệ thống monitoring để kiểm tra xem
    dịch vụ có đang hoạt động hay không.
    """
    return {"status": "ok"}

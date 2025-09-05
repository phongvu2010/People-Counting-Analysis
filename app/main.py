# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
"""
Điểm khởi đầu (entry point) của ứng dụng FastAPI.

Tệp này chịu trách nhiệm khởi tạo ứng dụng, cấu hình middleware (CORS),
đăng ký các router, và định nghĩa các endpoint cấp cao nhất như health check
và trang chủ.
"""
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .core.config import settings
from .routers import router as api_router
from .utils.logger import setup_logging

# Khởi tạo ứng dụng FastAPI.
app = FastAPI(
    title = settings.PROJECT_NAME,
    description = settings.DESCRIPTION,
    version = '1.0.0'
)

# Cấu hình CORS (Cross-Origin Resource Sharing) Middleware.
# Cho phép frontend từ các domain khác có thể gọi API này.
if settings.BACKEND_CORS_ORIGINS:
    origins = [str(origin) for origin in settings.BACKEND_CORS_ORIGINS]
    app.add_middleware(
        CORSMiddleware,
        allow_origins = origins,    # Cho phép các origin trong danh sách
        allow_credentials = True,   # Cho phép gửi cookie
        allow_methods = ['*'],      # Cho phép tất cả các phương thức (GET, POST, etc.)
        allow_headers = ['*']       # Cho phép tất cả các header
    )

# Mount thư mục `static` để phục vụ các tệp tĩnh (CSS, JS, Images).
app.mount('/static', StaticFiles(directory='app/static'), name='static')

# Cấu hình Jinja2 để render các template HTML.
templates = Jinja2Templates(directory='app/templates')

# --- API Routers ---
# Bao gồm các router từ các module khác với một tiền tố chung.
app.include_router(
    api_router,
    prefix = '/api/v1',     # Tiền tố cho tất cả các route trong router này
    tags = ['Dashboard']    # Gắn tag để nhóm các API trong giao diện Swagger
)

# --- Application Events ---
@app.on_event('startup')
async def startup_event():
    """Thiết lập logging khi ứng dụng khởi động."""
    setup_logging('FastAPI')
    logging.info('Application startup complete.')

@app.on_event('shutdown')
async def shutdown_event():
    """Ghi log khi ứng dụng tắt."""
    logging.info('Application shutdown.')

# --- Top-level Endpoints ---
@app.get('/health', tags=['Health Check'])
def health_check():
    """Endpoint để kiểm tra tình trạng hoạt động của ứng dụng."""
    return {'status': 'ok'}

@app.get('/', response_class=HTMLResponse, include_in_schema=False)
async def read_root(request: Request):
    """Phục vụ trang dashboard chính (dashboard.html)."""
    return templates.TemplateResponse(
        'dashboard.html',
        {
            'request': request,
            'project_name': settings.PROJECT_NAME,
            'description': settings.DESCRIPTION
        }
    )

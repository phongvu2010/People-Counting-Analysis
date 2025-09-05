"""
Định nghĩa các Pydantic model (schema) để xác thực dữ liệu API.

Các model này đóng vai trò là "hợp đồng dữ liệu" (data contract), đảm bảo
dữ liệu được gửi và nhận qua API luôn tuân thủ một cấu trúc nhất quán. FastAPI
sử dụng chúng để tự động xác thực request, tuần tự hóa response và tạo tài
liệu API (Swagger/OpenAPI).
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class Metric(BaseModel):
    """
    Cấu trúc cho các thẻ chỉ số (KPIs) chính trên dashboard.
    """
    total_in: int
    average_in: float
    peak_time: Optional[str] = None
    current_occupancy: int
    busiest_store: Optional[str] = None
    growth: float


class ChartDataPoint(BaseModel):
    """
    Một điểm dữ liệu duy nhất trên biểu đồ.
    """
    # Trục hoành: có thể là ngày, giờ (str), hoặc tên cửa hàng (str).
    x: Any
    # Trục tung: giá trị số (ví dụ: lượt khách).
    y: int


class ChartData(BaseModel):
    """
    Dữ liệu hoàn chỉnh cho một biểu đồ (ví dụ: biểu đồ xu hướng).
    """
    series: List[ChartDataPoint]


class SummaryTableRow(BaseModel):
    """
    Cấu trúc cho một hàng trong bảng dữ liệu chi tiết.
    """
    period: str
    total_in: int
    pct_change: float
    proportion_pct: float
    proportion_change: float


class TableData(BaseModel):
    """
    Dữ liệu hoàn chỉnh cho bảng chi tiết, bao gồm cả phần tóm tắt.
    """
    data: List[SummaryTableRow]
    summary: Dict[str, Any]


class ErrorLog(BaseModel):
    """
    Cấu trúc cho một bản ghi log lỗi từ thiết bị.
    """
    id: int
    store_name: str
    log_time: datetime
    error_code: int
    error_message: str


class DashboardData(BaseModel):
    """
    Model tổng hợp, định nghĩa cấu trúc response cuối cùng cho API dashboard.

    Đây là đối tượng dữ liệu chính mà frontend sẽ nhận và sử dụng để hiển thị
    toàn bộ thông tin trên trang.
    """
    metrics: Metric
    trend_chart: ChartData
    store_comparison_chart: ChartData
    table_data: TableData
    error_logs: List[ErrorLog]
    latest_record_time: Optional[datetime] = None

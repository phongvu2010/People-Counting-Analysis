from datetime import datetime
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Union

class Metric(BaseModel):
    """Định nghĩa cấu trúc cho các thẻ chỉ số (KPIs) chính trên dashboard."""
    total_in: int
    average_in: float
    peak_time: Optional[str]
    current_occupancy: int
    busiest_store: Optional[str]
    growth: float

class ChartDataPoint(BaseModel):
    """Định nghĩa một điểm dữ liệu duy nhất trên biểu đồ (ví dụ: một cột, một điểm)."""
    # x: Any  # Trục hoành, có thể là ngày, giờ, hoặc tên cửa hàng
    x: Union[str, datetime, int]  # Trục hoành, có thể là ngày, giờ, hoặc tên cửa hàng
    y: int  # Trục tung, thường là giá trị số (ví dụ: lượt khách)

class ChartData(BaseModel):
    """Định nghĩa dữ liệu cho một biểu đồ hoàn chỉnh."""
    series: List[ChartDataPoint]

class SummaryTableRow(BaseModel):
    """Định nghĩa cấu trúc cho một hàng trong bảng dữ liệu chi tiết."""
    period: str
    total_in: int
    pct_change: float

class PaginatedTable(BaseModel):
    """Định nghĩa cấu trúc cho toàn bộ bảng dữ liệu có phân trang."""
    total_records: int
    page: int
    page_size: int
    data: List[SummaryTableRow]
    summary: Dict[str, Any]

class ErrorLog(BaseModel):
    """Định nghĩa cấu trúc cho một bản ghi log lỗi."""
    id: int
    store_name: str
    log_time: datetime
    error_code: int
    error_message: str

class DashboardData(BaseModel):
    """Model tổng hợp, định nghĩa cấu trúc response cuối cùng cho API dashboard.
    
    Đây là đối tượng dữ liệu chính mà frontend sẽ nhận và sử dụng để hiển thị
    toàn bộ thông tin trên trang.
    """
    metrics: Metric
    trend_chart: ChartData
    store_comparison_chart: ChartData
    table_data: PaginatedTable
    error_logs: List[ErrorLog]
    latest_record_time: Optional[datetime] = None




from datetime import datetime
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Union

class Metric(BaseModel):
    """Định nghĩa cấu trúc cho các thẻ chỉ số (KPIs) chính trên dashboard."""
    total_in: int
    average_in: float
    peak_time: Optional[str]
    current_occupancy: int
    busiest_store: Optional[str]
    growth: float

class ChartDataPoint(BaseModel):
    """Định nghĩa một điểm dữ liệu duy nhất trên biểu đồ (ví dụ: một cột, một điểm)."""
    x: Union[str, datetime, int]  # Trục hoành, có thể là ngày, giờ, hoặc tên cửa hàng
    y: int  # Trục tung, thường là giá trị số (ví dụ: lượt khách)

class ChartData(BaseModel):
    """Định nghĩa dữ liệu cho một biểu đồ hoàn chỉnh."""
    series: List[ChartDataPoint]

class SummaryTableRow(BaseModel):
    """Định nghĩa cấu trúc cho một hàng trong bảng dữ liệu chi tiết."""
    period: str
    total_in: int
    pct_change: float

class PaginatedTable(BaseModel):
    """Định nghĩa cấu trúc cho toàn bộ bảng dữ liệu có phân trang."""
    total_records: int
    page: int
    page_size: int
    data: List[SummaryTableRow]
    summary: Dict[str, Any]

class ErrorLog(BaseModel):
    """Định nghĩa cấu trúc cho một bản ghi log lỗi."""
    id: int
    store_name: str
    log_time: datetime
    error_code: int
    error_message: str

class DashboardData(BaseModel):
    """Model tổng hợp, định nghĩa cấu trúc response cuối cùng cho API dashboard.
    
    Đây là đối tượng dữ liệu chính mà frontend sẽ nhận và sử dụng để hiển thị
    toàn bộ thông tin trên trang.
    """
    metrics: Metric
    trend_chart: ChartData
    store_comparison_chart: ChartData
    table_data: PaginatedTable
    error_logs: List[ErrorLog]
    latest_record_time: Optional[datetime] = None

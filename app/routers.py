"""
Module định nghĩa các API endpoints cho ứng dụng.

Sử dụng `APIRouter` của FastAPI để nhóm các endpoint liên quan đến dashboard,
giúp mã nguồn có tổ chức và dễ dàng mở rộng.
"""

import asyncio
import logging
from datetime import date
from typing import Annotated, List

from fastapi import (APIRouter, Depends, Header, HTTPException, Query, Response,
                     status)

from . import schemas
from .core.caching import clear_service_cache
from .core.config import settings
from .services import DashboardService

logger = logging.getLogger(__name__)

# Khởi tạo router với tiền tố và tag chung để nhóm các API liên quan.
router = APIRouter(prefix="/api/v1", tags=["Dashboard"])


def get_dashboard_service(
    period: str = Query("day", description="Khoảng thời gian: `day`, `week`, `month`, `year`"),
    start_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    store: str = Query("all", description="Lọc theo cửa hàng hoặc `all` cho tất cả"),
) -> DashboardService:
    """
    Dependency để khởi tạo và cung cấp `DashboardService` cho mỗi request.

    Hàm này nhận các tham số query từ request, khởi tạo một instance của
    `DashboardService` và inject nó vào các endpoint cần thiết. Đây là một
    pattern mạnh mẽ của FastAPI giúp tái sử dụng logic và tách biệt các mối quan tâm.
    """
    return DashboardService(period, start_date, end_date, store)


@router.get("/dashboard", response_model=schemas.DashboardData)
async def get_dashboard_data(
    service: Annotated[DashboardService, Depends(get_dashboard_service)]
):
    """
    Cung cấp toàn bộ dữ liệu cần thiết cho trang dashboard.

    Endpoint này tổng hợp dữ liệu từ nhiều phương thức của service bằng cách
    thực thi các tác vụ I/O (truy vấn database) một cách song song, giúp
    tối ưu hóa đáng kể thời gian phản hồi.
    """
    # Thực thi các tác vụ bất đồng bộ (I/O-bound) cùng lúc với asyncio.gather
    # để giảm thời gian chờ đợi tổng thể.
    (
        metrics_data,
        trend_data,
        store_comparison_data,
        table_data,
    ) = await asyncio.gather(
        service.get_metrics(),
        service.get_trend_chart_data(),
        service.get_store_comparison_chart_data(),
        service.get_table_details(),
    )

    # Các hàm static (đồng bộ) có thể được gọi tuần tự vì chúng nhanh.
    error_logs = DashboardService.get_error_logs()
    latest_time = DashboardService.get_latest_record_time()

    # Xây dựng đối tượng response hoàn chỉnh theo schema đã định nghĩa.
    return schemas.DashboardData(
        metrics=schemas.Metric(**metrics_data),
        trend_chart=schemas.ChartData(series=trend_data),
        store_comparison_chart=schemas.ChartData(series=store_comparison_data),
        table_data=schemas.TableData(**table_data),
        error_logs=error_logs,
        latest_record_time=latest_time,
    )


@router.get("/stores", response_model=List[str])
def get_stores():
    """
    Lấy danh sách duy nhất tất cả các tên cửa hàng.

    Dữ liệu này được dùng để khởi tạo bộ lọc (filter) trên giao diện người dùng.
    """
    return DashboardService.get_all_stores()


@router.post(
    "/admin/clear-cache",
    tags=["Admin"],
    summary="Xóa cache của ứng dụng",
    status_code=status.HTTP_204_NO_CONTENT,
)
def clear_cache(x_internal_token: Annotated[str, Header()]):
    """
    Endpoint nội bộ để xóa toàn bộ cache của service.

    Yêu cầu một token xác thực trong header `X-Internal-Token` để thực hiện.
    Hữu ích sau khi chạy ETL để đảm bảo dashboard hiển thị dữ liệu mới nhất.
    """
    if x_internal_token != settings.INTERNAL_API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Token không hợp lệ."
        )
    clear_service_cache()
    return Response(status_code=status.HTTP_204_NO_CONTENT)

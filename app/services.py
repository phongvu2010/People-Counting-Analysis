"""
Module chứa lớp Service chịu trách nhiệm xử lý logic nghiệp vụ.

Lớp `DashboardService` đóng gói tất cả các phương thức cần thiết để truy vấn,
tính toán và định dạng dữ liệu cho dashboard từ kho dữ liệu DuckDB. Toàn bộ
logic truy vấn phức tạp như xử lý outlier hay điều chỉnh "ngày làm việc" đã
được chuyển vào VIEW `v_traffic_normalized` trong DuckDB, giúp cho service
này trở nên tinh gọn và chỉ tập trung vào việc tổng hợp dữ liệu.
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dateutil.relativedelta import relativedelta

from .core.caching import async_cache
from .core.config import settings
from .dependencies import query_db_to_df

logger = logging.getLogger(__name__)


class DashboardService:
    """
    Lớp chứa logic nghiệp vụ để truy vấn và tính toán dữ liệu cho dashboard.

    Mỗi instance của lớp này tương ứng với một bộ lọc (thời gian, cửa hàng)
    cụ thể từ người dùng, đóng vai trò là context cho tất cả các truy vấn.
    """

    def __init__(
        self, period: str, start_date: date, end_date: date, store: str = "all"
    ):
        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.store = store

    def _get_date_range_params(
        self, start_date: date, end_date: date
    ) -> Tuple[str, str]:
        """
        Tạo chuỗi thời gian cho query dựa trên định nghĩa "ngày làm việc".

        Giờ làm việc có thể kéo dài qua nửa đêm (ví dụ: 9h sáng đến 2h sáng
        hôm sau). Hàm này điều chỉnh ngày bắt đầu và kết thúc để bao trọn
        khung giờ này khi truy vấn.
        """
        start_dt = datetime.combine(
            start_date, datetime.min.time()
        ) + timedelta(hours=settings.WORKING_HOUR_START)

        end_dt = datetime.combine(
            end_date, datetime.min.time()
        ) + timedelta(days=1, hours=settings.WORKING_HOUR_END)

        return (
            start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        )

    def _get_base_filters(self) -> Tuple[str, list]:
        """
        Tạo mệnh đề WHERE và các tham số tương ứng cho các truy vấn.

        Hàm helper này giúp tái sử dụng logic lọc dữ liệu theo khoảng thời gian
        và cửa hàng, tránh lặp lại code.
        """
        start_str, end_str = self._get_date_range_params(
            self.start_date, self.end_date
        )
        params = [start_str, end_str]

        filter_clauses = "WHERE record_time >= ? AND record_time < ?"

        if self.store != "all":
            filter_clauses += " AND store_name = ?"
            params.append(self.store)

        return filter_clauses, params

    @async_cache
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Tính toán các chỉ số chính (KPIs) cho dashboard.

        Bao gồm tổng lượt vào, trung bình, giờ cao điểm, lượng khách ước tính
        hiện tại, cửa hàng đông nhất và tỷ lệ tăng trưởng so với kỳ trước.
        Sử dụng `asyncio.gather` để chạy các truy vấn song song.
        """
        filter_clauses, params = self._get_base_filters()

        time_unit_map = {
            "year": "month",
            "month": "day",
            "week": "day",
            "day": "hour",
        }
        peak_time_format_map = {
            "day": "%H:%M",
            "week": "%d/%m",
            "month": "%d/%m",
            "year": "Tháng %m",
        }
        time_unit = time_unit_map.get(self.period, "day")
        peak_time_format = peak_time_format_map.get(self.period, "%d/%m")

        # Truy vấn chính để lấy hầu hết các metrics trong một lần.
        query = f"""
        WITH filtered_data AS (
            SELECT * FROM v_traffic_normalized
            {filter_clauses}
        ),
        period_summary AS (
            SELECT
                date_trunc('{time_unit}', adjusted_time) as period,
                SUM(in_count) as total_in_per_period
            FROM filtered_data
            GROUP BY period
        )
        SELECT
            (SELECT SUM(in_count) FROM filtered_data) as total_in,
            (SELECT AVG(total_in_per_period) FROM period_summary) as average_in,
            (
                SELECT strftime(period + INTERVAL '{settings.WORKING_HOUR_START} hours', '{peak_time_format}')
                FROM period_summary ORDER BY total_in_per_period DESC LIMIT 1
            ) as peak_time,
            (SELECT SUM(in_count) - SUM(out_count) FROM filtered_data) as current_occupancy,
            (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
        """
        df_task = asyncio.to_thread(query_db_to_df, query, params=params)
        prev_total_task = self._get_previous_period_total_in()

        df, prev_total = await asyncio.gather(df_task, prev_total_task)

        if df.empty or pd.isna(df["total_in"].iloc[0]):
            return {
                "total_in": 0, "average_in": 0, "peak_time": "--:--",
                "current_occupancy": 0, "busiest_store": "N/A", "growth": 0.0,
            }

        data = df.iloc[0].to_dict()
        current_total = data.get("total_in") or 0

        # Tính toán tăng trưởng
        if prev_total > 0:
            growth = round(((current_total - prev_total) / prev_total) * 100, 1)
        elif current_total > 0:
            growth = 100.0  # Từ 0 lên > 0, coi như tăng 100%
        else:
            growth = 0.0

        # Làm sạch các giá trị trả về
        avg_val = data.get("average_in")
        data["average_in"] = 0 if pd.isna(avg_val) else int(round(avg_val))
        data["growth"] = growth
        if data.get("busiest_store"):
            data["busiest_store"] = data["busiest_store"].split(" (")[0]

        return data

    async def _get_previous_period_total_in(self) -> int:
        """
        Tính tổng lượt khách của kỳ liền trước để so sánh tăng trưởng.
        """
        period_map = {
            "day": {"days": 1}, "week": {"weeks": 1},
            "month": {"months": 1}, "year": {"years": 1},
        }
        delta = period_map.get(self.period)
        if not delta:
            return 0

        prev_start_date = self.start_date - relativedelta(**delta)
        prev_end_date = self.end_date - relativedelta(**delta)
        
        start_str, end_str = self._get_date_range_params(
            prev_start_date, prev_end_date
        )
        params = [start_str, end_str]
        filter_clauses = "WHERE record_time >= ? AND record_time < ?"
        if self.store != "all":
            filter_clauses += " AND store_name = ?"
            params.append(self.store)

        query = f"SELECT SUM(in_count) as total FROM v_traffic_normalized {filter_clauses}"
        df = await asyncio.to_thread(query_db_to_df, query, params=params)

        return 0 if df.empty or pd.isna(df["total"].iloc[0]) else int(df["total"].iloc[0])

    @staticmethod
    def get_all_stores() -> List[str]:
        """Lấy danh sách duy nhất tất cả các cửa hàng (static method)."""
        df = query_db_to_df("SELECT DISTINCT store_name FROM dim_stores ORDER BY store_name")
        return [] if df.empty else df["store_name"].tolist()

    @async_cache
    async def get_trend_chart_data(self) -> List[Dict[str, Any]]:
        """Lấy dữ liệu chuỗi thời gian cho biểu đồ xu hướng."""
        filter_clauses, params = self._get_base_filters()
        time_unit = {"year": "month", "month": "day", "week": "day", "day": "hour"}.get(self.period, "day")

        query = f"""
        SELECT
            (date_trunc('{time_unit}', adjusted_time) + INTERVAL '{settings.WORKING_HOUR_START} hours') as x,
            SUM(in_count) as y
        FROM v_traffic_normalized
        {filter_clauses}
        GROUP BY x ORDER BY x
        """
        df = await asyncio.to_thread(query_db_to_df, query, params=params)
        
        # Định dạng lại trục X cho dễ đọc trên biểu đồ
        if time_unit == "month":
            df["x"] = pd.to_datetime(df["x"]).dt.strftime("%Y-%m")
        elif time_unit == "day":
            df["x"] = pd.to_datetime(df["x"]).dt.strftime("%Y-%m-%d")
        else:  # hour
            df["x"] = pd.to_datetime(df["x"]).dt.strftime("%Y-%m-%d %H:00")

        return df.to_dict(orient="records")

    @async_cache
    async def get_store_comparison_chart_data(self) -> List[Dict[str, Any]]:
        """Lấy dữ liệu phân bổ lượt khách theo từng cửa hàng."""
        filter_clauses, params = self._get_base_filters()
        query = f"""
            SELECT store_name as x, SUM(in_count) as y
            FROM v_traffic_normalized
            {filter_clauses}
            GROUP BY x ORDER BY y DESC
        """
        df = await asyncio.to_thread(query_db_to_df, query, params=params)
        return df.to_dict(orient="records")

    @async_cache
    async def get_table_details(self) -> Dict[str, Any]:
        """Lấy dữ liệu chi tiết cho bảng, giới hạn 31 dòng gần nhất."""
        filter_clauses, params = self._get_base_filters()
        time_unit = {"year": "month", "month": "day", "week": "day", "day": "hour"}.get(self.period, "day")
        date_format = {"hour": "%Y-%m-%d %H:00", "day": "%Y-%m-%d", "month": "%Y-%m"}.get(time_unit, "%Y-%m-%d")

        query = f"""
        WITH filtered_data AS (
            SELECT * FROM v_traffic_normalized {filter_clauses}
        ),
        aggregated AS (
            SELECT
                date_trunc('{time_unit}', adjusted_time) as period_start,
                SUM(in_count) as total_in
            FROM filtered_data GROUP BY period_start
        ),
        with_lag AS (
            -- Sử dụng hàm cửa sổ LAG để lấy giá trị của kỳ trước đó
            SELECT *, LAG(total_in, 1, 0) OVER (ORDER BY period_start) as previous_in
            FROM aggregated
        )
        SELECT
            strftime(period_start + INTERVAL '{settings.WORKING_HOUR_START} hours', '{date_format}') as period,
            total_in,
            CASE
                WHEN previous_in = 0 THEN 0.0
                ELSE ROUND(((total_in - previous_in) * 100.0) / previous_in, 1)
            END as pct_change
        FROM with_lag
        ORDER BY period_start DESC
        LIMIT 31
        """
        df = await asyncio.to_thread(query_db_to_df, query, params=params)

        if df.empty:
            return {"data": [], "summary": {"total_sum": 0, "average_in": 0}}

        total_sum = int(df["total_in"].sum())
        df["proportion_pct"] = (df["total_in"] / total_sum * 100) if total_sum > 0 else 0.0
        df["proportion_change"] = df["proportion_pct"].diff(periods=-1).fillna(0)

        summary = {"total_sum": total_sum, "average_in": df["total_in"].mean()}

        return {"data": df.to_dict(orient="records"), "summary": summary}

    @staticmethod
    def get_latest_record_time() -> Optional[datetime]:
        """Lấy thời gian của bản ghi gần nhất trong toàn bộ dữ liệu."""
        df = query_db_to_df("SELECT MAX(recorded_at) as latest_time FROM fact_traffic")
        if df.empty or pd.isna(df["latest_time"].iloc[0]):
            return None
        return df["latest_time"].iloc[0]

    @staticmethod
    def get_error_logs(limit: int = 100) -> List[Dict[str, Any]]:
        """Lấy các log lỗi gần nhất từ bảng `fact_errors`."""
        query = """
        SELECT
            a.log_id as id,
            b.store_name,
            a.logged_at as log_time,
            a.error_code,
            a.error_message
        FROM fact_errors AS a
        LEFT JOIN dim_stores AS b ON a.store_id = b.store_id
        ORDER BY a.logged_at DESC
        LIMIT ?
        """
        df = query_db_to_df(query, params=[limit])
        return df.to_dict(orient="records")

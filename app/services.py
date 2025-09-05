import asyncio
import pandas as pd

from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Optional, Tuple

from .core.caching import async_cache # Sẽ cần cập nhật caching.py để sử dụng settings.CACHE_TTL_SECONDS
from .core.config import settings
# from .core.data_handler import query_parquet_as_dataframe
from .core.data_handler import query_dataframe # Đã đổi tên từ query_parquet_as_dataframe

class DashboardService:
    """Lớp chứa logic nghiệp vụ để xử lý và truy vấn dữ liệu cho dashboard.

    Mỗi instance của lớp này tương ứng với một bộ lọc (thời gian, cửa hàng)
    cụ thể từ người dùng, đóng vai trò là context cho các truy vấn dữ liệu.
    """
    def __init__(self, period: str, start_date: date, end_date: date, store: str = 'all'):
        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.store = store

    def _get_date_range_params(self, start_date: date, end_date: date) -> Tuple[str, str]:
        """Tạo chuỗi thời gian query dựa trên "ngày làm việc" đã định nghĩa.

        Giờ làm việc có thể kéo dài qua nửa đêm (ví dụ: 9h sáng đến 2h sáng hôm sau).
        Hàm này điều chỉnh ngày bắt đầu và kết thúc để bao trọn khung giờ này.
        """
        start_dt = datetime.combine(start_date, datetime.min.time()) + timedelta(hours=settings.WORKING_HOUR_START)
        end_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1, hours=settings.WORKING_HOUR_END)
        return start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')

    def _build_base_query(self, start_date_str: str, end_date_str: str) -> Tuple[str, list]:
        """Xây dựng câu truy vấn CTE (Common Table Expression) cơ sở và tham số.

        Hàm này tạo ra một CTE chuẩn hóa dữ liệu nguồn, bao gồm:
        - Lọc dữ liệu theo khoảng thời gian và cửa hàng.
        - Xử lý các giá trị ngoại lệ (outliers) theo cấu hình.
        - Điều chỉnh timestamp để phân tích theo "ngày làm việc".
        CTE này được tái sử dụng trong nhiều phương thức khác để tránh lặp code.
        """
        params = [start_date_str, end_date_str]

        store_filter_clause = ''
        if self.store != 'all':
            store_filter_clause = 'AND store_name = ?'
            params.append(self.store)

        # Quyết định nguồn dữ liệu dựa trên cấu hình
        source_table_or_path = ''
        if settings.DATABASE_TYPE == 'duckdb_file':
            source_table_or_path = 'crowd_counts' # Tên bảng trong file DuckDB
        else: # Mặc định hoặc 'parquet_folder'
            source_table_or_path = f"read_parquet('{settings.CROWD_COUNTS_PATH}')" # Đường dẫn Parquet

        # Xử lý outlier: thay thế các giá trị quá lớn bằng một tỷ lệ nhỏ hoặc giá trị cố định.
        if settings.OUTLIER_SCALE_RATIO > 0:
            then_logic_in = f'CAST(ROUND(in_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)'
            then_logic_out = f'CAST(ROUND(out_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)'
        else:
            # Nếu không muốn scale, có thể thay bằng một giá trị mặc định, ví dụ là 1.
            then_logic_in, then_logic_out = '1', '1'

        base_cte = f"""
        WITH source_data AS (
            SELECT
                CAST(record_time AS TIMESTAMP) as record_time,
                store_name,
                CASE WHEN in_count > {settings.OUTLIER_THRESHOLD} THEN {then_logic_in} ELSE in_count END as in_count,
                CASE WHEN out_count > {settings.OUTLIER_THRESHOLD} THEN {then_logic_out} ELSE out_count END as out_count
            # FROM read_parquet('{settings.CROWD_COUNTS_PATH}')
            FROM {source_table_or_path}
            WHERE record_time >= ? AND record_time < ?
            {store_filter_clause}
        ),
        filtered_data AS (
            SELECT *, (record_time - INTERVAL '{settings.WORKING_HOUR_START} hours') AS adjusted_time
            FROM source_data
        )
        """
        return base_cte, params

    @async_cache
    async def get_metrics(self) -> Dict[str, Any]:
        """Lấy các chỉ số chính (KPIs) cho dashboard.

        Bao gồm tổng lượt vào, trung bình, giờ cao điểm, lượng khách hiện tại,
        cửa hàng đông nhất và tỷ lệ tăng trưởng so với kỳ trước.
        Xử lý các trường hợp không có dữ liệu để tránh lỗi.
        """
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)

        peak_time_format = {'day': '%H:%M', 'week': '%d/%m', 'month': '%d/%m', 'year': 'Tháng %m'}.get(self.period, '%d/%m')
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

        query = f"""
        {base_cte}
        , period_summary AS (
            SELECT SUM(in_count) as total_in_per_period FROM filtered_data
            GROUP BY date_trunc('{time_unit}', adjusted_time)
        )
        SELECT
            (SELECT SUM(in_count) FROM filtered_data) as total_in,
            (SELECT AVG(total_in_per_period) FROM period_summary) as average_in,
            (SELECT strftime(arg_max(record_time, in_count), '{peak_time_format}') FROM filtered_data) as peak_time,
            (SELECT SUM(in_count) - SUM(out_count) FROM filtered_data) as current_occupancy,
            (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
        """

        # df, total_in_previous = await asyncio.gather(
        #     asyncio.to_thread(query_parquet_as_dataframe, query, params=params),
        #     self._get_previous_period_total_in()
        # )

        df, total_in_previous = await asyncio.gather(
            asyncio.to_thread(query_dataframe, query, params=params),
            self._get_previous_period_total_in()
        )

        if df.empty or pd.isna(df['total_in'].iloc[0]):
            return {
                'total_in': 0,
                'average_in': 0,
                'peak_time': '--:--',
                'current_occupancy': 0,
                'busiest_store': 'N/A',
                'growth': 0.0
            }

        data = df.iloc[0].to_dict()
        total_in_current = data.get('total_in', 0) or 0

        growth = 0.0
        if total_in_previous > 0:
            growth = round(((total_in_current - total_in_previous) / total_in_previous) * 100, 1)
        elif total_in_current > 0:
            growth = 100.0

        avg_val = data.get('average_in')
        data['average_in'] = 0 if pd.isna(avg_val) else int(round(avg_val))
        data['growth'] = growth
        if data.get('busiest_store'):
            data['busiest_store'] = data['busiest_store'].split(' (')[0]

        return data

    async def _get_previous_period_total_in(self) -> int:
        """Tính tổng lượt khách của kỳ liền trước để so sánh tăng trưởng."""
        time_delta = self.end_date - self.start_date

        period_logic = {
            'day': {
                'start': self.start_date - (time_delta + timedelta(days=1)),
                'end': self.end_date - (time_delta + timedelta(days=1))
            },
            'week': {
                'start': self.start_date - timedelta(weeks=1),
                'end': self.end_date - timedelta(weeks=1)
            },
            'month': {
                'start': self.start_date - relativedelta(months=1),
                'end': self.start_date - timedelta(days=1)
            },
            'year': {
                'start': self.start_date - relativedelta(years=1),
                'end': self.end_date - relativedelta(years=1)
            }
        }
        dates = period_logic.get(self.period)

        if not dates:
            return 0

        start_str, end_str = self._get_date_range_params(dates['start'], dates['end'])
        base_cte, params = self._build_base_query(start_str, end_str)

        query = f'{base_cte} SELECT SUM(in_count) as total FROM filtered_data'
        # df = await asyncio.to_thread(query_parquet_as_dataframe, query, params=params)
        df = await asyncio.to_thread(query_dataframe, query, params=params)

        if df.empty or df['total'].iloc[0] is None:
            return 0
        return int(df['total'].iloc[0])

    @async_cache
    async def get_trend_chart_data(self) -> List[Dict[str, Any]]:
        """Lấy dữ liệu chuỗi thời gian cho biểu đồ cột (column chart)."""
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

        query = f"""
        {base_cte}
        SELECT
            (date_trunc('{time_unit}', adjusted_time) + INTERVAL '{settings.WORKING_HOUR_START} hours') as x,
            SUM(in_count) as y
        FROM filtered_data
        GROUP BY x ORDER BY x
        """

        # df = await asyncio.to_thread(query_parquet_as_dataframe, query, params=params)
        df = await asyncio.to_thread(query_dataframe, query, params=params)

        if time_unit == 'month': df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m')
        elif time_unit == 'day': df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d')
        else: df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d %H:00')

        return df.to_dict(orient='records')

    @async_cache
    async def get_store_comparison_chart_data(self) -> List[Dict[str, Any]]:
        """Lấy dữ liệu phân bổ lượt khách theo từng cửa hàng cho biểu đồ tròn (donut chart)."""
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)

        query = f"""
        {base_cte}
        SELECT store_name as x, SUM(in_count) as y
        FROM filtered_data
        GROUP BY x
        ORDER BY y DESC
        """
        # df = await asyncio.to_thread(query_parquet_as_dataframe, query, params=params)
        df = await asyncio.to_thread(query_dataframe, query, params=params)
        return df.to_dict(orient='records')

    @async_cache
    async def get_paginated_details(self, page: int, page_size: int) -> Dict[str, Any]:
        """Lấy dữ liệu chi tiết, đã được phân trang cho bảng."""
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)

        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')
        date_format = {'hour': '%Y-%m-%d %H:00', 'day': '%Y-%m-%d', 'month': '%Y-%m'}.get(time_unit, '%Y-%m-%d')

        aggregation_query = f"""
        {base_cte}
        , period_summary AS (
            SELECT date_trunc('{time_unit}', adjusted_time) as period_start, SUM(in_count) as total_in
            FROM filtered_data GROUP BY period_start
        ),
        period_summary_with_lag AS (
            SELECT *, LAG(total_in, 1, 0) OVER (ORDER BY period_start) as previous_period_in
            FROM period_summary
        )
        SELECT
            strftime(period_start + INTERVAL '{settings.WORKING_HOUR_START} hours', '{date_format}') as period,
            total_in,
            CASE WHEN previous_period_in = 0 THEN 0.0 ELSE ROUND(((total_in - previous_period_in) * 100.0) / previous_period_in, 1) END as pct_change
        FROM period_summary_with_lag
        """

        final_query_cte = f"WITH query_result AS ({aggregation_query})"
        data_query = f"{final_query_cte} SELECT * FROM query_result ORDER BY period DESC LIMIT ? OFFSET ?"
        summary_query = f"{final_query_cte} SELECT SUM(total_in) as total_sum, AVG(total_in) as average_in FROM query_result"
        paginated_params = params + [page_size, (page - 1) * page_size]

        # df, summary_df = await asyncio.gather(
        #     asyncio.to_thread(query_parquet_as_dataframe, data_query, params=paginated_params),
        #     asyncio.to_thread(query_parquet_as_dataframe, summary_query, params=params)
        # )
        df, summary_df = await asyncio.gather(
            asyncio.to_thread(query_dataframe, data_query, params=paginated_params),
            asyncio.to_thread(query_dataframe, summary_query, params=params)
        )
        summary_data = summary_df.iloc[0].to_dict() if not summary_df.empty else {'total_sum': 0, 'average_in': 0}

        return {
            'total_records': len(df),
            'page': page,
            'page_size': page_size,
            'data': df.to_dict(orient='records'),
            'summary': summary_data
        }
# @staticmethod
#     def get_latest_record_time() -> Optional[datetime]:
#         """Lấy thời gian của bản ghi gần nhất trong toàn bộ dữ liệu."""
#         query = f"SELECT MAX(record_time) as latest_time FROM read_parquet('{settings.CROWD_COUNTS_PATH}')"
#         df = query_parquet_as_dataframe(query)
#         if not df.empty and pd.notna(df['latest_time'].iloc[0]):
#             return df['latest_time'].iloc[0]
#         return None

#     @staticmethod
#     def get_all_stores() -> List[str]:
#         """Lấy danh sách duy nhất tất cả các cửa hàng có trong dữ liệu."""
#         query = f"SELECT DISTINCT store_name FROM read_parquet('{settings.CROWD_COUNTS_PATH}') ORDER BY store_name"
#         df = query_parquet_as_dataframe(query)
#         return df['store_name'].tolist()

#     @staticmethod
#     def get_error_logs(limit: int = 100) -> List[Dict[str, Any]]:
#         """Lấy các log lỗi gần nhất từ dữ liệu."""
#         query = f"""
#         SELECT id, store_name, log_time, error_code, error_message
#         FROM read_parquet('{settings.ERROR_LOGS_PATH}')
#         ORDER BY log_time DESC
#         LIMIT ?
#         """
#         df = query_parquet_as_dataframe(query, params=[limit])
#         return df.to_dict(orient='records')

    @staticmethod
    def get_latest_record_time() -> Optional[datetime]:
        """Lấy thời gian của bản ghi gần nhất trong toàn bộ dữ liệu."""
        source_table_or_path = ''
        if settings.DATABASE_TYPE == 'duckdb_file':
            source_table_or_path = 'crowd_counts'
        else:
            source_table_or_path = f"read_parquet('{settings.CROWD_COUNTS_PATH}')"

        query = f"SELECT MAX(record_time) as latest_time FROM {source_table_or_path}"
        df = query_dataframe(query)
        if not df.empty and pd.notna(df['latest_time'].iloc[0]):
            return df['latest_time'].iloc[0]
        return None

    @staticmethod
    def get_all_stores() -> List[str]:
        """Lấy danh sách duy nhất tất cả các cửa hàng có trong dữ liệu."""
        source_table_or_path = ''
        if settings.DATABASE_TYPE == 'duckdb_file':
            source_table_or_path = 'crowd_counts'
        else:
            source_table_or_path = f"read_parquet('{settings.CROWD_COUNTS_PATH}')"

        query = f"SELECT DISTINCT store_name FROM {source_table_or_path} ORDER BY store_name"
        df = query_dataframe(query)
        return df['store_name'].tolist()

    @staticmethod
    def get_error_logs(limit: int = 100) -> List[Dict[str, Any]]:
        """Lấy các log lỗi gần nhất từ dữ liệu."""
        source_table_or_path = ''
        if settings.DATABASE_TYPE == 'duckdb_file':
            source_table_or_path = 'error_logs'
        else:
            source_table_or_path = f"read_parquet('{settings.ERROR_LOGS_PATH}')"

        query = f"""
        SELECT id, store_name, log_time, error_code, error_message
        FROM {source_table_or_path}
        ORDER BY log_time DESC
        LIMIT ?
        """
        df = query_dataframe(query, params=[limit])
        return df.to_dict(orient='records')






import asyncio
import pandas as pd

from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Optional, Tuple

from .core.caching import async_cache # Sẽ cần cập nhật caching.py để sử dụng settings.CACHE_TTL_SECONDS
from .core.config import settings
from .core.data_handler import query_dataframe # Đã đổi tên từ query_parquet_as_dataframe

class DashboardService:
    """Lớp chứa logic nghiệp vụ để xử lý và truy vấn dữ liệu cho dashboard.

    Mỗi instance của lớp này tương ứng với một bộ lọc (thời gian, cửa hàng)
    cụ thể từ người dùng, đóng vai trò là context cho các truy vấn dữ liệu.
    """
    def __init__(self, period: str, start_date: date, end_date: date, store: str = 'all'):
        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.store = store

    def _get_date_range_params(self, start_date: date, end_date: date) -> Tuple[str, str]:
        """Tạo chuỗi thời gian query dựa trên "ngày làm việc" đã định nghĩa.

        Giờ làm việc có thể kéo dài qua nửa đêm (ví dụ: 9h sáng đến 2h sáng hôm sau).
        Hàm này điều chỉnh ngày bắt đầu và kết thúc để bao trọn khung giờ này.
        """
        start_dt = datetime.combine(start_date, datetime.min.time()) + timedelta(hours=settings.WORKING_HOUR_START)
        end_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1, hours=settings.WORKING_HOUR_END)
        return start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')

    def _build_base_query(self, start_date_str: str, end_date_str: str) -> Tuple[str, list]:
        """Xây dựng câu truy vấn CTE (Common Table Expression) cơ sở và tham số.

        Hàm này tạo ra một CTE chuẩn hóa dữ liệu nguồn, bao gồm:
        - Lọc dữ liệu theo khoảng thời gian và cửa hàng.
        - Xử lý các giá trị ngoại lệ (outliers) theo cấu hình.
        - Điều chỉnh timestamp để phân tích theo "ngày làm việc".
        CTE này được tái sử dụng trong nhiều phương thức khác để tránh lặp code.
        """
        params = [start_date_str, end_date_str]

        store_filter_clause = ''
        if self.store != 'all':
            store_filter_clause = 'AND store_name = ?'
            params.append(self.store)

        # Quyết định nguồn dữ liệu dựa trên cấu hình
        source_table_or_path = ''
        if settings.DATABASE_TYPE == 'duckdb_file':
            source_table_or_path = 'crowd_counts' # Tên bảng trong file DuckDB
        else: # Mặc định hoặc 'parquet_folder'
            source_table_or_path = f"read_parquet('{settings.CROWD_COUNTS_PATH}')" # Đường dẫn Parquet

        # Xử lý outlier: thay thế các giá trị quá lớn bằng một tỷ lệ nhỏ hoặc giá trị cố định.
        if settings.OUTLIER_SCALE_RATIO > 0:
            then_logic_in = f'CAST(ROUND(in_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)'
            then_logic_out = f'CAST(ROUND(out_count * {settings.OUTLIER_SCALE_RATIO}, 0) AS INTEGER)'
        else:
            # Nếu không muốn scale, có thể thay bằng một giá trị mặc định, ví dụ là 1.
            then_logic_in, then_logic_out = '1', '1'

        base_cte = f"""
        WITH source_data AS (
            SELECT
                CAST(record_time AS TIMESTAMP) as record_time,
                store_name,
                CASE WHEN in_count > {settings.OUTLIER_THRESHOLD} THEN {then_logic_in} ELSE in_count END as in_count,
                CASE WHEN out_count > {settings.OUTLIER_THRESHOLD} THEN {then_logic_out} ELSE out_count END as out_count
            FROM {source_table_or_path}
            WHERE record_time >= ? AND record_time < ?
            {store_filter_clause}
        ),
        filtered_data AS (
            SELECT *, (record_time - INTERVAL '{settings.WORKING_HOUR_START} hours') AS adjusted_time
            FROM source_data
        )
        """
        return base_cte, params

    @async_cache
    async def get_metrics(self) -> Dict[str, Any]:
        """Lấy các chỉ số chính (KPIs) cho dashboard.

        Bao gồm tổng lượt vào, trung bình, giờ cao điểm, lượng khách hiện tại,
        cửa hàng đông nhất và tỷ lệ tăng trưởng so với kỳ trước.
        Xử lý các trường hợp không có dữ liệu để tránh lỗi.
        """
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)

        peak_time_format = {'day': '%H:%M', 'week': '%d/%m', 'month': '%d/%m', 'year': 'Tháng %m'}.get(self.period, '%d/%m')
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

        query = f"""
        {base_cte}
        , period_summary AS (
            SELECT SUM(in_count) as total_in_per_period FROM filtered_data
            GROUP BY date_trunc('{time_unit}', adjusted_time)
        )
        SELECT
            (SELECT SUM(in_count) FROM filtered_data) as total_in,
            (SELECT AVG(total_in_per_period) FROM period_summary) as average_in,
            (SELECT strftime(arg_max(record_time, in_count), '{peak_time_format}') FROM filtered_data) as peak_time,
            (SELECT SUM(in_count) - SUM(out_count) FROM filtered_data) as current_occupancy,
            (SELECT store_name FROM filtered_data GROUP BY store_name ORDER BY SUM(in_count) DESC LIMIT 1) as busiest_store
        """

        df, total_in_previous = await asyncio.gather(
            asyncio.to_thread(query_dataframe, query, params=params),
            self._get_previous_period_total_in()
        )

        if df.empty or pd.isna(df['total_in'].iloc[0]):
            return {
                'total_in': 0,
                'average_in': 0,
                'peak_time': '--:--',
                'current_occupancy': 0,
                'busiest_store': 'N/A',
                'growth': 0.0
            }

        data = df.iloc[0].to_dict()
        total_in_current = data.get('total_in', 0) or 0

        growth = 0.0
        if total_in_previous > 0:
            growth = round(((total_in_current - total_in_previous) / total_in_previous) * 100, 1)
        elif total_in_current > 0:
            growth = 100.0

        avg_val = data.get('average_in')
        data['average_in'] = 0 if pd.isna(avg_val) else int(round(avg_val))
        data['growth'] = growth
        if data.get('busiest_store'):
            data['busiest_store'] = data['busiest_store'].split(' (')[0]

        return data

    async def _get_previous_period_total_in(self) -> int:
        """Tính tổng lượt khách của kỳ liền trước để so sánh tăng trưởng."""
        time_delta = self.end_date - self.start_date

        period_logic = {
            'day': {
                'start': self.start_date - (time_delta + timedelta(days=1)),
                'end': self.end_date - (time_delta + timedelta(days=1))
            },
            'week': {
                'start': self.start_date - timedelta(weeks=1),
                'end': self.end_date - timedelta(weeks=1)
            },
            'month': {
                'start': self.start_date - relativedelta(months=1),
                'end': self.start_date - timedelta(days=1)
            },
            'year': {
                'start': self.start_date - relativedelta(years=1),
                'end': self.end_date - relativedelta(years=1)
            }
        }
        dates = period_logic.get(self.period)

        if not dates:
            return 0

        start_str, end_str = self._get_date_range_params(dates['start'], dates['end'])
        base_cte, params = self._build_base_query(start_str, end_str)

        query = f'{base_cte} SELECT SUM(in_count) as total FROM filtered_data'
        df = await asyncio.to_thread(query_dataframe, query, params=params)

        if df.empty or df['total'].iloc[0] is None:
            return 0
        return int(df['total'].iloc[0])

    @async_cache
    async def get_trend_chart_data(self) -> List[Dict[str, Any]]:
        """Lấy dữ liệu chuỗi thời gian cho biểu đồ cột (column chart)."""
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)
        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')

        query = f"""
        {base_cte}
        SELECT
            (date_trunc('{time_unit}', adjusted_time) + INTERVAL '{settings.WORKING_HOUR_START} hours') as x,
            SUM(in_count) as y
        FROM filtered_data
        GROUP BY x ORDER BY x
        """

        df = await asyncio.to_thread(query_dataframe, query, params=params)

        if time_unit == 'month': df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m')
        elif time_unit == 'day': df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d')
        else: df['x'] = pd.to_datetime(df['x']).dt.strftime('%Y-%m-%d %H:00')

        return df.to_dict(orient='records')

    @async_cache
    async def get_store_comparison_chart_data(self) -> List[Dict[str, Any]]:
        """Lấy dữ liệu phân bổ lượt khách theo từng cửa hàng cho biểu đồ tròn (donut chart)."""
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)

        query = f"""
        {base_cte}
        SELECT store_name as x, SUM(in_count) as y
        FROM filtered_data
        GROUP BY x
        ORDER BY y DESC
        """
        df = await asyncio.to_thread(query_dataframe, query, params=params)
        return df.to_dict(orient='records')

    @async_cache
    async def get_paginated_details(self, page: int, page_size: int) -> Dict[str, Any]:
        """Lấy dữ liệu chi tiết, đã được phân trang cho bảng."""
        start_str, end_str = self._get_date_range_params(self.start_date, self.end_date)
        base_cte, params = self._build_base_query(start_str, end_str)

        time_unit = {'year': 'month', 'month': 'day', 'week': 'day', 'day': 'hour'}.get(self.period, 'day')
        date_format = {'hour': '%Y-%m-%d %H:00', 'day': '%Y-%m-%d', 'month': '%Y-%m'}.get(time_unit, '%Y-%m-%d')

        aggregation_query = f"""
        {base_cte}
        , period_summary AS (
            SELECT date_trunc('{time_unit}', adjusted_time) as period_start, SUM(in_count) as total_in
            FROM filtered_data GROUP BY period_start
        ),
        period_summary_with_lag AS (
            SELECT *, LAG(total_in, 1, 0) OVER (ORDER BY period_start) as previous_period_in
            FROM period_summary
        )
        SELECT
            strftime(period_start + INTERVAL '{settings.WORKING_HOUR_START} hours', '{date_format}') as period,
            total_in,
            CASE WHEN previous_period_in = 0 THEN 0.0 ELSE ROUND(((total_in - previous_period_in) * 100.0) / previous_period_in, 1) END as pct_change
        FROM period_summary_with_lag
        """

        final_query_cte = f"WITH query_result AS ({aggregation_query})"
        data_query = f"{final_query_cte} SELECT * FROM query_result ORDER BY period DESC LIMIT ? OFFSET ?"
        summary_query = f"{final_query_cte} SELECT SUM(total_in) as total_sum, AVG(total_in) as average_in FROM query_result"
        paginated_params = params + [page_size, (page - 1) * page_size]

        df, summary_df = await asyncio.gather(
            asyncio.to_thread(query_dataframe, data_query, params=paginated_params),
            asyncio.to_thread(query_dataframe, summary_query, params=params)
        )
        summary_data = summary_df.iloc[0].to_dict() if not summary_df.empty else {'total_sum': 0, 'average_in': 0}

        return {
            'total_records': len(df),
            'page': page,
            'page_size': page_size,
            'data': df.to_dict(orient='records'),
            'summary': summary_data
        }

    @staticmethod
    def get_latest_record_time() -> Optional[datetime]:
        """Lấy thời gian của bản ghi gần nhất trong toàn bộ dữ liệu."""
        source_table_or_path = ''
        if settings.DATABASE_TYPE == 'duckdb_file':
            source_table_or_path = 'crowd_counts'
        else:
            source_table_or_path = f"read_parquet('{settings.CROWD_COUNTS_PATH}')"

        query = f"SELECT MAX(record_time) as latest_time FROM {source_table_or_path}"
        df = query_dataframe(query)
        if not df.empty and pd.notna(df['latest_time'].iloc[0]):
            return df['latest_time'].iloc[0]
        return None

    @staticmethod
    def get_all_stores() -> List[str]:
        """Lấy danh sách duy nhất tất cả các cửa hàng có trong dữ liệu."""
        source_table_or_path = ''
        if settings.DATABASE_TYPE == 'duckdb_file':
            source_table_or_path = 'crowd_counts'
        else:
            source_table_or_path = f"read_parquet('{settings.CROWD_COUNTS_PATH}')"

        query = f"SELECT DISTINCT store_name FROM {source_table_or_path} ORDER BY store_name"
        df = query_dataframe(query)
        return df['store_name'].tolist()

    @staticmethod
    def get_error_logs(limit: int = 100) -> List[Dict[str, Any]]:
        """Lấy các log lỗi gần nhất từ dữ liệu."""
        source_table_or_path = ''
        if settings.DATABASE_TYPE == 'duckdb_file':
            source_table_or_path = 'error_logs'
        else:
            source_table_or_path = f"read_parquet('{settings.ERROR_LOGS_PATH}')"

        query = f"""
        SELECT id, store_name, log_time, error_code, error_message
        FROM {source_table_or_path}
        ORDER BY log_time DESC
        LIMIT ?
        """
        df = query_dataframe(query, params=[limit])
        return df.to_dict(orient='records')

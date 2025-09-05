"""
Module xử lý giai đoạn 'E' (Extract) của pipeline ETL.

Chức năng chính là kết nối tới nguồn dữ liệu (MS SQL Server) và trích xuất
dữ liệu theo từng khối (chunk). Việc xử lý theo chunk giúp tối ưu hóa việc
sử dụng bộ nhớ, cho phép pipeline xử lý các tập dữ liệu lớn hơn nhiều
so với dung lượng RAM.
"""

import logging
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Iterator

from ..core.config import settings, TableConfig

logger = logging.getLogger(__name__)


def from_sql_server(
    sql_engine: Engine, config: TableConfig, last_timestamp: str
) -> Iterator[pd.DataFrame]:
    """
    Trích xuất dữ liệu từ MS SQL Server theo từng khối (chunk).

    Hàm này xây dựng và thực thi câu lệnh SQL để lấy toàn bộ dữ liệu (full-load)
    hoặc chỉ dữ liệu mới (incremental load) dựa trên cấu hình và "high-water mark".

    Args:
        sql_engine: SQLAlchemy engine đã kết nối tới SQL Server.
        config: Đối tượng cấu hình cho bảng đang được xử lý.
        last_timestamp: Giá trị "high-water mark" từ lần chạy thành công cuối cùng.

    Yields:
        Một iterator của các Pandas DataFrame, mỗi DataFrame là một chunk dữ liệu.

    Raises:
        SQLAlchemyError: Nếu có lỗi xảy ra trong quá trình thực thi truy vấn SQL.
    """
    source_columns = list(config.rename_map.keys())

    # Nếu là incremental, đảm bảo cột timestamp có trong danh sách để lọc.
    if config.timestamp_col and config.timestamp_col not in source_columns:
        source_columns.append(config.timestamp_col)

    if not source_columns:
        logger.warning(
            f"Bảng '{config.source_table}': Không có cột nào trong 'rename_map'. "
            f"Sử dụng 'SELECT *' làm mặc định."
        )
        columns_selection = "*"
    else:
        # Xây dựng chuỗi các cột được chọn, bọc trong `[]` để tương thích T-SQL.
        columns_selection = ", ".join(f"[{col}]" for col in source_columns)

    query = f"SELECT {columns_selection} FROM {config.source_table}"
    params = {}

    # Nếu là incremental load, thêm mệnh đề WHERE và ORDER BY.
    if config.incremental and config.timestamp_col:
        query += f" WHERE [{config.timestamp_col}] > :last_ts ORDER BY [{config.timestamp_col}]"
        params["last_ts"] = last_timestamp
        logger.info(
            f"Trích xuất incremental từ '{config.source_table}' "
            f"với high-water-mark > '{last_timestamp}'."
        )
    else:
        logger.info(f"Trích xuất full-load từ '{config.source_table}'.")

    # Ghi log câu lệnh SQL đầy đủ ở cấp độ DEBUG để tiện cho việc gỡ lỗi.
    logger.debug(f"Executing SQL: {query} with params: {params}")

    try:
        # Sử dụng `pd.read_sql` với `chunksize` để trả về một iterator,
        # giúp tiết kiệm bộ nhớ khi làm việc với dữ liệu lớn.
        # Dùng `text()` và `params` của SQLAlchemy để chống SQL Injection.
        return pd.read_sql(
            sql=text(query),
            con=sql_engine,
            params=params,
            chunksize=settings.ETL_CHUNK_SIZE,
        )
    except SQLAlchemyError as e:
        logger.error(
            f"Lỗi SQL khi trích xuất từ bảng '{config.source_table}': {e}"
        )
        # Ném lại lỗi để cơ chế retry của `cli.py` có thể bắt và xử lý.
        raise

"""
Module định nghĩa các dependency cho API, giúp tái sử dụng logic.

Dependency Injection là một tính năng cốt lõi của FastAPI, cho phép tách biệt
và tái sử dụng các thành phần như kết nối database, xác thực người dùng.
Điều này giúp mã nguồn trở nên module hóa, dễ kiểm thử và bảo trì hơn.
"""

import logging
from contextlib import contextmanager
from typing import Iterator

import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection
from duckdb import Error as DuckDBError

from .core.config import settings

logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection() -> Iterator[DuckDBPyConnection]:
    """
    Context manager để quản lý vòng đời kết nối đến DuckDB.

    Hàm này tạo ra một kết nối khi vào khối `with` và đảm bảo nó được đóng lại
    an toàn khi kết thúc, kể cả khi có lỗi xảy ra. Kết nối được mở ở chế độ
    chỉ đọc (read-only) để đảm bảo an toàn cho dữ liệu trong môi trường API.

    Yields:
        Một đối tượng kết nối DuckDB (DuckDBPyConnection) đang hoạt động.

    Raises:
        DuckDBError: Nếu không thể kết nối tới tệp database.
    """
    conn = None
    try:
        db_path = str(settings.DUCKDB_PATH.resolve())
        logger.debug(f"Đang mở kết nối tới DuckDB (read-only): {db_path}")

        # Kết nối ở chế độ READ_ONLY để đảm bảo an toàn, API chỉ có quyền đọc.
        conn = duckdb.connect(database=db_path, read_only=True)
        yield conn

    except DuckDBError as e:
        logger.critical(f"Không thể kết nối tới DuckDB: {e}", exc_info=True)
        # Ném lại lỗi để FastAPI xử lý và trả về lỗi 500 Internal Server Error.
        raise

    finally:
        if conn:
            conn.close()
            logger.debug("Kết nối DuckDB đã được đóng.")


def query_db_to_df(query: str, params: list = None) -> pd.DataFrame:
    """
    Hàm tiện ích để thực thi SQL và trả về kết quả dưới dạng DataFrame.

    Tự quản lý việc mở và đóng kết nối, phù hợp cho các tác vụ truy vấn
    đơn lẻ trong tầng service.

    Args:
        query: Câu lệnh SQL cần thực thi.
        params: Danh sách các tham số cho câu lệnh SQL để chống SQL injection.

    Returns:
        Một Pandas DataFrame chứa kết quả. Trả về DataFrame rỗng nếu có lỗi.
    """
    try:
        # Sử dụng context manager để đảm bảo kết nối được quản lý an toàn.
        with get_db_connection() as conn:
            return conn.execute(query, parameters=params).df()
    except Exception:
        # Lỗi đã được log trong `get_db_connection`. Ở đây, chúng ta chỉ cần
        # trả về một DataFrame rỗng để business logic có thể xử lý trường hợp
        # không có dữ liệu mà không làm sập ứng dụng.
        return pd.DataFrame()

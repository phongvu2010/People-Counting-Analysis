import duckdb
import logging
import pandas as pd

# from ..utils.logger import setup_logging
from ..core.config import settings # Import settings


# def get_duckdb_connection():
#     return duckdb.connect(database=':memory:', read_only=False)

# Kết nối DuckDB sẽ phụ thuộc vào cấu hình
def get_duckdb_connection_for_query():
    if settings.DATABASE_TYPE == 'duckdb_file':
        if not settings.DUCKDB_FILE_PATH:
            raise ValueError("DUCKDB_FILE_PATH phải được cung cấp khi DATABASE_TYPE là 'duckdb_file'")
        return duckdb.connect(database=settings.DUCKDB_FILE_PATH, read_only=True) # Chỉ đọc để đảm bảo an toàn
    else: # Mặc định hoặc 'parquet_folder'
        return duckdb.connect(database=':memory:', read_only=False) # In-memory cho parquet, cần write_only=False để đọc

# def query_parquet_as_dataframe(query: str, params: list = None) -> pd.DataFrame:
#     """Thực thi một câu lệnh SQL trên các tệp Parquet bằng DuckDB.

#     Hàm này mở một kết nối DuckDB, thực thi truy vấn, và đóng kết nối
#     để giải phóng tài nguyên.

#     Args:
#         query: Câu lệnh SQL để thực thi.
#         params: Danh sách các tham số để truyền vào câu lệnh SQL một cách an toàn.

#     Returns:
#         Một DataFrame chứa kết quả, hoặc DataFrame rỗng nếu có lỗi.
#     """
#     con = get_duckdb_connection()
#     try:
#         # Thực thi câu lệnh và trả về kết quả dưới dạng Pandas DataFrame
#         return con.execute(query, parameters=params).df()
#     except Exception as e:
#         setup_logging('database_duckdb')
#         logging.error(f'Lỗi khi thực thi query với DuckDB: {e}\nQuery: {query}\nParams: {params}')
#         return pd.DataFrame()
#     finally:
#         # Đảm bảo kết nối luôn được đóng sau khi sử dụng.
#         con.close()

def query_dataframe(query: str, params: list = None) -> pd.DataFrame:
    """Thực thi một câu lệnh SQL trên dữ liệu (Parquet hoặc DuckDB file) bằng DuckDB.

    Hàm này mở một kết nối DuckDB, thực thi truy vấn, và đóng kết nối
    để giải phóng tài nguyên.
    """
    try:
        with get_duckdb_connection_for_query() as con: # Sử dụng context manager
            return con.execute(query, parameters=params).df()
    except Exception as e:
        logging.error(f'Lỗi khi thực thi query với DuckDB: {e}\nQuery: {query}\nParams: {params}')
        return pd.DataFrame()

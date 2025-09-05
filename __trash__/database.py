# import logging
# import pandas as pd

# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker, Session

# from .config import settings
# from ..utils.logger import setup_logging

# # Tạo SQLAlchemy engine từ chuỗi kết nối trong config
# # echo = False để không in các câu lệnh SQL ra console trong môi trường production
# engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, echo=False)

# # Tạo một lớp SessionLocal được cấu hình để tạo các session CSDL
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# def get_db():
#     """
#     Dependency function cho FastAPI để cung cấp một session DB cho mỗi request.
#     Sử dụng 'yield' để đảm bảo session được đóng lại sau khi request hoàn tất,
#     kể cả khi có lỗi xảy ra, giúp quản lý kết nối hiệu quả.
#     """
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

# def execute_query_as_dataframe(query: str, db: Session, params: dict = None) -> pd.DataFrame:
#     """
#     Thực thi một câu lệnh SQL và trả về kết quả dưới dạng Pandas DataFrame.
#     Đây là một hàm helper rất hữu ích cho việc phân tích dữ liệu.

#     Args:
#         query (str): Câu lệnh SQL (có thể chứa tham số placeholder).
#         db (Session): Session SQLAlchemy được inject từ dependency.
#         params (dict, optional): Các tham số cho câu lệnh SQL để tránh SQL injection.

#     Returns:
#         pd.DataFrame: DataFrame chứa kết quả, hoặc DataFrame rỗng nếu có lỗi.
#     """
#     try:
#         # Sử dụng connection của session để thực thi với pandas
#         return pd.read_sql_query(query, db.connection(), params=params)
#     except Exception as e:
#         setup_logging('database_mssql')
#         logging.error(f'Lỗi khi thực thi query với SQLAlchemy: {e}\n')
#         return pd.DataFrame()

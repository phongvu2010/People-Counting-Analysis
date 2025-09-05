import logging
import os

from datetime import date

def setup_logging(log_name: str, log_dir: str = 'logs'):
    """
    Thiết lập hệ thống logging để ghi ra cả console và file.
    Tên file log sẽ được tạo dựa trên tên được cung cấp.

    Args:
        log_name (str): Tên của logger, thường là tên script (ví dụ: 'etl_script').
        log_dir (str): Thư mục để lưu file log.
    """
    os.makedirs(log_dir, exist_ok=True)

    # Tạo tên file log dựa trên tên script và ngày tháng
    date_str = date.today().strftime('%Y-%m-%d')
    log_file = os.path.join(log_dir, f'{log_name}_{date_str}.log')

    # Tránh thêm handlers nhiều lần nếu hàm được gọi lại trong cùng một tiến trình
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()

    logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers = [
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    # Gán tên cho logger gốc để nó xuất hiện trong format string
    logging.root.name = log_name

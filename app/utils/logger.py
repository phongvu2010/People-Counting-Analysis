"""
Module tiện ích để thiết lập hệ thống logging cho toàn bộ ứng dụng.

Cung cấp chức năng đọc cấu hình từ một tệp YAML, cho phép thiết lập linh hoạt
các handlers (ví dụ: console, file), formatters và log levels. Hỗ trợ ghi đè
log level thông qua biến môi trường, giúp việc gỡ lỗi trở nên dễ dàng hơn.
"""

import logging
import logging.config
import os
from pathlib import Path
from typing import Union

import yaml


class MaxLevelFilter(logging.Filter):
    """
    Filter để chỉ cho phép các log record có level DƯỚI hoặc BẰNG một mức cho trước.

    Sử dụng để tách biệt các luồng output. Ví dụ, handler cho `stdout`
    (đầu ra chuẩn) sẽ dùng filter này để chỉ hiển thị log INFO và DEBUG, trong
    khi handler cho `stderr` (đầu ra lỗi) sẽ hiển thị các log từ WARNING trở lên.
    """

    def __init__(self, level: Union[str, int], **kwargs):
        super().__init__(**kwargs)
        if isinstance(level, str):
            # Chuyển đổi tên level dạng chuỗi (không phân biệt hoa thường)
            # thành giá trị số nguyên tương ứng (ví dụ: "WARNING" -> 30).
            self.level = logging.getLevelNamesMapping()[level.upper()]
        else:
            self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Kiểm tra xem một log record có nên được xử lý hay không.

        Returns:
            True nếu level của record <= level của filter, ngược lại là False.
        """
        return record.levelno <= self.level


def setup_logging(
    config_path: Union[str, Path] = "configs/logger.yaml",
    default_level: int = logging.INFO,
) -> None:
    """
    Thiết lập cấu hình logging cho toàn bộ ứng dụng từ một tệp YAML.

    Hàm này sẽ đọc tệp cấu hình, đảm bảo thư mục log tồn tại, và áp dụng cấu hình.
    Nếu có lỗi, nó sẽ quay về sử dụng cấu hình logging cơ bản để đảm bảo
    ứng dụng không bị crash.

    Args:
        config_path: Đường dẫn đến tệp YAML cấu hình logging.
        default_level: Log level mặc định nếu không tìm thấy tệp cấu hình.
    """
    config_path = Path(config_path)

    # Đảm bảo thư mục 'logs' tồn tại để các file handler có thể ghi file.
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    if not config_path.is_file():
        logging.basicConfig(level=default_level)
        logging.warning(
            f"Không tìm thấy tệp cấu hình tại '{config_path}'. "
            f"Sử dụng cấu hình logging cơ bản."
        )
        return

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config_dict = yaml.safe_load(f)
        if not config_dict:
            raise ValueError("Tệp YAML rỗng hoặc không hợp lệ.")

        # Cho phép ghi đè log level bằng biến môi trường `LOG_LEVEL`.
        # Rất hữu ích khi cần gỡ lỗi trên môi trường production mà không
        # cần thay đổi code hay tệp config.
        log_level_from_env = os.environ.get("LOG_LEVEL")
        if log_level_from_env and "root" in config_dict:
            config_dict["root"]["level"] = log_level_from_env.upper()
            logging.info(
                f"Log level được ghi đè thành '{log_level_from_env.upper()}' "
                f"bởi biến môi trường LOG_LEVEL."
            )

        logging.config.dictConfig(config_dict)

    except Exception as e:
        # Nếu có bất kỳ lỗi nào, quay về cấu hình cơ bản để đảm bảo
        # ứng dụng vẫn có thể ghi log lỗi.
        logging.basicConfig(level=default_level)
        logging.exception(f"Lỗi khi cấu hình logging từ tệp YAML: {e}")

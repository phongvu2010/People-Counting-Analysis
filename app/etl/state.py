"""
Module quản lý trạng thái của pipeline ETL.

Chức năng chính là đọc và ghi "high-water mark" (thường là timestamp lớn
nhất đã xử lý) cho mỗi bảng vào một tệp JSON. Điều này cho phép pipeline
"ghi nhớ" đã xử lý đến đâu, để trong lần chạy tiếp theo, nó chỉ cần lấy
các bản ghi mới hơn.
"""

import json
import logging
from pathlib import Path
from typing import Dict

import pandas as pd

from ..core.config import settings

logger = logging.getLogger(__name__)
STATE_FILE = Path(settings.STATE_FILE)


def load_etl_state() -> Dict[str, str]:
    """
    Tải trạng thái ETL (high-water marks) từ tệp JSON.

    Nếu tệp không tồn tại hoặc lỗi, một dictionary rỗng sẽ được trả về,
    khiến pipeline tự động chạy ở chế độ full-load cho lần đầu tiên.

    Returns:
        Một dictionary chứa trạng thái của các bảng.
    """
    if not STATE_FILE.exists():
        logger.warning(f"Không tìm thấy tệp trạng thái '{STATE_FILE}'. "
                       f"Giả định đây là lần chạy đầu tiên (full-load).")
        return {}
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning(f"Không thể đọc tệp trạng thái '{STATE_FILE}'. "
                       f"Bắt đầu lại từ đầu.")
        return {}


def save_etl_state(state: Dict[str, str]):
    """
    Lưu trạng thái ETL hiện tại vào tệp JSON.

    Args:
        state: Dictionary chứa trạng thái cần lưu.
    """
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
        logger.debug(f"Trạng thái ETL đã được lưu vào: {STATE_FILE}")
    except IOError as e:
        logger.error(f"Lỗi nghiêm trọng khi ghi tệp trạng thái '{STATE_FILE}': {e}")


def get_last_timestamp(state: Dict[str, str], table_name: str) -> str:
    """
    Lấy high-water mark của một bảng từ state.

    Args:
        state: Dictionary trạng thái ETL.
        table_name: Tên của bảng đích cần lấy timestamp.

    Returns:
        Chuỗi timestamp của lần chạy thành công cuối cùng, hoặc giá trị
        mặc định nếu bảng chưa có trong state.
    """
    return state.get(table_name, settings.ETL_DEFAULT_TIMESTAMP)


def update_timestamp(
    state: Dict[str, str], table_name: str, new_timestamp: pd.Timestamp
):
    """
    Cập nhật high-water mark cho một bảng trong dictionary state.

    Args:
        state: Dictionary trạng thái ETL (sẽ được cập nhật tại chỗ).
        table_name: Tên của bảng đích cần cập nhật.
        new_timestamp: Giá trị pandas.Timestamp mới.
    """
    if pd.notna(new_timestamp):
        # Chuyển đổi timestamp thành chuỗi ISO 8601, định dạng chuẩn và rõ ràng.
        # Ví dụ: '2023-10-27 15:30:00'
        timestamp_str = new_timestamp.isoformat(sep=" ", timespec="seconds")
        state[table_name] = timestamp_str
        logger.debug(f"Cập nhật high-water-mark cho '{table_name}': {timestamp_str}")
    else:
        logger.warning(
            f"Bỏ qua cập nhật high-water-mark cho '{table_name}' vì giá trị mới không hợp lệ."
        )

"""
Module triển khai cơ chế caching cho các hàm service.

Sử dụng `cachetools.TTLCache` để tạo một bộ nhớ cache trong bộ nhớ (in-memory)
với chính sách hết hạn theo thời gian (Time-To-Live) và giới hạn kích thước
(LRU - Least Recently Used).
"""

import logging
from functools import wraps
from typing import Any, Callable

from cachetools import TTLCache

logger = logging.getLogger(__name__)

# Khởi tạo một bộ nhớ cache dùng chung cho toàn bộ ứng dụng.
# - maxsize=128: Lưu trữ tối đa 128 kết quả gần nhất. Khi cache đầy, các
#   item cũ nhất sẽ bị loại bỏ (LRU).
# - ttl=1800: Time-To-Live. Mỗi item trong cache sẽ tự động hết hạn sau
#   1800 giây (30 phút), đảm bảo dữ liệu không quá cũ.
service_cache = TTLCache(maxsize=128, ttl=1800)


def async_cache(func: Callable) -> Callable:
    """
    Decorator để cache kết quả của một hàm `async`.

    Nó tạo ra một cache key duy nhất dựa trên tên hàm và các tham số đầu vào.
    Nếu key đã tồn tại trong cache, kết quả được trả về ngay lập tức.
    Nếu không, hàm gốc sẽ được gọi và kết quả sẽ được lưu vào cache.
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs) -> Any:
        # Tạo cache key từ một tuple chứa các thành phần bất biến (immutable)
        # để đảm bảo tính duy nhất và khả năng băm (hashable).
        key_parts = (
            func.__name__,
            self.period,
            self.start_date.isoformat(),
            self.end_date.isoformat(),
            self.store,
            args,
            # frozenset đảm bảo các item trong kwargs được xử lý
            # không phụ thuộc vào thứ tự.
            frozenset(kwargs.items()),
        )
        # Sử dụng hàm hash() để tạo ra một key ngắn gọn, hiệu quả.
        key = hash(key_parts)

        # 1. Cache hit: Nếu key tồn tại, trả về kết quả ngay lập tức.
        if key in service_cache:
            logger.debug(f"Cache hit for function '{func.__name__}' with key '{key}'")
            return service_cache[key]

        # 2. Cache miss: Nếu không có trong cache, gọi hàm gốc.
        logger.debug(f"Cache miss for function '{func.__name__}' with key '{key}'")
        result = await func(self, *args, **kwargs)

        # 3. Lưu vào cache: Lưu kết quả mới vào cache với key đã tạo.
        service_cache[key] = result
        logger.debug(f"Result for '{func.__name__}' stored in cache.")

        return result

    return wrapper


def clear_service_cache():
    """
    Xóa toàn bộ các item trong `service_cache`.

    Hữu ích khi cần làm mới dữ liệu sau khi ETL hoàn tất.
    """
    logger.info(f"Đang xóa cache. Kích thước hiện tại: {service_cache.currsize} items.")
    service_cache.clear()
    logger.info("✅ Cache đã được xóa thành công.")

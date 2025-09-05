from cachetools import TTLCache
from functools import wraps
from typing import Callable, Any

from app.core.config import settings

# Bộ nhớ cache chia sẻ trong ứng dụng, có thời gian sống (TTL).
# - maxsize=128: Lưu trữ tối đa 128 kết quả gần nhất.
# - ttl=1800: Mỗi item trong cache sẽ hết hạn sau 1800 giây (30 phút).
# service_cache = TTLCache(maxsize=128, ttl=1800)
service_cache = TTLCache(maxsize=128, ttl=settings.CACHE_TTL_SECONDS)

def async_cache(func: Callable) -> Callable:
    """Decorator để cache kết quả của các hàm async trong service.

    Decorator này giải quyết vấn đề service được tạo mới mỗi request bằng cách
    tạo ra một cache key duy nhất dựa trên các thuộc tính quan trọng của
    instance service (như khoảng thời gian, cửa hàng) và các tham số của hàm.
    Điều này đảm bảo các request với cùng bộ lọc sẽ nhận lại kết quả từ cache.
    """
    @wraps(func)
    async def wrapper(self, *args, **kwargs) -> Any:
        # Tạo cache key duy nhất từ tên hàm, các thuộc tính của service,
        # và các tham số truyền vào.
        key = (
            func.__name__,
            self.period,
            self.start_date.isoformat(),
            self.end_date.isoformat(),
            self.store,
            args,
            frozenset(kwargs.items())   # Chuyển kwargs thành dạng hashable
        )

        # Kiểm tra và trả về kết quả từ cache nếu tồn tại.
        if key in service_cache:
            return service_cache[key]

        # Nếu không có trong cache, gọi hàm gốc để lấy dữ liệu mới.
        result = await func(self, *args, **kwargs)

        # Lưu kết quả vào cache trước khi trả về.
        service_cache[key] = result
        return result
    return wrapper

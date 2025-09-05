"""
Module định nghĩa các schema xác thực dữ liệu bằng Pandera.

Mỗi schema tương ứng với một bảng đích trong DuckDB và hoạt động như một
"hợp đồng dữ liệu" (data contract). Việc xác thực này đảm bảo dữ liệu được
nạp vào kho luôn tuân thủ đúng định dạng, kiểu dữ liệu và các ràng buộc,
giúp duy trì chất lượng và tính toàn vẹn của dữ liệu.
"""

import pandera.pandas as pa
from pandera.typing import DateTime, Int, Series, String


class DimStoresSchema(pa.DataFrameModel):
    """Schema xác thực cho bảng `dim_stores`."""

    store_id: Series[Int] = pa.Field(unique=True, nullable=False)
    store_name: Series[String] = pa.Field(nullable=False)

    class Config:
        strict = True  # Đảm bảo DataFrame không có cột nào thừa so với schema.
        coerce = True  # Tự động ép kiểu dữ liệu nếu hợp lệ (ví dụ: "1" -> 1).


class FactTrafficSchema(pa.DataFrameModel):
    """Schema xác thực cho bảng `fact_traffic`."""

    recorded_at: Series[DateTime] = pa.Field(nullable=False)
    # ge=0: đảm bảo giá trị lớn hơn hoặc bằng 0.
    visitors_in: Series[Int] = pa.Field(ge=0, default=0)
    visitors_out: Series[Int] = pa.Field(ge=0, default=0)
    device_position: Series[String] = pa.Field(nullable=True)
    store_id: Series[Int] = pa.Field(nullable=False)

    # Các cột partition được thêm vào trong quá trình transform.
    year: Series[Int]
    month: Series[Int]

    class Config:
        strict = True
        coerce = True


class FactErrorsSchema(pa.DataFrameModel):
    """Schema xác thực cho bảng `fact_errors`."""

    log_id: Series[Int] = pa.Field(unique=True, nullable=False)
    store_id: Series[Int] = pa.Field(nullable=False)
    device_code: Series[Int] = pa.Field(nullable=True)
    logged_at: Series[DateTime] = pa.Field(nullable=False)
    error_code: Series[Int] = pa.Field(nullable=True)
    error_message: Series[String] = pa.Field(nullable=True)

    # Các cột partition được thêm vào trong quá trình transform.
    year: Series[Int]
    month: Series[Int]

    class Config:
        strict = True
        coerce = True


# Dictionary để dễ dàng truy cập schema từ tên bảng trong pipeline.
table_schemas = {
    "dim_stores": DimStoresSchema,
    "fact_traffic": FactTrafficSchema,
    "fact_errors": FactErrorsSchema,
}

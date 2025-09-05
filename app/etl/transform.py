"""
Module xử lý giai đoạn 'T' (Transform) của pipeline ETL.

Nhận vào một DataFrame thô và áp dụng một chuỗi các thao tác biến đổi:
- Điều chỉnh chênh lệch thời gian (time offsets).
- Đổi tên cột và làm sạch dữ liệu chuỗi.
- Chuẩn hóa kiểu dữ liệu.
- Tạo các cột partition (ví dụ: year, month).
- Xác thực dữ liệu với schema của Pandera để đảm bảo chất lượng.
"""

import logging
import pandas as pd
import pandera.errors as pa_errors
from typing import Optional

from .schemas import table_schemas
from ..core.config import settings, TableConfig

logger = logging.getLogger(__name__)


# --- Các hàm biến đổi riêng lẻ (Private Helper Functions) ---


def _apply_time_offsets(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """Áp dụng điều chỉnh chênh lệch thời gian cho cột timestamp dựa trên cấu hình."""
    if not config.timestamp_col:
        return df

    table_name_key = config.source_table.split(".")[-1]
    offsets_config = settings.TIME_OFFSETS.get(table_name_key)
    if not offsets_config:
        return df

    # Các cột cần thiết cho việc điều chỉnh
    store_id_col = "storeid"
    ts_col = config.timestamp_col
    if store_id_col not in df.columns or ts_col not in df.columns:
        logger.warning(
            f"Bỏ qua điều chỉnh time offset cho '{table_name_key}' do thiếu cột."
        )
        return df

    # Vector hóa việc điều chỉnh để tăng hiệu suất
    offsets = df[store_id_col].map(offsets_config).fillna(0)
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce") - pd.to_timedelta(
        offsets, unit="m"
    )

    logger.debug(f"Đã áp dụng điều chỉnh chênh lệch thời gian cho '{table_name_key}'.")
    return df


def _rename_and_clean(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """Đổi tên cột theo `rename_map` và áp dụng các quy tắc làm sạch."""
    if config.rename_map:
        df = df.rename(columns=config.rename_map)

    for rule in config.cleaning_rules:
        # Lấy tên cột sau khi đã đổi tên để áp dụng rule
        col_to_clean = config.rename_map.get(rule.column, rule.column)

        if rule.action == "strip" and col_to_clean in df.columns:
            if pd.api.types.is_object_dtype(df[col_to_clean]):
                df[col_to_clean] = df[col_to_clean].str.strip()
    return df


def _handle_data_types(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """Chuẩn hóa kiểu dữ liệu cho các cột quan trọng."""
    # Xử lý các cột số (in/out)
    numeric_cols = [
        config.rename_map.get("in_num"),
        config.rename_map.get("out_num"),
    ]
    for col in filter(None, numeric_cols):
        if col in df.columns:
            # Chuyển đổi, điền giá trị rỗng bằng 0, đảm bảo không âm
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            df[col] = df[col].apply(lambda x: max(0, x))

    # Xử lý cột timestamp và tạo partition
    ts_col = config.final_timestamp_col
    if ts_col and ts_col in df.columns:
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
        df = df.dropna(subset=[ts_col])  # Loại bỏ các dòng có timestamp không hợp lệ

        if not df.empty:
            if "year" in config.partition_cols:
                df["year"] = df[ts_col].dt.year
            if "month" in config.partition_cols:
                df["month"] = df[ts_col].dt.month
    return df


def _select_and_validate(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """Chọn các cột cuối cùng và xác thực DataFrame với schema Pandera."""
    schema = table_schemas.get(config.dest_table)
    if not schema:
        logger.warning(
            f"Không tìm thấy schema cho '{config.dest_table}'. Bỏ qua xác thực."
        )
        return df

    # Chọn các cột có trong schema để tránh lỗi validation với cột thừa
    schema_cols = list(schema.to_schema().columns.keys())
    final_cols = [col for col in schema_cols if col in df.columns]
    df_subset = df[final_cols]

    try:
        # `validate` trả về DataFrame đã được ép kiểu (coerced) đúng chuẩn
        return schema.validate(df_subset, lazy=True)
    except pa_errors.SchemaErrors as err:
        logger.error(
            f"Xác thực dữ liệu cho '{config.dest_table}' thất bại! "
            f"Xem chi tiết các dòng lỗi bên dưới:\n{err.failure_cases.to_string()}"
        )
        # Lưu dữ liệu lỗi để phân tích (Dead-Letter Queue)
        rejected_path = settings.DATA_DIR / "rejected" / config.dest_table
        rejected_path.mkdir(parents=True, exist_ok=True)
        timestamp_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        file_path = rejected_path / f"rejected_{timestamp_str}.parquet"
        
        try:
            err.failure_cases.to_parquet(file_path)
            logger.warning(f"Dữ liệu lỗi đã được lưu tại: {file_path}")
        except Exception as e:
            logger.error(f"Không thể lưu file dữ liệu lỗi: {e}")

        raise


# --- Hàm điều phối chính (Public Orchestrator Function) ---


def run_transformations(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Điều phối toàn bộ quy trình biến đổi dữ liệu trên một DataFrame.

    Sử dụng phương thức `.pipe()` của Pandas để chuỗi các hàm biến đổi lại
    với nhau, giúp mã nguồn dễ đọc, dễ hiểu và dễ dàng thay đổi thứ tự
    hoặc thêm/bớt các bước.

    Args:
        df: DataFrame đầu vào từ bước Extract.
        config: Cấu hình cho bảng đang được xử lý.

    Returns:
        DataFrame đã được biến đổi, làm sạch và xác thực.
    """
    if df.empty:
        return df

    try:
        # Chuỗi các bước biến đổi dữ liệu
        transformed_df = df.pipe(_apply_time_offsets, config).pipe(
            _rename_and_clean, config
        ).pipe(_handle_data_types, config).pipe(_select_and_validate, config)
        return transformed_df
    except pa_errors.SchemaErrors:
        # Nếu validation thất bại, trả về một DataFrame rỗng để pipeline
        # không nạp dữ liệu lỗi vào database.
        logger.error(
            f"Quy trình transform cho '{config.dest_table}' đã dừng do lỗi validation."
        )
        return pd.DataFrame()
    except Exception as e:
        logger.error(
            f"Lỗi không mong muốn trong quá trình transform '{config.dest_table}': {e}",
            exc_info=True,
        )
        return pd.DataFrame()


def get_max_timestamp(
    df: pd.DataFrame, config: TableConfig
) -> Optional[pd.Timestamp]:
    """
    Lấy giá trị timestamp lớn nhất từ một chunk đã biến đổi thành công.

    Giá trị này sẽ được dùng để cập nhật "high-water mark" cho lần ETL tiếp theo.

    Args:
        df: DataFrame đã được biến đổi.
        config: Cấu hình của bảng.

    Returns:
        Timestamp lớn nhất, hoặc None nếu không áp dụng (ví dụ: full-load).
    """
    if not config.incremental or df.empty:
        return None

    ts_col = config.final_timestamp_col
    if ts_col and ts_col in df.columns:
        # Đảm bảo cột là kiểu datetime trước khi lấy max
        if pd.api.types.is_datetime64_any_dtype(df[ts_col]):
            max_ts = df[ts_col].max()
            return max_ts if pd.notna(max_ts) else None

    return None

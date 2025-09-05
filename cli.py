"""
Module định nghĩa giao diện dòng lệnh (CLI) cho ứng dụng.

Sử dụng Typer để tạo các câu lệnh tiện ích, bao gồm:
- `run-etl`: Chạy quy trình ETL đa luồng để đồng bộ dữ liệu.
- `init-db`: Khởi tạo các đối tượng cần thiết trong DuckDB (ví dụ: VIEWs).
- `serve`: Khởi chạy web server FastAPI.
"""

import contextlib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Iterator
from typing_extensions import Annotated

import duckdb
import pandera.errors as pa_errors
import requests
import typer
import uvicorn
from duckdb import DuckDBPyConnection
from duckdb import Error as DuckdbError
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_fixed,
)

from app.core.config import settings, TableConfig
from app.etl import extract, state, transform
from app.etl.load import ParquetLoader, prepare_destination, refresh_duckdb_table
from app.utils.logger import setup_logging

# Cấu hình logging ngay từ đầu để áp dụng cho toàn bộ ứng dụng.
setup_logging("configs/logger.yaml")
logger = logging.getLogger(__name__)

# Khởi tạo Typer App để quản lý các câu lệnh.
cli_app = typer.Typer(
    help="CLI để quản lý và vận hành ứng dụng Analytics iCount People."
)


@contextlib.contextmanager
def _get_database_connections() -> Iterator[tuple[Engine, DuckDBPyConnection]]:
    """
    Context manager để quản lý vòng đời kết nối đến các database.

    Yields:
        Một tuple chứa (SQLAlchemy Engine, DuckDB Connection).

    Raises:
        SQLAlchemyError: Nếu không thể kết nối tới MS SQL Server.
        DuckdbError: Nếu không thể kết nối tới DuckDB.
    """
    sql_engine, duckdb_conn = None, None
    try:
        logger.info("Đang thiết lập kết nối tới MS SQL Server...")
        sql_engine = create_engine(
            settings.db.sqlalchemy_db_uri, pool_pre_ping=True
        )
        with sql_engine.connect() as conn:
            conn.execute(text("SELECT 1"))  # Ping để kiểm tra
        logger.info("✅ Kết nối MS SQL Server thành công.")

        logger.info("Đang thiết lập kết nối tới DuckDB...")
        duckdb_path = str(settings.DUCKDB_PATH.resolve())
        duckdb_conn = duckdb.connect(database=duckdb_path, read_only=False)
        logger.info(f"✅ Kết nối DuckDB ('{duckdb_path}') thành công.\n")

        yield sql_engine, duckdb_conn

    except SQLAlchemyError as e:
        logger.critical(f"❌ Lỗi nghiêm trọng khi kết nối SQL Server: {e}", exc_info=True)
        raise
    except DuckdbError as e:
        logger.critical(f"❌ Lỗi nghiêm trọng khi kết nối DuckDB: {e}", exc_info=True)
        raise
    finally:
        if sql_engine:
            sql_engine.dispose()
            logger.debug("Kết nối SQL Server đã được đóng.")
        if duckdb_conn:
            duckdb_conn.close()
            logger.debug("Kết nối DuckDB đã được đóng.")


def _is_retryable_exception(exception: BaseException) -> bool:
    """Kiểm tra xem một exception có thuộc loại có thể thử lại hay không."""
    return isinstance(exception, (SQLAlchemyError, DuckdbError, IOError))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(15),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    retry=retry_if_exception(_is_retryable_exception),
)
def _process_table(
    sql_engine: Engine,
    duckdb_conn: DuckDBPyConnection,
    config: TableConfig,
    etl_state: dict,
    state_lock: Lock,
):
    """
    Xử lý toàn bộ pipeline ETL cho một bảng duy nhất (Extract -> Transform -> Load).

    Hàm này được bọc bởi decorator @retry để tự động thử lại nếu gặp lỗi
    liên quan đến kết nối hoặc I/O.
    """
    logger.info(
        f"Bắt đầu xử lý bảng: '{config.source_table}' -> '{config.dest_table}' "
        f"(Incremental: {config.incremental})"
    )
    prepare_destination(config)

    last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)
    data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp)

    total_rows, max_ts_in_run = 0, None

    try:
        with ParquetLoader(config) as loader:
            for chunk in data_iterator:
                transformed_chunk = transform.run_transformations(chunk, config)
                if transformed_chunk.empty:
                    continue

                loader.write_chunk(transformed_chunk)
                total_rows += len(transformed_chunk)

                current_max_ts = transform.get_max_timestamp(
                    transformed_chunk, config
                )
                if current_max_ts and (
                    max_ts_in_run is None or current_max_ts > max_ts_in_run
                ):
                    max_ts_in_run = current_max_ts

        if total_rows > 0:
            logger.info(
                f"Đã xử lý {total_rows:,} dòng. Bắt đầu nạp vào DuckDB..."
            )
            refresh_duckdb_table(duckdb_conn, config, loader.has_written_data)
            logger.info(f"Nạp dữ liệu vào DuckDB '{config.dest_table}' hoàn tất.")

            if config.incremental and max_ts_in_run:
                with state_lock:
                    state.update_timestamp(
                        etl_state, config.dest_table, max_ts_in_run
                    )
                    state.save_etl_state(etl_state)
        else:
            logger.info(f"Không có dữ liệu mới cho bảng '{config.dest_table}'.")

        return config.dest_table

    except pa_errors.SchemaErrors as e:
        logger.error(
            f"❌ Lỗi validation dữ liệu cho '{config.dest_table}': {e}",
            exc_info=True,
        )
        raise
    except Exception as e:
        logger.error(
            f"❌ Lỗi không thể phục hồi sau các lần thử lại cho "
            f"'{config.dest_table}': {e}",
            exc_info=True,
        )
        raise


def _trigger_cache_clear(host: str, port: int):
    """Gửi yêu cầu POST đến API server để xóa cache."""
    if not settings.INTERNAL_API_TOKEN:
        logger.warning(
            "INTERNAL_API_TOKEN chưa được cấu hình, bỏ qua việc xóa cache."
        )
        return

    clear_cache_url = f"http://{host}:{port}/api/v1/admin/clear-cache"
    headers = {"X-Internal-Token": settings.INTERNAL_API_TOKEN}

    try:
        logger.info(f"Đang gửi yêu cầu xóa cache đến {clear_cache_url}...")
        response = requests.post(clear_cache_url, headers=headers, timeout=10)
        response.raise_for_status()
        if response.status_code == 204:
            logger.info("✅ Yêu cầu xóa cache được API server chấp nhận.")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Không thể xóa cache của API server: {e}")
        logger.warning(
            "Lưu ý: Dữ liệu mới có thể mất tới 30 phút để hiển thị trên dashboard."
        )


@cli_app.command()
def run_etl(
    max_workers: int = typer.Option(
        4, help="Số luồng tối đa để xử lý ETL song song."
    ),
    clear_cache: bool = typer.Option(
        True, help="Tự động xóa cache của API server sau khi ETL thành công."
    ),
    api_host: str = typer.Option(
        "127.0.0.1", help="Host của API server đang chạy."
    ),
    api_port: int = typer.Option(8000, help="Port của API server đang chạy."),
):
    """Chạy quy trình ETL đa luồng để đồng bộ dữ liệu từ SQL Server sang DuckDB."""
    logger.info("=" * 60)
    logger.info(f"🚀 BẮT ĐẦU QUY TRÌNH ETL (Tối đa {max_workers} luồng)")
    logger.info("=" * 60)

    succeeded, failed = [], []
    etl_state = state.load_etl_state()
    state_lock = Lock()

    tables_to_process = sorted(
        settings.TABLE_CONFIG.values(), key=lambda cfg: cfg.processing_order
    )
    total_tables = len(tables_to_process)

    try:
        with _get_database_connections() as (sql_engine, duckdb_conn):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_table = {
                    executor.submit(
                        _process_table,
                        sql_engine,
                        duckdb_conn,
                        config,
                        etl_state,
                        state_lock,
                    ): config
                    for config in tables_to_process
                }

                for future in as_completed(future_to_table):
                    config = future_to_table[future]
                    try:
                        result = future.result()
                        succeeded.append(result)
                        logger.info(f"✅ Xử lý thành công '{config.dest_table}'.\n")
                    except Exception:
                        failed.append(config.dest_table)
                        logger.error(
                            f"❌ Xử lý '{config.dest_table}' thất bại sau tất cả "
                            f"các lần thử lại.\n"
                        )
    except Exception as e:
        logger.critical(
            f"Quy trình ETL bị dừng đột ngột do lỗi kết nối ban đầu: {e}"
        )

    finally:
        if clear_cache and succeeded:
            _trigger_cache_clear(host=api_host, port=api_port)

        logger.info("=" * 60)
        logger.info("📊 TÓM TẮT KẾT QUẢ ETL")
        logger.info(f"Tổng số bảng: {total_tables}")
        logger.info(f"✅ Thành công: {len(succeeded)}")
        logger.info(f"❌ Thất bại: {len(failed)}")
        if failed:
            logger.warning(f"Danh sách bảng thất bại: {', '.join(failed)}")
        logger.info("=" * 60 + "\n")


@cli_app.command()
def init_db():
    """Khởi tạo hoặc cập nhật các VIEWs cần thiết trong DuckDB."""
    logger.info("Bắt đầu khởi tạo/cập nhật VIEW 'v_traffic_normalized'...")

    scale = settings.OUTLIER_SCALE_RATIO
    then_logic_in = (
        f"CAST(ROUND(a.visitors_in * {scale}, 0) AS INTEGER)" if scale > 0 else "1"
    )
    then_logic_out = (
        f"CAST(ROUND(a.visitors_out * {scale}, 0) AS INTEGER)" if scale > 0 else "1"
    )

    create_view_sql = f"""
    CREATE OR REPLACE VIEW v_traffic_normalized AS
    SELECT
        CAST(a.recorded_at AS TIMESTAMP) AS record_time,
        b.store_name,
        CASE
            WHEN a.visitors_in > {settings.OUTLIER_THRESHOLD} THEN {then_logic_in}
            ELSE a.visitors_in
        END AS in_count,
        CASE
            WHEN a.visitors_out > {settings.OUTLIER_THRESHOLD} THEN {then_logic_out}
            ELSE a.visitors_out
        END AS out_count,
        -- Dịch chuyển thời gian để ngày làm việc bắt đầu từ 00:00
        (record_time - INTERVAL '{settings.WORKING_HOUR_START} hours') AS adjusted_time
    FROM fact_traffic AS a
    LEFT JOIN dim_stores AS b ON a.store_id = b.store_id;
    """

    try:
        db_path = str(settings.DUCKDB_PATH.resolve())
        with duckdb.connect(database=db_path, read_only=False) as conn:
            conn.execute(create_view_sql)
        logger.info("✅ Đã tạo/cập nhật thành công VIEW 'v_traffic_normalized'.")
    except Exception as e:
        logger.error(f"❌ Lỗi khi khởi tạo VIEW: {e}", exc_info=True)
        raise typer.Exit(code=1)


@cli_app.command()
def serve(
    host: Annotated[
        str, typer.Option(help="Host để chạy server.")
    ] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port để chạy server.")] = 8000,
    reload: Annotated[
        bool, typer.Option(help="Tự động tải lại khi code thay đổi.")
    ] = True,
):
    """Khởi chạy ứng dụng web FastAPI với Uvicorn."""
    logger.info(f"🚀 Khởi chạy FastAPI server tại http://{host}:{port}")
    uvicorn.run(
        "app.main:api_app",
        host=host,
        port=port,
        reload=reload,
        reload_dirs=["app", "configs", "template"],
    )


if __name__ == "__main__":
    cli_app()

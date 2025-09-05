"""
Module xử lý giai đoạn 'L' (Load) của pipeline ETL.

Chức năng chính bao gồm:
1. Ghi dữ liệu đã biến đổi vào một khu vực trung gian (staging area)
   dưới định dạng Parquet, có hỗ trợ partition.
2. Nạp dữ liệu từ các tệp Parquet vào DuckDB một cách an toàn và không
   gây gián đoạn bằng kỹ thuật "atomic swap".
"""

import logging
import shutil
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from duckdb import DuckDBPyConnection

from ..core.config import settings, TableConfig

logger = logging.getLogger(__name__)
BASE_DATA_PATH = Path(settings.DATA_DIR)


class ParquetLoader:
    """
    Context manager để quản lý việc ghi dữ liệu vào tệp/dataset Parquet.

    Lớp này trừu tượng hóa logic phức tạp của việc ghi dữ liệu theo từng khối,
    tự động xử lý việc mở và đóng `ParquetWriter` một cách an toàn,
    đảm bảo tài nguyên được giải phóng đúng cách.
    """

    def __init__(self, config: TableConfig):
        self.config = config
        self.dest_path = BASE_DATA_PATH / self.config.dest_table
        self.writer: Optional[pq.ParquetWriter] = None
        self.has_written_data = False

    def __enter__(self):
        # Đảm bảo thư mục đích tồn tại khi bắt đầu
        self.dest_path.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Đảm bảo writer được đóng lại an toàn khi kết thúc khối `with`
        if self.writer:
            self.writer.close()
        if exc_type is not None:
            logger.error(
                f"Lỗi khi ghi Parquet cho '{self.config.dest_table}': {exc_val}"
            )

    def write_chunk(self, df: pd.DataFrame):
        """Ghi một chunk DataFrame vào staging area (Parquet)."""
        if df.empty:
            return
        try:
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)

            if self.config.partition_cols:
                # Ghi dưới dạng dataset nếu có partition
                pq.write_to_dataset(
                    arrow_table,
                    root_path=str(self.dest_path),
                    partition_cols=self.config.partition_cols,
                    existing_data_behavior="overwrite_or_ignore",
                )
            else:
                # Ghi vào một tệp Parquet duy nhất
                if self.writer is None:
                    output_file = self.dest_path / "data.parquet"
                    if not self.config.incremental and output_file.exists():
                        output_file.unlink()
                    self.writer = pq.ParquetWriter(
                        str(output_file), arrow_table.schema
                    )
                self.writer.write_table(arrow_table)

            self.has_written_data = True
        except pa.ArrowException as e:
            logger.error(f"Lỗi PyArrow khi ghi chunk cho '{self.config.dest_table}': {e}")
            raise


def prepare_destination(config: TableConfig):
    """
    Chuẩn bị thư mục staging: dọn dẹp thư mục cũ nếu là full-load.
    """
    dest_path = BASE_DATA_PATH / config.dest_table
    if not config.incremental and dest_path.exists():
        logger.info(f"Full-load: Đang dọn dẹp staging cũ: {dest_path}")
        try:
            shutil.rmtree(dest_path)
        except OSError as e:
            logger.error(f"Lỗi khi dọn dẹp staging cũ '{dest_path}': {e}")
            raise
    dest_path.mkdir(parents=True, exist_ok=True)


def refresh_duckdb_table(
    conn: DuckDBPyConnection, config: TableConfig, has_new_data: bool
):
    """
    Tải dữ liệu từ Parquet vào DuckDB và thực hiện "atomic swap".

    Quy trình này đảm bảo an toàn và không gián đoạn cho người dùng cuối:
    1. Tải dữ liệu từ Parquet vào một bảng tạm (_staging).
    2. Bắt đầu một TRANSACTION.
    3. Đổi tên bảng chính hiện tại (nếu có) thành bảng cũ (_old).
    4. "Thăng cấp" bảng tạm thành bảng chính.
    5. COMMIT transaction. Bước này diễn ra gần như tức thời.
    6. Dọn dẹp bảng cũ và chạy ANALYZE để tối ưu hóa hiệu năng.
    7. Nếu có lỗi, ROLLBACK để quay về trạng thái ban đầu.
    """
    if not has_new_data:
        logger.info(
            f"Bỏ qua refresh DuckDB cho '{config.dest_table}' vì không có dữ liệu mới."
        )
        return

    dest_table = config.dest_table
    staging_table = f"{dest_table}_staging"
    backup_table = f"{dest_table}_old"
    staging_dir = str(BASE_DATA_PATH / dest_table)

    try:
        # 1. Tải Parquet vào bảng staging
        logger.info(f"Bắt đầu nạp Parquet vào staging table '{staging_table}'...")
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {staging_table} AS
            SELECT * FROM read_parquet('{staging_dir}/**', hive_partitioning=true);
        """
        )

        # 2. Thực hiện hoán đổi nguyên tử (atomic swap) trong một transaction
        logger.info(f"Bắt đầu hoán đổi (atomic swap) cho bảng '{dest_table}'...")
        conn.execute(
            f"""
            BEGIN TRANSACTION;

            -- Dọn dẹp bảng backup cũ nếu nó còn tồn tại từ lần chạy lỗi trước.
            DROP TABLE IF EXISTS {backup_table};

            -- Đổi tên bảng chính hiện tại thành bảng backup.
            ALTER TABLE IF EXISTS {dest_table} RENAME TO {backup_table};

            -- "Thăng cấp" bảng staging mới thành bảng chính.
            ALTER TABLE {staging_table} RENAME TO {dest_table};

            COMMIT;
        """
        )
        logger.info(f"Hoán đổi bảng '{dest_table}' thành công.")

        # 3. Dọn dẹp và tối ưu hóa
        conn.execute(f"DROP TABLE IF EXISTS {backup_table};")
        logger.debug(f"Đã dọn dẹp bảng backup '{backup_table}'.")

        if not config.incremental and settings.ETL_CLEANUP_ON_FAILURE:
            shutil.rmtree(staging_dir)
            logger.info(f"Đã dọn dẹp staging area '{staging_dir}'.")

        # Cập nhật thống kê để bộ tối ưu hóa truy vấn của DuckDB hoạt động hiệu quả.
        logger.info(f"Đang cập nhật thống kê cho bảng '{dest_table}'...")
        conn.execute(f"ANALYZE {dest_table};")
        logger.info(f"✅ Cập nhật thống kê cho bảng '{dest_table}' thành công.")

    except Exception as e:
        logger.error(
            f"Lỗi khi refresh bảng DuckDB '{dest_table}': {e}", exc_info=True
        )
        conn.execute("ROLLBACK;")
        logger.warning(f"Đã ROLLBACK transaction cho bảng '{dest_table}'.")
        raise

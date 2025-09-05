"""
Module quản lý cấu hình tập trung cho toàn bộ ứng dụng.

Sử dụng Pydantic Settings để đọc, xác thực và quản lý cấu hình từ các nguồn
như tệp .env và biến môi trường. Điều này đảm bảo rằng tất cả các giá trị
cấu hình đều đúng kiểu dữ liệu, nhất quán và dễ dàng truy cập thông qua
một đối tượng `settings` duy nhất.
"""

import yaml
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Annotated,
)
from urllib import parse

from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    TypeAdapter,
    ValidationError,
    AnyUrl,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


def _parse_cors_origins(value: Any) -> List[str] | str:
    """
    Chuyển đổi chuỗi CORS từ biến môi trường thành danh sách các URL.

    Args:
        value: Giá trị đầu vào, có thể là chuỗi hoặc danh sách.

    Returns:
        Danh sách các origin hợp lệ.
    """
    if isinstance(value, str) and not value.startswith("["):
        return [item.strip() for item in value.split(",")]
    if isinstance(value, (list, str)):
        return value
    raise ValueError(value)


class CleaningRule(BaseModel):
    """Định nghĩa một quy tắc làm sạch dữ liệu cho một cột."""

    column: str
    action: Literal["strip"]


class DatabaseSettings(BaseModel):
    """Cấu hình kết nối đến MS SQL Server."""

    SQLSERVER_DRIVER: str
    SQLSERVER_SERVER: str
    SQLSERVER_DATABASE: str
    SQLSERVER_UID: str
    SQLSERVER_PWD: str

    @property
    def sqlalchemy_db_uri(self) -> str:
        """
        Tạo chuỗi kết nối SQLAlchemy an toàn, tương thích với pyodbc.

        Tự động mã hóa các ký tự đặc biệt trong mật khẩu và driver
        để đảm bảo chuỗi kết nối luôn hợp lệ.

        Returns:
            Chuỗi kết nối SQLAlchemy.
        """
        encoded_pwd = parse.quote_plus(self.SQLSERVER_PWD)
        driver_for_query = self.SQLSERVER_DRIVER.replace(" ", "+")

        return (
            f"mssql+pyodbc://{self.SQLSERVER_UID}:{encoded_pwd}"
            f"@{self.SQLSERVER_SERVER}/{self.SQLSERVER_DATABASE}"
            f"?driver={driver_for_query}"
        )


class TableConfig(BaseModel):
    """
    Định nghĩa cấu hình chi tiết cho quy trình ETL của một bảng.

    Mỗi đối tượng này đại diện cho một pipeline nhỏ, từ trích xuất,
    biến đổi đến nạp dữ liệu cho một bảng cụ thể.
    """

    source_table: str
    dest_table: str
    incremental: bool = True
    description: Optional[str] = None
    processing_order: int = 99
    rename_map: Dict[str, str] = Field(default_factory=dict)
    partition_cols: List[str] = Field(default_factory=list)
    cleaning_rules: List[CleaningRule] = Field(default_factory=list)
    timestamp_col: Optional[str] = None

    @model_validator(mode="after")
    def _validate_incremental_config(self) -> "TableConfig":
        """Đảm bảo `timestamp_col` tồn tại nếu `incremental` là True."""
        if self.incremental and not self.timestamp_col:
            raise ValueError(
                f"Bảng '{self.source_table}': 'timestamp_col' là bắt buộc "
                f"khi 'incremental' được bật."
            )
        return self

    @property
    def final_timestamp_col(self) -> Optional[str]:
        """Lấy tên cột timestamp cuối cùng (sau khi đã đổi tên)."""
        if not self.timestamp_col:
            return None
        return self.rename_map.get(self.timestamp_col, self.timestamp_col)


class Settings(BaseSettings):
    """
    Model cấu hình chính, tổng hợp tất cả thiết lập cho ứng dụng.

    Tự động đọc các biến từ tệp `.env` và môi trường, sau đó xác thực
    và gán chúng vào các thuộc tính đã định nghĩa.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Cấu hình chung ---
    PROJECT_NAME: str = "Analytics iCount People API"
    DESCRIPTION: str = "API cung cấp dữ liệu phân tích lượt ra vào cửa hàng."
    INTERNAL_API_TOKEN: str
    BACKEND_CORS_ORIGINS: Annotated[
        List[AnyUrl], BeforeValidator(_parse_cors_origins)
    ] = []

    # --- Cấu hình logic nghiệp vụ ---
    OUTLIER_THRESHOLD: int = 100
    OUTLIER_SCALE_RATIO: float = 0.00001
    WORKING_HOUR_START: int = 9
    WORKING_HOUR_END: int = 2

    # --- Cấu hình Database (sẽ được nhóm vào đối tượng `db`) ---
    SQLSERVER_DRIVER: str = "ODBC Driver 17 for SQL Server"
    SQLSERVER_SERVER: str
    SQLSERVER_DATABASE: str
    SQLSERVER_UID: str
    SQLSERVER_PWD: str

    # --- Cấu hình ETL ---
    DATA_DIR: Path = Path("data")
    ETL_CHUNK_SIZE: int = 100_000
    ETL_DEFAULT_TIMESTAMP: str = "1900-01-01 00:00:00"
    ETL_CLEANUP_ON_FAILURE: bool = True
    TABLE_CONFIG_PATH: Path = Path("configs/tables.yaml")
    TIME_OFFSETS_PATH: Path = Path("configs/time_offsets.yaml")

    # --- Thuộc tính được tính toán và tải động ---
    db: Optional[DatabaseSettings] = None
    TABLE_CONFIG: Dict[str, TableConfig] = Field(default_factory=dict)
    TIME_OFFSETS: Dict[str, Dict[int, int]] = Field(default_factory=dict)

    @property
    def DUCKDB_PATH(self) -> Path:
        """Đường dẫn đầy đủ đến tệp cơ sở dữ liệu DuckDB."""
        return self.DATA_DIR / "analytics.duckdb"

    @property
    def STATE_FILE(self) -> Path:
        """Đường dẫn đầy đủ đến tệp JSON lưu trạng thái ETL."""
        return self.DATA_DIR / "etl_state.json"

    @model_validator(mode="after")
    def _assemble_settings(self) -> "Settings":
        """Tự động tạo các đối tượng cấu hình phụ sau khi load .env."""
        # 1. Nhóm các biến kết nối DB vào một đối tượng `db`
        if not self.db:
            self.db = DatabaseSettings(
                SQLSERVER_DRIVER=self.SQLSERVER_DRIVER,
                SQLSERVER_SERVER=self.SQLSERVER_SERVER,
                SQLSERVER_DATABASE=self.SQLSERVER_DATABASE,
                SQLSERVER_UID=self.SQLSERVER_UID,
                SQLSERVER_PWD=self.SQLSERVER_PWD,
            )

        # 2. Tải cấu hình bảng và time offsets từ file YAML
        self._load_table_config()
        self._load_time_offsets()

        return self

    def _load_table_config(self):
        """Tải và xác thực cấu hình bảng từ tệp YAML."""
        if self.TABLE_CONFIG:
            return

        try:
            with self.TABLE_CONFIG_PATH.open("r", encoding="utf-8") as f:
                raw_config = yaml.safe_load(f)
            if not raw_config:
                raise ValueError(f"Tệp cấu hình '{self.TABLE_CONFIG_PATH}' rỗng.")

            adapter = TypeAdapter(Dict[str, TableConfig])
            self.TABLE_CONFIG = adapter.validate_python(raw_config)
        except FileNotFoundError:
            raise ValueError(f"Không tìm thấy tệp cấu hình: {self.TABLE_CONFIG_PATH}")
        except (yaml.YAMLError, ValidationError) as e:
            raise ValueError(f"Lỗi cú pháp trong tệp '{self.TABLE_CONFIG_PATH}':\n{e}")

    def _load_time_offsets(self):
        """Tải cấu hình chênh lệch thời gian từ tệp YAML."""
        if self.TIME_OFFSETS:
            return

        try:
            with self.TIME_OFFSETS_PATH.open("r", encoding="utf-8") as f:
                raw_offsets = yaml.safe_load(f)
            if not raw_offsets:
                raise ValueError(f"Tệp cấu hình '{self.TIME_OFFSETS_PATH}' rỗng.")
            self.TIME_OFFSETS = raw_offsets
        except FileNotFoundError:
            raise ValueError(f"Không tìm thấy tệp: {self.TIME_OFFSETS_PATH}")
        except yaml.YAMLError as e:
            raise ValueError(f"Lỗi cú pháp trong tệp '{self.TIME_OFFSETS_PATH}':\n{e}")


# Khởi tạo instance duy nhất để sử dụng trong toàn bộ ứng dụng.
settings = Settings()

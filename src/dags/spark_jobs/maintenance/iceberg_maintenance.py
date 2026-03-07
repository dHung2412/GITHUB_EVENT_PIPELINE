import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

# Setup Path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config import config
from spark_client import SparkClient

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

class IcebergMaintenance:
    def __init__(self, table_name: str, app_name: str):
        self.table_name = table_name
        self.app_name = app_name
        self.spark = None

    def _init_spark(self):
        if not self.spark:
            self.spark = SparkClient(app_name=self.app_name, job_type="batch").get_session()

    def compact(self, sort_order: str, days_back: int = 7, min_files: int = 5, target_size_mb: int = 20):
        """Thực hiện nén (Compaction) các file nhỏ"""
        self._init_spark()
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        target_bytes = target_size_mb * 1024 * 1024

        logger.info(f"-----> [COMPACTION] Table: {self.table_name}, Days back: {days_back}")

        sql = f"""
            CALL {config.ICEBERG_CATALOG}.system.rewrite_data_files(
                table => '{self.table_name}',
                strategy => 'sort',
                sort_order => '{sort_order}',
                options => map(
                    'target-file-size-bytes', '{target_bytes}',
                    'min-input-files', '{min_files}'
                ),
                where => "ingestion_date >= DATE '{cutoff_date}'"
            )
        """
        result = self.spark.sql(sql).first()
        if result:
            logger.info(f"-----> Result: Rewritten {result.rewritten_data_files_count} files, Added {result.added_data_files_count} files.")

    def expire_snapshots(self, days_back: int = 7, retain_last: int = 5):
        """Xóa các Snapshot cũ dựa trên thời gian"""
        self._init_spark()
        logger.info(f"-----> [EXPIRE-SNAPSHOTS] Table: {self.table_name}, Older than: {days_back} days")
        
        sql = f"CALL {config.ICEBERG_CATALOG}.system.expire_snapshots(table => '{self.table_name}', older_than => TIMESTAMP '{ (datetime.now() - timedelta(days=days_back)).isoformat() }', retain_last => {retain_last})"
        self.spark.sql(sql).show()

    def remove_orphan_files(self, days_back: int = 3):
        """Xóa các file rác không thuộc về metadata nào"""
        self._init_spark()
        logger.info(f"-----> [REMOVE-ORPHAN] Table: {self.table_name}")
        
        sql = f"CALL {config.ICEBERG_CATALOG}.system.remove_orphan_files(table => '{self.table_name}', older_than => TIMESTAMP '{ (datetime.now() - timedelta(days=days_back)).isoformat() }')"
        self.spark.sql(sql).show()

    def rewrite_manifests(self):
        """Tối ưu hóa file manifest (danh mục file)"""
        self._init_spark()
        logger.info(f"-----> [REWRITE-MANIFESTS] Table: {self.table_name}")
        self.spark.sql(f"CALL {config.ICEBERG_CATALOG}.system.rewrite_manifests(table => '{self.table_name}')")

    def stop(self):
        if self.spark:
            self.spark.stop()
            logger.info("-----> Spark session closed.")

def get_table_path(layer: str) -> str:
    if layer.lower() == 'bronze':
        return f"{config.ICEBERG_CATALOG}.{config.BRONZE_NAMESPACE}.{config.BRONZE_TABLE}"
    elif layer.lower() == 'silver':
        return f"{config.ICEBERG_CATALOG}.{config.SILVER_NAMESPACE}.{config.SILVER_TABLE}"
    raise ValueError(f"Unknown layer: {layer}")

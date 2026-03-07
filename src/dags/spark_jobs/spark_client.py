import os
import sys
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from pyspark.sql import SparkSession
from config import config

class SparkClient:
    def __init__(self, app_name: str, job_type: str = "batch"):
        self.app_name = app_name
        self.job_type = job_type
        self.logger = logging.getLogger(__name__)

        # Đảm bảo Python environment đồng nhất
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        os.environ['AWS_REGION'] = 'us-east-1' # Quan trọng cho S3FileIO
    
    def get_session(self) -> SparkSession:
        self.logger.info(f"-----> [SPARK_CLIENT] Initializing {self.app_name} ({self.job_type})...")    

        # 1. Detect Environment & Hosts
        is_docker = os.getenv("HOSTNAME") is not None and not os.getenv("HOSTNAME").startswith("DESKTOP")
        minio_host = "minio:9000" if is_docker else "localhost:9000"
        rest_host = "rest:8181" if is_docker else "localhost:8181"
        master_conf = "local[*]" if self.job_type == "streaming" else "local[2]"

        self.logger.info(f"-----> [SPARK_CLIENT] Env: {'Docker' if is_docker else 'Local'} | Master: {master_conf}")

        # 2. Build Session
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master_conf)

        # 3. Apply Config Groups
        builder = self._set_s3a_configs(builder, minio_host)
        builder = self._set_iceberg_configs(builder, minio_host, rest_host)
        
        if self.job_type == "streaming":
            builder = self._set_streaming_configs(builder)
            
        return builder.getOrCreate()

    def _set_s3a_configs(self, builder: SparkSession.Builder, host: str) -> SparkSession.Builder:
        """Cấu hình S3A cho Spark"""
        return builder \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{host}") \
            .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ROOT_USER) \
            .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_ROOT_PASSWORD) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    def _set_iceberg_configs(self, builder: SparkSession.Builder, minio_host: str, rest_host: str) -> SparkSession.Builder:
        """Cấu hình Apache Iceberg với REST Catalog"""
        catalog = config.ICEBERG_CATALOG
        
        # Iceberg S3FileIO nên dùng s3:// thay vì s3a://
        warehouse = config.ICEBERG_WAREHOUSE_PATH.replace("s3a://", "s3://")
        
        return builder \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{catalog}.type", "rest") \
            .config(f"spark.sql.catalog.{catalog}.uri", f"http://{rest_host}") \
            .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse) \
            .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config(f"spark.sql.catalog.{catalog}.s3.endpoint", f"http://{minio_host}") \
            .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true") \
            .config(f"spark.sql.catalog.{catalog}.s3.connection.ssl.enabled", "false") \
            .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", config.MINIO_ROOT_USER) \
            .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", config.MINIO_ROOT_PASSWORD) \
            .config(f"spark.sql.catalog.{catalog}.client.region", "us-east-1") \
            .config("spark.sql.defaultCatalog", catalog)

    def _set_streaming_configs(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        """Tối ưu hóa các tham số mạng cho Streaming Job"""
        return builder \
            .config("spark.python.worker.reuse", "true") \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.heartbeatInterval", "60s")

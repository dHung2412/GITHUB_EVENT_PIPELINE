"""
Bronze to Silver Layer Processing
=================================
Pipeline xử lý dữ liệu từ tầng Bronze (Iceberg) sang tầng Silver (Iceberg).
Flow:
    Bronze Iceberg Table
    -> Incremental Load (Filter by ingestion_timestamp)
    -> Deduplication (Drop duplicates by ID)
    -> Transformation (Parse JSON payload, casting types)
    -> Silver Iceberg Table (Merge: Insert on not matched)
"""

import logging
import os
import sys
from typing import List, Optional

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType

# ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config import config
from spark_client import SparkClient
from utils.helper.load_sql import load_sql_from_file

# ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# =================== Schema ===================
def get_unified_payload_schema() -> StructType:
    return StructType([
        StructField("action", StringType(), True),
        StructField("ref", StringType(), True),
        StructField("ref_type", StringType(), True),
        StructField("size", IntegerType(), True),
        StructField("distinct_size", IntegerType(), True),
        StructField("head", StringType(), True),
        
        StructField("pull_request", StructType([
            StructField("id", LongType(), True),
            StructField("number", IntegerType(), True),
            StructField("state", StringType(), True),
            StructField("title", StringType(), True),
            StructField("merged", BooleanType(), True),
            StructField("merged_at", StringType(), True),
        ]), True),
    
        StructField("issue", StructType([
            StructField("number", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("state", StringType(), True),
        ]), True)
    ])

# =================== Table ===================
def create_silver_table_if_not_exists(spark: SparkSession, catalog_name: str, namespace_name: str, full_table_path: str):
    sql_file_path = os.path.join(parent_dir, "utils", "sql", "silver_table.sql")

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{namespace_name}")

        if spark.catalog.tableExists(full_table_path):
            logger.info(f"Table {full_table_path} already exists.")
            return
        
        logger.info(f"-----> Reading DDL from: {sql_file_path}.")
        create_sql = load_sql_from_file(
            str(sql_file_path),
            full_table_name=full_table_path
        )

        spark.sql(create_sql)
        logger.info(f"-----> Create Table: {full_table_path}.")

    except Exception as e:
        logger.error(f"-----> Failed create Table: {e}.")
        raise

def get_bronze_watermark(spark: SparkSession, silver_table: str) -> Optional[str]:
    """Lấy Snapshot ID cuối cùng của Bronze đã được xử lý từ thuộc tính bảng Silver"""
    try:
        props = spark.sql(f"SHOW TBLPROPERTIES {silver_table}").collect()
        for row in props:
            if row['key'] == 'last_processed_snapshot':
                return row['value']
        return None
    except Exception:
        return None

def set_bronze_watermark(spark: SparkSession, silver_table: str, snapshot_id: str):
    """Lưu Snapshot ID của Bronze vào thuộc tính bảng Silver sau khi xử lý thành công"""
    if snapshot_id:
        spark.sql(f"ALTER TABLE {silver_table} SET TBLPROPERTIES ('last_processed_snapshot' = '{snapshot_id}')")
        logger.info(f"-----> Watermark updated: {snapshot_id}")

def get_current_snapshot_id(spark: SparkSession, table_name: str) -> Optional[str]:
    """Lấy Snapshot ID mới nhất hiện tại của bảng"""
    try:
        row = spark.sql(f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
        return str(row['snapshot_id']) if row else None
    except Exception:
        return None

# =================== Transformation logic ===================
def get_silver_projection() -> List[Column]:
    p = col("payload_parsed")
    
    return [
        col("id").alias("event_id"),
        col("type").alias("event_type"),
        col("created_at_ts").alias("created_at"),
        col("public"),
        
        col("actor_id"), 
        col("actor_login"), 
        col("actor_url"), 
        col("actor_avatar_url"),
        
        col("repo_id"), 
        col("repo_name"), 
        col("repo_url"),

        p.getItem("action").alias("payload_action"),
        p.getItem("ref").alias("payload_ref"),
        p.getItem("ref_type").alias("payload_ref_type"),
        
        p.getItem("size").alias("push_size"),
        p.getItem("distinct_size").alias("push_distinct_size"),
        p.getItem("head").alias("push_head_sha"),

        p.getItem("pull_request").getItem("number").alias("pull_request_number"),
        p.getItem("pull_request").getItem("id").alias("pull_request_id"),
        p.getItem("pull_request").getItem("state").alias("pull_request_state"),
        p.getItem("pull_request").getItem("title").alias("pull_request_title"),
        p.getItem("pull_request").getItem("merged").alias("pull_request_merge"),
        when(p.getItem("pull_request").getItem("merged_at").isNotNull(),
             to_timestamp(p.getItem("pull_request").getItem("merged_at"))
        ).alias("pull_request_merge_at"),

        p.getItem("issue").getItem("number").alias("issue_number"),
        p.getItem("issue").getItem("title").alias("issue_title"),
        p.getItem("issue").getItem("state").alias("issue_state"),
        
        col("ingestion_date"),
        current_timestamp().alias("processed_at")
    ]

def run_process_bronze_to_silver(spark: SparkSession):
    catalog = config.ICEBERG_CATALOG
    bronze_table_full = f"{catalog}.{config.BRONZE_NAMESPACE}.{config.BRONZE_TABLE}"
    silver_table_full = f"{catalog}.{config.SILVER_NAMESPACE}.{config.SILVER_TABLE}"

    logger.info(f"-----> Start processing {bronze_table_full} -> {silver_table_full}")
    # 1. Initialize table
    create_silver_table_if_not_exists(spark, catalog, config.SILVER_NAMESPACE, silver_table_full)

    # 2. Watermark & Snapshot Management
    last_snapshot_id = get_bronze_watermark(spark, silver_table_full)
    current_bronze_snapshot = get_current_snapshot_id(spark, bronze_table_full)

    # 3. Read Incremental Data using Iceberg native feature
    if last_snapshot_id:
        if last_snapshot_id == current_bronze_snapshot:
            logger.info("-----> No new data to process. Skipping.")
            return
        
        logger.info(f"-----> Incremental Load: Reading since snapshot {last_snapshot_id}")
        bronze_df = spark.read \
            .option("start-snapshot-id", last_snapshot_id) \
            .table(bronze_table_full)
    else:
        logger.info("-----> Initial Full Load.")
        bronze_df = spark.table(bronze_table_full)

    # Deduplication & Transformation
    bronze_df = bronze_df.dropDuplicates(["id"])
    
    logger.info("-----> Applying Transformation.")
    transformed_df = (
        bronze_df
        .withColumn("created_at_ts", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("payload_parsed", from_json(col("payload"), get_unified_payload_schema()))
        .select(*get_silver_projection())
    )

    # 4. Merge
    logger.info(f"-----> Merging data into {silver_table_full}")
    transformed_df.createOrReplaceTempView("batch_source")
    
    spark.sql(f"""
        MERGE INTO {silver_table_full} t USING batch_source s ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    # 5. Update Watermark
    if current_bronze_snapshot:
        set_bronze_watermark(spark, silver_table_full, current_bronze_snapshot)

    logger.info(f"-----> Pipeline Finished.")

def main():
    spark = None
    try:
        logger.info("-----> Starting Silver Batch Pipeline...")
        spark = SparkClient(app_name="bronze-to-silver", job_type="batch").get_session()
        
        run_process_bronze_to_silver(spark)
        
    except Exception as e:
        logger.error(f"-----> Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        logger.info("-----> Silver Pipeline completed.")

if __name__ == "__main__":
    main()
import os
import sys
import logging
from pathlib import Path

# Thêm PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from spark_client import SparkClient
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def query_gold_table():
    spark = None
    try:
        logger.info("-----> Initializing Spark Session to query Gold Layer...")
        spark = SparkClient(app_name="gold-data-explorer", job_type="batch").get_session()
        
        # Bảng Gold: {catalog}.{namespace}.{table}
        gold_table = f"{config.ICEBERG_CATALOG}.gold.gold_user_activity"
        
        logger.info(f"-----> Querying table: {gold_table}")

        # 1. Top 10 Users by activity score
        print("\n" + "="*80)
        print("TOP 10 MOST ACTIVE USERS")
        print("="*80)
        spark.sql(f"""
            SELECT actor_login, activity_date, total_activity_score, 
                   user_activity_level, user_specialization, total_commits
            FROM {gold_table}
            ORDER BY total_activity_score DESC
            LIMIT 10
        """).show(truncate=False)

        # 2. Activity status summary
        print("\n" + "="*80)
        print("USER ACTIVITY LEVEL SUMMARY")
        print("="*80)
        spark.sql(f"""
            SELECT user_activity_level, COUNT(*) as user_count, 
                   SUM(total_events) as total_events
            FROM {gold_table}
            GROUP BY user_activity_level
            ORDER BY total_events DESC
        """).show()

        # 3. Specialization summary
        print("\n" + "="*80)
        print("USER SPECIALIZATION DISTRIBUTION")
        print("="*80)
        spark.sql(f"""
            SELECT user_specialization, COUNT(*) as user_count
            FROM {gold_table}
            GROUP BY user_specialization
            ORDER BY user_count DESC
        """).show()

        # 4. Top 5 Repositories with most contributions
        print("\n" + "="*80)
        print("TOP 10 REPOSITORIES CONTRIBUTED TO")
        print("="*80)
        spark.sql(f"""
            SELECT top_repo, COUNT(DISTINCT actor_id) as contributor_count, 
                   SUM(total_events) as total_activity
            FROM {gold_table}
            GROUP BY top_repo
            ORDER BY total_activity DESC
            LIMIT 10
        """).show(truncate=False)

    except Exception as e:
        logger.error(f"-----> Query failed: {e}")
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    query_gold_table()

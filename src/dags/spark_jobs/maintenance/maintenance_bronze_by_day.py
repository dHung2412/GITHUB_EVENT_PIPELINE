"""
Bronze Table Maintenance Job - Daily
- Nén các file vào target 20,000 KB
- Chỉ compact nếu có ít nhất 5 file nhỏ
- Chỉ áp dụng cho 7 ngày gần nhất
"""

from iceberg_maintenance import IcebergMaintenance, get_table_path

def run_maintenance_bronze_by_day():
    m = IcebergMaintenance(get_table_path("bronze"), "Bronze-Maintenance-Daily")
    try:
        m.compact(sort_order="ingestion_timestamp ASC NULLS LAST")
    finally:
        m.stop()

if __name__ == "__main__":
    run_maintenance_bronze_by_day()

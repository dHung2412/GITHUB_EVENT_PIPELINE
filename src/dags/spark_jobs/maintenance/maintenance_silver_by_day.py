"""
Silver Table Maintenance Job - Daily
- Nén các file vào target 20,000 KB
- Chỉ compact nếu có ít nhất 2 file rác
- Strategy: sort (event_type, created_at) để tối ưu query
"""
from iceberg_maintenance import IcebergMaintenance, get_table_path

def run_maintenance_silver_by_day():
    m = IcebergMaintenance(get_table_path("silver"), "Silver-Maintenance-Daily")
    try:
        # Silver dùng min_files thấp hơn (2) để dọn rác triệt để hơn
        m.compact(
            sort_order="event_type ASC NULLS LAST, created_at ASC NULLS LAST",
            min_files=2
        )
    finally:
        m.stop()

if __name__ == "__main__":
    run_maintenance_silver_by_day()

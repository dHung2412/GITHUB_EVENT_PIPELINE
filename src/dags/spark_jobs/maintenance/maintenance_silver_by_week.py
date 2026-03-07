"""
Silver Table Maintenance Job - Weeklu
- Xóa snapshot cũ hơn 7 ngày
- Dọn rác (Orphan files)
"""
from iceberg_maintenance import IcebergMaintenance, get_table_path

def run_maintenance_silver_by_week():
    m = IcebergMaintenance(get_table_path("silver"), "Silver-Maintenance-Weekly")
    try:
        m.expire_snapshots()
        m.remove_orphan_files()
        m.rewrite_manifests()
    finally:
        m.stop()

if __name__ == "__main__":
    run_maintenance_silver_by_week()

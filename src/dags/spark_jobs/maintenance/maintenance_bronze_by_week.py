"""
Bronze Table Maintenance Job - Weekly
- Expire snapshots cũ (> 7 ngày)
- Remove orphan files (> 3 ngày)
- Rewrite position delete files
"""
from iceberg_maintenance import IcebergMaintenance, get_table_path

def run_maintenance_bronze_by_week():
    m = IcebergMaintenance(get_table_path("bronze"), "Bronze-Maintenance-Weekly")
    try:
        m.expire_snapshots(days_back=7, retain_last=5)
        m.remove_orphan_files(days_back=3)
        m.rewrite_manifests()
    finally:
        m.stop()

if __name__ == "__main__":
    run_maintenance_bronze_by_week()

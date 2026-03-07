# GITHUB_EVENT_PIPELINE

### _End-to-End Data Engineering Pipeline with Medallion Architecture & High Availability Ingestion_

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/dHung2412/DE_Scheduler)
[![Version](https://img.shields.io/badge/version-1.1.0-blue)](https://github.com/dHung2412/DE_Scheduler)
[![Tech Stack](https://img.shields.io/badge/stack-Airflow%20|%20Spark%20|%20MinIO%20|%20Kafka%20|%20Iceberg%20|%20Prometheus%20|%20Grafana-orange)](https://github.com/dHung2412/DE_Scheduler)

**GITHUB_EVENT_PIPELINE** là một hệ thống Data Pipeline toàn diện (End-to-End) được thiết kế để giải quyết các thách thức thực tế trong việc thu thập và xử lý dữ liệu lớn theo thời gian thực. Dự án không chỉ là một luồng ETL đơn thuần mà là một giải pháp hoàn chỉnh kết hợp giữa tính sẵn sàng cao, hiệu suất vượt trội và quản trị dữ liệu chặt chẽ.

---

## 🚀 Mục đích chính của dự án

Dự án nhấn mạnh vào việc giải quyết 4 bài toán cốt lõi trong Data Engineering:

1.  **Duy trì High Availability & Zero Data Loss**: Xây dựng một Ingestion Gateway bền bỉ với các lớp bảo vệ dữ liệu (Spill-to-disk, Multi-tier Fallback). Đảm bảo mọi sự kiện được thu thập thành công ngay cả khi hạ tầng lưu trữ chính gặp sự cố.
2.  **Tối ưu hóa Ingestion Performance**: Áp dụng kỹ thuật nén Avro Binary và Micro-batching ngay tại Application Level để giảm tải cho băng thông mạng và Kafka Broker, tối ưu hóa chi phí vận hành hạ tầng.
3.  **Quản trị dữ liệu theo Medallion Lakehouse**: Triển khai Apache Iceberg để biến Data Lake thành Lakehouse với các tính năng ACID Transactions, Schema Evolution và Time Travel. Dữ liệu được tổ chức lớp lang (Bronze, Silver, Gold) để duy trì tính minh bạch và tin cậy.
4.  **Tự động hóa & Giám sát toàn diện (Observability)**: Đồng bộ hóa quá trình xử lý bằng Airflow và giám sát sức khỏe hệ thống theo thời gian thực bằng Prometheus/Grafana, cho phép phát hiện và phản ứng nhanh với các điểm nghẽn (bottleneck).

---

## 1. Các tính năng chính (Features)

- **High-Performance Ingestion**: Hỗ trợ nạp dữ liệu đơn lẻ (`/collect-single`) hoặc nạp hàng loạt (`/collect-batch`) qua FastAPI, đạt tốc độ mô phỏng lên tới ~18,000 events/giây.
- **Zero Data Loss Architecture**:
  - Cơ chế **Spill-to-disk** (RAM -> JSONL) khi hàng đợi bị đầy.
  - Cơ chế **Multi-tier Fallback** (Kafka -> S3/MinIO -> Local Disk) đảm bảo dữ liệu luôn được bảo vệ ngay cả khi hạ tầng mạng gặp sự cố.
- **Reliable Serialization**: Sử dụng **Avro Binary** kết hợp với cơ chế **Data Cloning** khi đóng gói, đảm bảo dữ liệu gốc luôn sạch để phục vụ Retry logic.
- **Medallion Lakehouse Architecture**: Dữ liệu đi qua 3 lớp:
  - **Bronze**: Lưu trữ thô từ Kafka (Apache Iceberg).
  - **Silver**: Làm phẳng, chuẩn hóa và dọn dẹp dữ liệu.
  - **Gold**: Tính toán các Business Metrics cho báo cáo.
- **ACID & Time Travel**: Tận dụng triệt để sức mạnh của **Apache Iceberg** cho các thao tác Transactional và quản lý lịch sử dữ liệu.
- **Observability**: Giám sát sâu sắc hiệu suất ứng dụng qua **Prometheus** và **Grafana**, từ latency của API đến độ trễ của Kafka Producer.

---

## 2. Công nghệ sử dụng (Tech Stack)

- **Language**: Python, SQL
- **Orchestration**: Apache Airflow.
- **Computing Engine**: Apache Spark (Structured Streaming & Batch).
- **Messaging & Buffer**: Apache Kafka, asyncio.Queue.
- **Storage**: Apache Iceberg, MinIO (S3 Compatible).
- **API Framework**: FastAPI (Asynchronous logic).
- **Monitoring**: Prometheus, Grafana.
- **Hạ tầng**: Docker, Docker Compose.

---

## 3. Kiến trúc hệ thống (System Architecture)

[Chi tiết được trình bày bên trong các thư mục]

Dự án tuân thủ mô hình xử lý dữ liệu hiện đại, kết hợp giữa Streaming và Batch:

1.  **Collection Layer (API)**: FastAPI Service nhận dữ liệu JSON thô. Sử dụng **Shared Queue** làm vùng đệm để tách biệt việc nhận request và xử lý.
2.  **Ingestion Layer (Worker)**:
    - **Batching**: Worker gom metric từ Queue theo kích thước (1000 items) hoặc thời gian (5s).
    - **Packing**: Nén cả lô thành 1 file **Avro Binary** duy nhất.
    - **Reliability**: Gửi file Avro vào Kafka Topic `raw_metrics_avro`. Nếu lỗi, tự động ghi file xuống đĩa (Local Fallback) để Replay sau.
3.  **Bronze Layer**: Spark Structured Streaming đọc từ Kafka -> Giải mã Avro -> Ghi vào Iceberg Bronze tables.
4.  **Silver Layer**: Spark Batch (được Airflow trigger định kỳ) -> Parse JSON payload -> Làm phẳng cấu trúc dữ liệu.
5.  **Gold Layer**: dbt thực hiện các logic Business -> Tính toán metric -> Lưu trữ dữ liệu phân tích cuối cùng.

![Pipeline Architecture](./utils/pipeline.png)

---

## 4. Hướng dẫn cài đặt (Installation & Setup)

### Yêu cầu hệ thống

- Docker & Docker Compose.
- RAM tối thiểu: 8GB (Khuyến nghị 12GB+).

### Các bước cài đặt

1.  **Clone repository**:

    ```bash
    git clone https://github.com/dHung2412/DE_Scheduler.git
    cd DE_Scheduler
    ```

2.  **Cấu hình biến môi trường**:
    Tạo file `.env` từ file mẫu hoặc thay đổi các tham số

3.  **Khởi chạy hệ thống**:

```bash
docker-compose up -d
```

4.  **Kiểm tra trạng thái**:
    - Airflow UI: `http://localhost:8080` (admin/admin)
    - Spark UI: `http://localhost:8081`
    - MinIO Console: `http://localhost:9000` (admin/admin123)
    - Grafana UI: `http://localhost:3000` (admin/admin)

---

## 5. Cách sử dụng (Usage)

### Bước 1: Chuẩn bị dữ liệu (Data Ingestion)

1.  **Khởi chạy Collector Service** (FastAPI):

    ```bash
    cd src/metric_collector
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```

2.  **Khởi chạy luồng Streaming (Kafka to Bronze)**:
    Mở một terminal mới và chạy Spark Structured Streaming để nạp dữ liệu từ Kafka vào Iceberg:

    ```bash
    python -m dags.spark_jobs.kafka_bronze.process_kafka_to_bronze
    ```

3.  **Bắn dữ liệu giả lập (Simulator)**:
    ```bash
    # Chạy từ thư mục src
    python -m metric_collector.batch_injector
    ```

### Bước 2: Điều phối Pipeline chính (Airflow DAG)

1.  Truy cập Airflow UI tại `http://localhost:8080`.
2.  Kích hoạt DAG **`github_events_pipeline`** để thực hiện chu trình:
    - **Bronze to Silver (Spark)**: Tăng trưởng dữ liệu và làm phẳng log.
    - **Silver to Gold (dbt)**: Chạy các models dbt để tổng hợp dữ liệu Business.
    - **Data Quality (dbt test)**: Kiểm tra tính toàn vẹn và duy nhất của dữ liệu.

### Bước 3: Bảo trì hệ thống (Maintenance DAG)

Kích hoạt DAG **`iceberg_table_maintenance`** để tự động hóa các tác vụ:

- **Daily**: Compaction (gom file nhỏ) cho cả hai tầng Bronze và Silver.
- **Weekly**: Xóa Snapshots cũ và dọn dẹp Orphan files để tối ưu dung lượng MinIO.

---

## 6. Cấu trúc dự án (Project Structure)

```text
DE_Scheduler/
├── src/
│   ├── airflow/          # Dockerfile & Plugins cho môi trường Airflow
│   ├── dags/             # Airflow DAGs & Spark Jobs xử lý chính
│   │   ├── spark_jobs/   # Scripts PySpark (Kafka-to-Bronze, Bronze-to-Silver)
│   │   └── af_*.py       # Định nghĩa Workflow điều phối (Orchestration)
│   ├── dbt_project/      # Dự án dbt xử lý tầng Gold & Data Quality tests
│   ├── metric_collector/ # API Gateway (FastAPI) & Kafka Producer (Avro)
│   ├── monitoring/       # Cấu hình Prometheus & Grafana dashboard
│   ├── utils/            # Shared components (Avro Schema, SQL templates)
│   └── data/             # Dữ liệu sự kiện mẫu (JSONL) cho simulation
├── docker-compose.yaml   # Điều phối toàn bộ hạ tầng (Kafka, Spark, MinIO, Airflow,...)
└── .env                  # Quản lý tập trung các biến môi trường
```

---

## 7. Analytics & Monitoring

Cung cấp cái nhìn toàn diện về sức khỏe hệ thống và chất lượng dữ liệu:

- **Jupyter Notebook**: [http://localhost:8888](http://localhost:8888)  
  => Dùng để thăm dò dữ liệu (EDA) và truy vấn SQL trên các tầng **Bronze / Silver / Gold** thông qua Spark SQL.
- **Prometheus**: [http://localhost:9090](http://localhost:9090)  
  => Theo dõi các metrics kỹ thuật: Tốc độ Ingestion, Queue Status, Latency của Kafka Producer.
- **MinIO Console**: [http://localhost:9001](http://localhost:9001)  
  => Quản lý Storage Lakehouse, kiểm tra các tệp Iceberg (Parquet) và Metadata.
- **Airflow UI**: [http://localhost:8080](http://localhost:8080)  
  => Giám sát trạng thái vận hành của toàn bộ Pipeline và lịch sử thực thi.

---

_Dự án liên tục được cập nhật để áp dụng những kỹ thuật Data Engineering mới nhất._

import os
import time
import uuid
import json
import asyncio
import logging
from typing import List, Dict, Any
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Response, Body

from .utils.settings import settings
from .kafka_producer import run_kafka_producer_worker
from .shared_queue import shared_queue

try:
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from . import metrics
    METRICS_ENABLED = True
except ImportError:
    METRICS_ENABLED = False

# === LIFESPAN
@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_task
    logging.info("-----> [LIFESPAN] Bắt đầu khởi động Metric Collector Service")

    shared_queue.setup_signal_handlers()

    worker_task = asyncio.create_task(run_kafka_producer_worker(shared_queue.shutdown_event)) 

    yield

    logging.warning("-----> [LIFESPAN] Nhận tín hiệu dừng ứng dụng")
    if not shared_queue.shutdown_event.is_set():
        shared_queue.shutdown_event.set()
    
    if worker_task:        
        try:
            await worker_task
            logging.info("-----> [LIFESPAN] Worker đã dừng sạch sẽ")
        except Exception as e:
            logging.error(f"<----- [LIFESPAN] Lỗi khi dừng Worker: {e}")
    
# === INITIALIZATION
app = FastAPI(
    title="Metric Collector Service",
    description="Nhận metric, đưa vào queue để producer gửi đến Kafka.",
    version="1.0.0",
    lifespan=lifespan
)
# === HELPERS
async def enqueue_item(metric_data: Dict[str, Any]) -> str:
    event_id = str(uuid.uuid4())
    item = {"event_id": event_id, "metric": metric_data}
    await shared_queue.enqueue_metric(item)
    if METRICS_ENABLED:
        metrics.metrics_received_total.inc()
    return event_id

# === ENDPOINTS
@app.post("/collect-single")
async def collect_single(metric_data: Dict[str, Any] = Body(...)):
    try: 
        event_id = await enqueue_item(metric_data)
        return {"status": "Success", "event_id": event_id} 
    except Exception as e:
        logging.error(f"Error in collect_single: {e}")
        raise HTTPException(status_code=500, detail="Lỗi từ collect_single")

@app.post("/collect-batch")
async def collect_batch(metric_data: List[Dict[str, Any]] = Body(...)):
    if not isinstance(metric_data, list):
        raise HTTPException(status_code=400, detail="Dữ liệu phải là danh sách các Object")
    
    try:
        event_ids = []
        for metric_item in metric_data:
            eid = await enqueue_item(metric_item)
            event_ids.append(eid)

        return {
            "status": "Success",
            "record_processed": len(event_ids),
            "sample_event_id": event_ids[0] if event_ids else None
        }

    except Exception as e: 
        logging.error(f"Error in collect_batch: {e}")
        raise HTTPException(status_code=500, detail="Lỗi từ collect_batch")

@app.get("/health")
async def health_check():
    return {
        "status": "Healthy",
        "queue_size": shared_queue.metric_queue.qsize(),
        "max_size": settings.QUEUE_MAX_SIZE
    }

@app.get("/metrics")
async def prometheus_metrics():
    if not METRICS_ENABLED:
        raise HTTPException(status_code=503, detail="Metrics not available")
    shared_queue.check_queue_pressure()
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
import asyncio
import time
import uuid
import logging
from typing import List, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from .shared_queue import shared_queue
from .utils.settings import settings
from .utils.s3_handler import S3Handler
from .utils.helpers import serialize_batch_avro

try: from . import metrics; METRICS_ENABLED = True
except ImportError: METRICS_ENABLED = False

class KafkaProducerService:
    def __init__(self):
        self.producer = None
        self.s3_handler = S3Handler()
        self.worker_start_time = None
    
    async def run_worker(self, shutdown_event: asyncio.Event):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            compression_type="snappy",
            linger_ms=10 # đợi để gom thêm data vào 1 message
        )

        await self.producer.start()
        self.worker_start_time = time.time()
        logging.info("-----> [WORKER] Kafka Producer đã sẵn sàng bắn dữ liệu")

        try:
            while not shutdown_event.is_set():                
                batch_items = await self._collect_batch()
                if not batch_items: continue

                await self._handle_batch(batch_items)

        finally:
            logging.warning("-----> [WORKER] Đang dọn dẹp và dừng Producer")
            await self._drain_remaining()
            await self.producer.stop()

    # ===

    async def _collect_batch(self) -> List[dict]:
        batch = []

        try:
            item = await asyncio.wait_for(shared_queue.metric_queue.get(), timeout=1.0)
            batch_items.append(item)
        except (asyncio.TimeoutError, Exception): return []
        
        deadline = time.time() + settings.BATCH_MAX_TIME_S
        while len(batch) < settings.BATCH_MAX_SIZE and time.time() < deadline:
            try: 
                item = shared_queue.metric_queue.get_nowait()
                batch.append(item)
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.01)
        return batch

    async def _handle_batch(self, batch_items: List[dict]):
        batch_id = str(uuid.uuid4())[:8]
        metric_only = [item["metric"] for item in batch_items]

        try:
            serialize_data = await asyncio.to_thread(serialize_batch_avro, metric_only)
            
            success = await self._send_with_retry(serialize_data, batch_id)
            
            if not success:
                await self._fallback(serialize_data, batch_id)
        
        except Exception as e:
            logging.error(f"-----> [WORKER] Lỗi xử lý batch {batch_id}: {e}")
        finally:
            for _ in batch_items: shared_queue.metric_queue.task_done()
    
    async def _send_with_retry(self, b_data: bytes, b_id: str) -> bool:
        attempt = 0
        while attempt < settings.KAFKA_RETRY_MAX:
            try: 
                await self.producer.send_and_wait(settings.KAFKA_TOPIC, b_data)
                logging.info(f"-----> [KAFKA] Gửi thành công batch '{b_id}'({len(b_data)}) bytes")
                return True
            
            except KafkaError as e:
                attempt+=1
                logging.error(f"-----> [KAFKA] Thử lại {attempt}/{settings.KAFKA_RETRY_MAX} cho batch '{b_id}'")
                await asyncio.sleep(settings.KAFKA_RETRY_BACKOFF_BASE_S * attempt)
            except Exception as e:
                logging.exception(f"-----> Lỗi với Batch '{batch_id}' : {e}.")
                return None
        return False
    
    async def _fallback(self, b_data: bytes, b_id: str):
        logging.error(f"-----> [FALLBACK] Kafka thất bại, đang lưu lại batch '{b_id}'")
        success = await asyncio.to_thread(self.s3_handler.upload_data, b_data, f"fallback_{b_id}.avro")
        if not success:
            path = settings.KAFKA_FALLBACK_DIR / f"failed_{b_id}_{int(time.time())}.avro"
            with open(path, "wb") as f: f.write(b_data)

    async def _persist_to_local_disk(self, batch_bytes: bytes, batch_id: str):
        os.makedirs(settings.KAFKA_FALLBACK_DIR, exist_ok = True)
        
        file_name = os.path.join(
            settings.KAFKA_FALLBACK_DIR,
            f"failed_batch_{batch_id}_{int(time.time())}.avro"
        )
        
        await asyncio.to_thread(write_file_binary, file_name, batch_bytes)
        logging.warning(f"-----> Saved Fallback to local disk: {file_name}.")

    async def _drain_remaining(self):
        remaining = []
        
        while not shared_queue.metric_queue.empty():
            item = shared_queue.metric_queue.get_nowait()
            remaining.append(item["metric"])
        if remaining:
            logging.info(f"-----> [SHUTDOWN] Đang vớt nốt {len(remaining)} cuối cùng")
            b_data = serialize_batch_avro(remaining)
            await self._fallback(b_data, "shutdown_drain") 

kafka_producer = KafkaProducerService()
run_kafka_producer_worker = kafka_producer.run_worker
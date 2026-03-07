import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import List
from aiokafka import AIOKafkaProducer

from .utils.settings import settings
from .utils import serialize_batch_avro

class ReplayWorker:
    def __init__(self):
        self.overflow_dir = Path("data/queue_overflow")
        self.producer = None

    async def start_producer(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            compression_type="snappy",
            linger_ms=100
        )
        await self.producer.start()
        logging.info("Kafka Producer started for Replay.")

    async def stop_producer(self):
        if self.producer:
            await self.producer.stop()
            logging.info("Kafka Producer stopped.")

    async def process_file(self, filepath: Path):
        
        # 1. LOCK: Rename thành .processing
        processing_path = filepath.with_suffix('.processing')
        try:
            filepath.rename(processing_path)
            logging.info(f"--> [LOCK] Bắt đầu xử lý: {processing_path.name}")
        except FileNotFoundError:
            logging.warning(f"File {filepath} đã biến mất (có thể process khác đã xử lý).")
            return
        except Exception as e:
            logging.error(f"Không thể rename file {filepath}: {e}")
            return

        metrics_buffer = []
        total_records = 0
        success = True

        try:
            # 2. READ
            with open(processing_path, 'r', encoding='utf-8') as f:
                for line_idx, line in enumerate(f):
                    line = line.strip()
                    if not line: continue
                    
                    try:
                        record = json.loads(line)
                        # Extract metric từ record (record = {"event_id":..., "metric":...})
                        if "metric" in record:
                            metrics_buffer.append(record["metric"])
                    except json.JSONDecodeError:
                        logging.warning(f"Dòng {line_idx+1} bị lỗi JSON trong file {processing_path.name}. Skipping.")

                    # Gửi theo batch nhỏ để tránh RAM spike
                    if len(metrics_buffer) >= settings.BATCH_MAX_SIZE:
                        await self._send_batch(metrics_buffer, processing_path.name)
                        total_records += len(metrics_buffer)
                        metrics_buffer = [] # Reset buffer

            # Gửi nốt metrics còn lại
            if metrics_buffer:
                await self._send_batch(metrics_buffer, processing_path.name)
                total_records += len(metrics_buffer)

            # 3. CLEAN
            os.remove(processing_path)
            logging.info(f"--> [DONE] Đã replay {total_records} records. Xóa file {processing_path.name}")

        except Exception as e:
            logging.error(f"--> [FAIL] Lỗi khi xử lý file {processing_path.name}: {e}")
            success = False
            # Đổi tên thành .failed để review sau
            failed_path = processing_path.with_suffix('.failed')
            try:
                processing_path.rename(failed_path)
                logging.info(f"Đã đổi tên file lỗi thành: {failed_path.name}")
            except:
                pass

    async def _send_batch(self, metrics: List[dict], filename: str):
        """Helper: Serialize và gửi batch"""
        try:
            serialized_data = serialize_batch_avro(metrics)
            
            await self.producer.send_and_wait(settings.KAFKA_TOPIC, serialized_data)
            # logging.info(f"Sent chunk ({len(metrics)} items) from {filename}")
        except Exception as e:
            logging.error(f"Failed sending chunk to Kafka: {e}")
            raise e # Raise để trigger logic xử lý lỗi file

    async def run(self):
        """Main loop quét file"""
        if not self.overflow_dir.exists():
            logging.info(f"Thư mục {self.overflow_dir} không tồn tại. Nothing to replay.")
            return

        files = list(self.overflow_dir.glob("*.jsonl"))
        
        if not files:
            logging.info("Không tìm thấy file tràn (overflow) nào. Hệ thống ổn định!")
            return

        logging.info(f"Tìm thấy {len(files)} file cần replay. Bắt đầu...")
        
        await self.start_producer()
        try:
            for f in files:
                await self.process_file(f)
        finally:
            await self.stop_producer()

if __name__ == "__main__":
    try:
        worker = ReplayWorker()
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logging.info("Replay worker cancelled by user.")

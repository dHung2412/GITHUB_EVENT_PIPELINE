import os
import json
import time
import asyncio
import logging
from pathlib import Path

from .utils.settings import settings

try: from . import metrics; METRICS_ENABLED = True
except ImportError: METRICS_ENABLED = False


class SharedQueue:
    def __init__(self):
        self.metric_queue = asyncio.Queue(maxsize=settings.QUEUE_MAX_SIZE)
        self.shutdown_event = asyncio.Event()

        self.overflow_dir = Path("data/queue_overflow")
        self.overflow_dir.mkdir(parents=True, exist_ok=True)
        
        if METRICS_ENABLED:
            metrics.queue_capacity.set(settings.QUEUE_MAX_SIZE)

    async def enqueue_metric(self, item: dict) -> bool:
        try:
            self.metric_queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            await self._spill_to_disk(item)
            return False

    def check_queue_pressure(self):
        size = self.metric_queue.qsize()
        if METRICS_ENABLED:
            metrics.queue_size.set(size)
            metrics.queue_utilization.set((size / settings.QUEUE_MAX_SIZE) * 100)
        
        if size >= settings.QUEUE_WARNING_THRESHOLD:
            logging.warning(f"-----> [QUEUE ALERT] Áp lực cao: {size}/{settings.QUEUE_MAX_SIZE}")

    async def _spill_to_disk(self, item: dict):
        """Ghi dữ liệu ra đĩa khi hàng đợi đầy"""
        filename = self.overflow_dir / f"overflow_{time.strftime('%Y%m%d')}.jsonl"
        
        try:
            record = json.dumps(item, ensure_ascii=False)
            await asyncio.to_thread(self._write_append_sync, filename, record)
            logging.warning(f"<----- [QUEUE ALARM] Queue Full! Spilled EventID {item.get('event_id')} to {filename}")
        except Exception as e:
            logging.error(f"<----- [CRITICAL] Spill-to-disk failed: {e}")

    def _write_append_sync(self, filename, content):
        with open(filename, mode='a', encoding='utf-8') as f:
            f.write(content + "\n")

    # --- SIGNAL HANDLING 
    def setup_signal_handlers(self):
        import platform, signal
        if platform.system() != "Windows":
            try:
                loop = asyncio.get_running_loop()
                for s in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(s, lambda: self.shutdown_event.set())
            except Exception: pass

shared_queue = SharedQueue()
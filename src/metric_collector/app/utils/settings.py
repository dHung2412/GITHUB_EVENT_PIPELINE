import os
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Settings:
    def __init__(self):
        # ---
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
        self.KAFKA_RETRY_MAX = int(os.getenv("KAFKA_RETRY_MAX"))
        self.KAFKA_RETRY_BACKOFF_BASE_S = float(os.getenv("KAFKA_RETRY_BACKOFF_BASE_S"))
        self.KAFKA_FALLBACK_DIR = Path(os.getenv("KAFKA_FALLBACK_DIR"))

        # ---
        self.BATCH_MAX_SIZE = int(os.getenv("BATCH_MAX_SIZE"))
        self.BATCH_MAX_TIME_S = float(os.getenv("BATCH_MAX_TIME_S"))
        self.QUEUE_MAX_SIZE = int(os.getenv("QUEUE_MAX_SIZE"))
        self.QUEUE_PRESSURE_LOG_EVERY = int(os.getenv("QUEUE_PRESSURE_LOG_EVERY"))
        
        # ---
        _threshold = os.getenv("QUEUE_WARNING_THRESHOLD")
        self.QUEUE_WARNING_THRESHOLD = int(_threshold) if _threshold else int(self.QUEUE_MAX_SIZE * 0.8)

        # ---
        self.AVRO_SCHEMA_PATH = Path(os.getenv("AVRO_SCHEMA_PATH"))

settings = Settings()

import httpx
import asyncio
import json
import logging
import os
import time
from typing import List

# ---
API_ENDPOINT = "http://localhost:8000/collect-batch"
INPUT_FILE_PATH = r"D:\Project\Data_Engineering\DE_Scheduler\src\data\2015-03-01-17.json" 

# ---
CLIENT_BATCH_SIZE = 1000 # 1000 records vào 1 request HTTP
CONCURRENCY_LIMIT = 10 # 10 kết nối đồng thời

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

async def send_batch_to_api(client: httpx.AsyncClient, batch: List[dict], batch_id: int):
    try:
        response = await client.post(API_ENDPOINT, json=batch, timeout=30)
        if response.status_code == 200:
            logging.info(f"Batch #{batch_id:03} --- ({len(batch)} records) --- OK")
            return True
        return False
    except Exception as e:
        logging.error(f"Batch #{batch_id:03} --- Error: {e}")

async def main_injector():
    if not os.path.exists(INPUT_FILE_PATH):
        logging.error(f"Không tìm thấy: {INPUT_FILE_PATH}")
        return
    
    all_metrics = []
    with open(INPUT_FILE_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip(): all_metrics.append(json.loads(line))
    
    total = len(all_metrics)
    logging.info(f"{total} records sẽ được bắn")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tasks = []
    async with httpx.AsyncClient() as client:
        start_time = time.time()
        for i in range(0, total, CLIENT_BATCH_SIZE):
            batch_data = all_metrics[i : i + CLIENT_BATCH_SIZE]
            async def worker(b_id, b_data):
                async with semaphore: await send_batch_to_api(client, b_data, b_id)
            tasks.append(asyncio.create_task(worker(i // CLIENT_BATCH_SIZE + 1, batch_data)))

        await asyncio.gather(*tasks)
        end_time = time.time()
        
    logging.info(f"Hoàn tất! Tốc độ: {total / (end_time - start_time):.2f} events/sec")

if __name__ == "__main__":
    asyncio.run(main_injector())
import io
import json
import logging
from typing import List, Dict, Any
import fastavro
from .settings import settings

def _load_and_parse_schema():
    schema_path = settings.AVRO_SCHEMA_PATH
    logging.info(f"-----> [HELPERS] Đang chuẩn bị Avro schema: {schema_path}")

    try:
        if not schema_path.exists():
            raise FileNotFoundError(f"<----- [HELPERS] Không tìm thấy file schema: {schema_path}")
    
        with open(schema_path, "r", encoding='utf-8') as f:
            schema_dict = json.load(f)
   
        return fastavro.parse_schema(schema_dict)
    except Exception as e:
        logging.error(f"<----- [HELPERS] LỖI KHỞI TẠO SCHEMA: {e}")
        return None

PARSED_SCHEMA = _load_and_parse_schema()

def serialize_batch_avro(batch: list[dict]) -> bytes:
    if PARSED_SCHEMA is None:
        raise RuntimeError("<----- [HELPERS] Avro Schema chưa được tải thành công")
    
    processed_data = []
    for item in batch:
        record = item.copy() # Tách biệt dữ liệu gốc

        payload = record.get("payload")

        if payload is not None: 
            if not isinstance(payload, str):
                try: 
                    record["payload"] = json.dumps(payload, ensure_ascii=False)
                except Exception as e:
                    logging.warning(f"<----- [HELPERS] Lỗi parsed payload sang JSON: {e}")
        
        else:
            record["payload"] = None
        
        processed_data.append(record)

    try:
        with io.BytesIO() as fo:
            fastavro.writer(fo, PARSED_SCHEMA, batch)
            return fo.getvalue()
            
    except Exception as e:
        logging.exception(f"<----- [HELPERS] Error when serialize Avro: {e}")
        raise

def write_file_binary(filename: str, data : bytes):
    with open(filename, "wb") as f:
        f.write(data)
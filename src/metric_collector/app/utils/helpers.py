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
        # Tách biệt dữ liệu gốc, chỉ lấy các field schema hỗ trợ
        record = {
            "id": str(item.get("id", "")),
            "type": str(item.get("type", "UnknownEvent")),
            "public": bool(item.get("public", True)),
            "created_at": str(item.get("created_at", ""))
        }

        # 1. Handle Actor (Bắt buộc theo schema)
        actor = item.get("actor", {})
        if not actor: actor = {}
        record["actor"] = {
            "id": int(actor.get("id", 0)),
            "login": str(actor.get("login", "")),
            "gravatar_id": str(actor.get("gravatar_id", "")),
            "url": str(actor.get("url", "")),
            "avatar_url": str(actor.get("avatar_url", ""))
        }

        # 2. Handle Repo (Bắt buộc theo schema)
        repo = item.get("repo", {})
        if not repo: repo = {}
        record["repo"] = {
            "id": int(repo.get("id", 0)),
            "name": str(repo.get("name", "")),
            "url": str(repo.get("url", ""))
        }

        # 3. Handle Payload (Chuyển dict sang JSON string)
        payload = item.get("payload")
        if payload is not None: 
            if isinstance(payload, (dict, list)):
                record["payload"] = json.dumps(payload, ensure_ascii=False)
            else:
                record["payload"] = str(payload)
        else:
            record["payload"] = None
        
        processed_data.append(record)

    try:
        with io.BytesIO() as fo:
            # Sửa lỗi: dùng processed_data thay vì batch gốc
            fastavro.writer(fo, PARSED_SCHEMA, processed_data)
            return fo.getvalue()
            
    except Exception as e:
        logging.exception(f"<----- [HELPERS] Error when serialize Avro: {e}")
        raise

def write_file_binary(filename: str, data : bytes):
    with open(filename, "wb") as f:
        f.write(data)
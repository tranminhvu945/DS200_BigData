import base64
import json
import os
import threading
import time
from datetime import datetime, timezone

from fastapi import FastAPI
from kafka import KafkaConsumer
from pymongo import MongoClient, DESCENDING

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RESULT_TOPIC = os.getenv("RESULT_TOPIC", "detection_results")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "people_counting")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "detections")

RESULTS_DIR = "/app/results"
JSON_DIR = os.path.join(RESULTS_DIR, "json")
IMAGE_DIR = os.path.join(RESULTS_DIR, "images")

os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(IMAGE_DIR, exist_ok=True)

app = FastAPI(title="Storage Service")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

consumer_thread = None
stop_event = threading.Event()

# Lưu đúng 1 ảnh cho mỗi camera trong mỗi giây.
# Ví dụ key: cam_01_20260613_085304
saved_image_second_by_camera = set()


def parse_iso_time(value):
    if not value:
        return datetime.now(timezone.utc)

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


def get_frame_second_key(doc):
    camera_id = doc.get("camera_id", "unknown_camera")

    # Ưu tiên source_timestamp vì đây là thời điểm frame được lấy từ video/camera.
    # Nếu không có thì dùng processed_timestamp.
    ts = doc.get("source_timestamp") or doc.get("processed_timestamp")
    dt = parse_iso_time(ts)

    # Cắt xuống mức giây để bảo đảm 1 giây chỉ lưu 1 ảnh.
    dt = dt.replace(microsecond=0)

    second_str = dt.strftime("%Y%m%d_%H%M%S")
    return camera_id, second_str


def save_annotated_image(doc):
    image_b64 = doc.get("annotated_image")
    if not image_b64:
        return None

    camera_id, second_str = get_frame_second_key(doc)
    key = f"{camera_id}_{second_str}"

    # Nếu giây này của camera này đã lưu ảnh rồi thì bỏ qua.
    if key in saved_image_second_by_camera:
        return None

    saved_image_second_by_camera.add(key)

    image_filename = f"{key}.jpg"
    image_path = os.path.join(IMAGE_DIR, image_filename)

    image_bytes = base64.b64decode(image_b64)
    with open(image_path, "wb") as f:
        f.write(image_bytes)

    return image_path


def save_json_result(doc):
    frame_id = doc.get("frame_id", "unknown")
    json_filename = f"{frame_id}.json"
    json_path = os.path.join(JSON_DIR, json_filename)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)

    return json_path


def consume_loop():
    consumer = None

    while not stop_event.is_set():
        try:
            consumer = KafkaConsumer(
                RESULT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                group_id="storage-service",
                enable_auto_commit=True,
            )
            print("Connected to Kafka from storage-service")
            break
        except Exception as e:
            print(f"Kafka is not ready for storage-service: {e}")
            time.sleep(5)

    if consumer is None:
        return

    for msg in consumer:
        if stop_event.is_set():
            break

        doc = msg.value
        doc["stored_timestamp"] = datetime.now(timezone.utc).isoformat()

        # Lưu ảnh đại diện: 1 ảnh / 1 giây / 1 camera.
        image_path = save_annotated_image(doc)

        # Không lưu base64 ảnh vào MongoDB và JSON.
        doc_to_save = dict(doc)
        doc_to_save.pop("annotated_image", None)

        doc_to_save["image_path"] = image_path

        json_path = save_json_result(doc_to_save)
        doc_to_save["json_path"] = json_path

        # Ghi lại JSON lần nữa sau khi có json_path.
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(doc_to_save, f, ensure_ascii=False, indent=2)

        collection.insert_one(doc_to_save)


@app.on_event("startup")
def startup():
    global consumer_thread

    collection.create_index([("camera_id", 1), ("processed_timestamp", DESCENDING)])
    collection.create_index([("frame_id", 1)], unique=False)

    stop_event.clear()
    consumer_thread = threading.Thread(target=consume_loop, daemon=True)
    consumer_thread.start()


@app.on_event("shutdown")
def shutdown():
    stop_event.set()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "storage-service",
        "kafka": KAFKA_BOOTSTRAP_SERVERS,
        "topic": RESULT_TOPIC,
        "mongo_db": MONGO_DB,
        "mongo_collection": MONGO_COLLECTION,
        "image_saving_policy": "save 1 annotated image per second per camera",
    }


def clean_doc(doc):
    doc["_id"] = str(doc["_id"])
    return doc


@app.get("/detections/latest")
def latest(limit: int = 10):
    docs = collection.find().sort("processed_timestamp", DESCENDING).limit(limit)
    return [clean_doc(d) for d in docs]


@app.get("/detections/by-camera/{camera_id}")
def by_camera(camera_id: str, limit: int = 50):
    docs = (
        collection.find({"camera_id": camera_id})
        .sort("processed_timestamp", DESCENDING)
        .limit(limit)
    )
    return [clean_doc(d) for d in docs]


@app.get("/stats/person-count")
def person_count_stats(camera_id: str | None = None):
    match_stage = {}

    if camera_id:
        match_stage["camera_id"] = camera_id

    pipeline = []

    if match_stage:
        pipeline.append({"$match": match_stage})

    pipeline.extend(
        [
            {
                "$group": {
                    "_id": "$camera_id",
                    "total_frames": {"$sum": 1},
                    "avg_person_count": {"$avg": "$person_count"},
                    "max_person_count": {"$max": "$person_count"},
                    "min_person_count": {"$min": "$person_count"},
                }
            }
        ]
    )

    return list(collection.aggregate(pipeline))
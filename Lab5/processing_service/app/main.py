import base64
import json
import os
import threading
import time
from datetime import datetime, timezone

import cv2
import numpy as np
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer
from ultralytics import YOLO

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CAMERA_TOPIC = os.getenv("CAMERA_TOPIC", "camera_frames")
RESULT_TOPIC = os.getenv("RESULT_TOPIC", "detection_results")
YOLO_MODEL = os.getenv("YOLO_MODEL", "yolov8n.pt")
CONF_THRESHOLD = float(os.getenv("CONF_THRESHOLD", "0.35"))

app = FastAPI(title="Processing Service")

model = None
consumer_thread = None
stop_event = threading.Event()
stats = {
    "processed_frames": 0,
    "last_processing_time_ms": None,
    "last_person_count": None,
}


def load_model():
    global model
    if model is None:
        model = YOLO(YOLO_MODEL)
    return model

def decode_frame(image_base64):
    data = base64.b64decode(image_base64)
    arr = np.frombuffer(data, dtype=np.uint8)
    frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if frame is None:
        raise RuntimeError("Cannot decode image")
    return frame

def detect_people(frame):
    m = load_model()
    results = m.predict(
        source=frame,
        conf=CONF_THRESHOLD,
        classes=[0],
        verbose=False,
        device="cpu",
    )

    boxes = []
    for r in results:
        if r.boxes is None:
            continue
        for b in r.boxes:
            x1, y1, x2, y2 = b.xyxy[0].cpu().numpy().tolist()
            conf = float(b.conf[0].cpu().numpy())
            cls = int(b.cls[0].cpu().numpy())
            boxes.append({
                "x1": round(x1, 2),
                "y1": round(y1, 2),
                "x2": round(x2, 2),
                "y2": round(y2, 2),
                "confidence": round(conf, 4),
                "class_id": cls,
                "class_name": "person",
            })
    return boxes

def draw_boxes(frame, boxes):
    drawn = frame.copy()
    for box in boxes:
        x1 = int(box["x1"])
        y1 = int(box["y1"])
        x2 = int(box["x2"])
        y2 = int(box["y2"])
        conf = box["confidence"]
        label = f'person {conf:.2f}'

        cv2.rectangle(drawn, (x1, y1), (x2, y2), (0, 255, 0), 2)
        cv2.putText(
            drawn,
            label,
            (x1, max(y1 - 10, 0)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (0, 255, 0),
            2,
        )
    return drawn


def encode_jpg_base64(frame):
    ok, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
    if not ok:
        raise RuntimeError("Cannot encode annotated frame")
    return base64.b64encode(buffer).decode("utf-8")

def consume_loop():
    consumer = None
    producer = None

    while not stop_event.is_set():
        try:
            consumer = KafkaConsumer(
                CAMERA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                group_id="processing-service",
                enable_auto_commit=True,
            )

            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )

            print("Connected to Kafka from processing-service")
            break

        except Exception as e:
            print(f"Kafka is not ready for processing-service: {e}")
            time.sleep(5)

    if consumer is None or producer is None:
        return

    load_model()

    while not stop_event.is_set():
        for msg in consumer:
            if stop_event.is_set():
                break

            payload = msg.value
            start = time.time()

            try:
                frame = decode_frame(payload["image"])
                boxes = detect_people(frame)
                annotated_frame = draw_boxes(frame, boxes)
                annotated_image_base64 = encode_jpg_base64(annotated_frame)

                processing_time_ms = round((time.time() - start) * 1000, 2)

                result = {
                        "camera_id": payload.get("camera_id"),
                        "frame_id": payload.get("frame_id"),
                        "frame_index": payload.get("frame_index"),
                        "source_timestamp": payload.get("timestamp"),
                        "processed_timestamp": datetime.now(timezone.utc).isoformat(),
                        "person_count": len(boxes),
                        "boxes": boxes,
                        "processing_time_ms": processing_time_ms,
                        "model": YOLO_MODEL,
                        "annotated_image": annotated_image_base64,
                    }

                producer.send(RESULT_TOPIC, result)
                producer.flush()

                stats["processed_frames"] += 1
                stats["last_processing_time_ms"] = processing_time_ms
                stats["last_person_count"] = len(boxes)

            except Exception as e:
                print(f"Processing error: {e}")


@app.on_event("startup")
def startup():
    global consumer_thread
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
        "service": "processing-service",
        "kafka": KAFKA_BOOTSTRAP_SERVERS,
        "consume_topic": CAMERA_TOPIC,
        "produce_topic": RESULT_TOPIC,
        "model": YOLO_MODEL,
    }


@app.get("/stats")
def get_stats():
    return stats

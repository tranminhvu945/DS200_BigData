import base64
import os
import threading
import time
import uuid
from datetime import datetime, timezone

import cv2
from fastapi import FastAPI
from kafka import KafkaProducer
import json

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CAMERA_TOPIC = os.getenv("CAMERA_TOPIC", "camera_frames")

app = FastAPI(title="Camera Service")

producer = None
capture_thread = None
stop_event = threading.Event()


def get_producer():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
        )
    return producer


def encode_frame(frame):
    ok, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
    if not ok:
        raise RuntimeError("Cannot encode frame")
    return base64.b64encode(buffer).decode("utf-8")


def capture_loop(source: str, fps: float, camera_id: str):
    delay = 1.0 / max(fps, 0.1)
    cap_source = int(source) if str(source).isdigit() else source
    cap = cv2.VideoCapture(cap_source)

    if not cap.isOpened():
        print(f"Cannot open camera/video source: {source}")
        return

    p = get_producer()
    frame_index = 0

    while not stop_event.is_set():
        ret, frame = cap.read()

        if not ret:
            if not str(source).isdigit():
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                continue
            break

        frame_id = f"{camera_id}_{uuid.uuid4().hex[:12]}"
        payload = {
            "camera_id": camera_id,
            "frame_id": frame_id,
            "frame_index": frame_index,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "image": encode_frame(frame),
        }

        p.send(CAMERA_TOPIC, payload)
        p.flush()

        frame_index += 1
        time.sleep(delay)

    cap.release()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "camera-service",
        "kafka": KAFKA_BOOTSTRAP_SERVERS,
        "topic": CAMERA_TOPIC,
    }


@app.post("/start")
def start_camera(source: str = "0", fps: float = 1.0, camera_id: str = "cam_01"):
    global capture_thread

    if capture_thread and capture_thread.is_alive():
        return {"status": "already_running"}

    stop_event.clear()
    capture_thread = threading.Thread(
        target=capture_loop,
        args=(source, fps, camera_id),
        daemon=True,
    )
    capture_thread.start()

    return {
        "status": "started",
        "source": source,
        "fps": fps,
        "camera_id": camera_id,
    }


@app.post("/stop")
def stop_camera():
    stop_event.set()
    return {"status": "stopped"}

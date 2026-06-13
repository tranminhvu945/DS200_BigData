# DS200: Hệ thống đếm số lượng người trong camera.
**MSSV:** `23521819` - **Trần Minh Vũ**

---

Hệ thống đếm số lượng người hiện diện trong camera/video sử dụng kiến trúc xử lý dữ liệu dạng streaming. Hệ thống được xây dựng bằng các công nghệ:

* FastAPI
* Apache Kafka
* MongoDB
* YOLOv8n
* OpenCV
* Docker Compose

Do giới hạn tài nguyên phần cứng, toàn bộ hệ thống được triển khai trên một server CPU duy nhất bằng Docker Compose. Tuy nhiên, hệ thống vẫn được tách thành nhiều service độc lập, bao gồm Camera Service, Processing Service và Storage Service. Các service giao tiếp bất đồng bộ thông qua Kafka, phù hợp với ngữ cảnh xử lý dữ liệu lớn dạng streaming.


## 1. Mô tả bài toán

Bài toán yêu cầu xây dựng hệ thống đếm số lượng người trong một camera. Hệ thống nhận các khung hình từ camera/video, thực hiện nhận diện người trong từng khung hình, trả về bounding box của các đối tượng được phát hiện, sau đó lưu trữ kết quả để truy vấn và đánh giá.

Kết quả đầu ra gồm:

* Số lượng người trong từng frame.
* Danh sách bounding boxes của các đối tượng `person`.
* File JSON chứa metadata detection.
* Ảnh đã được vẽ bounding box.
* Thống kê số lượng người theo camera.

## 2. Luồng xử lý:

```text
Video/Camera
    ↓
Camera Service
    ↓
Kafka topic: camera_frames
    ↓
Processing Service - YOLOv8n
    ↓
Kafka topic: detection_results
    ↓
Storage Service
    ↓
MongoDB + results/json + results/images
```


## 3. Vai trò của từng service

### 3.1. Camera Service

Camera Service có nhiệm vụ:

* Đọc dữ liệu từ webcam hoặc video file.
* Trích xuất frame theo tốc độ FPS cấu hình.
* Encode frame thành JPEG base64.
* Gửi frame vào Kafka topic `camera_frames`.

### 3.2. Processing Service

Processing Service có nhiệm vụ:

* Nhận frame từ Kafka topic `camera_frames`.
* Decode ảnh từ base64.
* Sử dụng YOLOv8n để nhận diện người.
* Chỉ giữ lại đối tượng thuộc class `person`.
* Trả về số lượng người và bounding boxes.
* Vẽ bounding box lên frame.
* Gửi kết quả sang Kafka topic `detection_results`.

### 3.3. Storage Service

Storage Service có nhiệm vụ:

* Nhận kết quả detection từ Kafka topic `detection_results`.
* Lưu metadata vào MongoDB.
* Lưu JSON kết quả của từng frame vào thư mục `results/json`.
* Lưu ảnh đã vẽ bounding box vào thư mục `results/images`.

Để tránh lưu quá nhiều ảnh, hệ thống được thiết lập:

```text
Detection: xử lý liên tục theo FPS cấu hình
JSON: lưu kết quả cho mọi frame
Image: chỉ lưu 1 ảnh đại diện mỗi giây cho mỗi camera
```


## 4. Cấu trúc thư mục

```text
DS200_BigData/
├── camera_service/
│   ├── app/
│   │   └── main.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── processing_service/
│   ├── app/
│   │   └── main.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── storage_service/
│   ├── app/
│   │   └── main.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── data/
│   └── output.mp4
│
├── results/
│   ├── json/
│   ├── images/
│   ├── screenshots/
│   ├── latest_detection.json
│   └── person_count_stats.json
│
├── docker-compose.yml
├── README.md
└── .gitignore
```


## 5. Cách chạy hệ thống

### Bước 1: Vào thư mục project

```bash
cd ~/DS200_BigData
```

### Bước 2: Kiểm tra video đầu vào

Video đầu vào nên được đặt tại:

```text
data/output.mp4
```

### Bước 3: Dừng hệ thống cũ và xóa dữ liệu cũ

```bash
docker compose down -v
```

Xóa kết quả cũ:

```bash
sudo rm -rf results/json/*
sudo rm -rf results/images/*
sudo rm -rf results/screenshots/*
sudo rm -f results/latest_detection.json
sudo rm -f results/person_count_stats.json
```

Tạo lại thư mục kết quả:

```bash
mkdir -p results/json results/images results/screenshots
```

### Bước 4: Build hệ thống

```bash
docker compose build
```

### Bước 5: Chạy hệ thống

```bash
docker compose up
```

Giữ terminal này chạy.


## 6. Kiểm tra hệ thống

Mở terminal thứ hai và chạy:

```bash
cd ~/DS200_BigData
```

Kiểm tra container:

```bash
docker ps
```

Kiểm tra API:

```bash
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

Nếu các service đều trả về `"status": "ok"` thì hệ thống đã khởi động thành công.

## 7. Chạy video để detect người

Chạy video:

```bash
curl -X POST "http://localhost:8001/start?source=/app/data/output.mp4&fps=5"
```

Với video dài khoảng 1 phút, chờ 60 giây rồi dừng:

```bash
sleep 60
curl -X POST "http://localhost:8001/stop"
```

Dừng toàn bộ container:

```bash
docker compose down
```

## 8. Kết quả thực nghiệm

Hệ thống đã chạy thử trên video đầu vào `output.mp4`. Camera Service gửi frame với tốc độ `fps=5`. Processing Service sử dụng YOLOv8n để phát hiện người trong từng frame. Storage Service lưu metadata detection vào MongoDB, lưu JSON kết quả từng frame vào `results/json`, và lưu ảnh bounding box đại diện theo từng giây vào `results/images`.

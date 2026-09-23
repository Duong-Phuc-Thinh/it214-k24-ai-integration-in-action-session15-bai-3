# Choreography Saga với Apache Kafka

Bài tập này minh họa kiến trúc **Choreography Saga** (Mô hình Vũ điệu) cho nghiệp vụ Đặt vé sự kiện âm nhạc (Concert Booking) thông qua 3 dịch vụ:
1. **ConcertBookingService**: Tiếp nhận yêu cầu từ khách hàng và bắt đầu Saga.
2. **SeatAssignmentService**: Xác nhận và gán vị trí ghế ngồi cho khách hàng.
3. **NotificationService**: Gửi thông báo xác nhận thành công cuối cùng đến khách hàng.

Sự phối hợp giữa các dịch vụ được thực hiện hoàn toàn bất đồng bộ thông qua các Kafka topics (`concert-events` và `seat-events`) mà không có bất kỳ lệnh gọi REST API hoặc giao tiếp trực tiếp nào giữa chúng. 

Để đảm bảo tính liên kết và khả năng truy vết (traceability) xuyên suốt hệ thống phân tán, mã giao dịch **Correlation ID** được truyền nguyên vẹn từ dịch vụ đầu tiên tới dịch vụ cuối cùng.

---

## 1. Luồng Sự Kiện (Choreography Saga)

```
[ConcertBookingService] (Khởi tạo booking)
       │
       ▼ Publish to 'concert-events' topic
[SeatAssignmentService] (Nhận thông tin, giữ chỗ & sinh mã ghế)
       │
       ▼ Publish to 'seat-events' topic
[NotificationService] (Nhận kết quả và gửi Email thông báo)
```

---

## 2. Cách Chạy Chương Trình

Chương trình này hỗ trợ cả 2 chế độ: 
1. **In-Memory Simulation**: Mô phỏng hoạt động trực tiếp ngay trên Python để kiểm tra luồng log mà không cần cài đặt Kafka.
2. **Real Kafka mode**: Kết nối và chạy thực tế với Kafka Broker (sử dụng Docker Compose).

### Chế độ Mô Phỏng (Chạy ngay lập tức không cần Kafka)
Chạy file `main.py` trực tiếp bằng Python:
```bash
python main.py
```
**Output mong đợi:**
```text
=== CHOREOGRAPHY SAGA DEMONSTRATION (IN-MEMORY SIMULATION) ===
Starting background services...

--- Publishing Input Event ---

[SeatService] Received event with correlationId: CONCERT-2024-999
[SeatService] Seat reserved successfully for correlationId: CONCERT-2024-999
[SeatService] Publishing SeatReserved event with correlationId: CONCERT-2024-999 to topic: seat-events
[NotifyService] Received confirmation for correlationId: CONCERT-2024-999 - Sending email to nguyenvanA@email.com

================ DEMONSTRATION FINISHED ================
```

### Chế độ chạy thực tế với Kafka Broker

**Bước 1**: Khởi động Kafka & Zookeeper bằng Docker Compose:
```bash
docker-compose up -d
```

**Bước 2**: Cài đặt thư viện dependencies:
```bash
pip install -r requirements.txt
```

**Bước 3**: Chạy chương trình với cờ `--kafka` để kết nối tới Broker:
```bash
python main.py --kafka
```
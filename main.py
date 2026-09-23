import time
import json
import threading
import sys

# Class mô phỏng Broker tin nhắn khi không có Kafka
class SimpleMessageBroker:
    def __init__(self):
        self.topics = {}
        self.lock = threading.Lock()

    def publish(self, topic_name, message):
        with self.lock:
            if topic_name in self.topics:
                for callback in self.topics[topic_name]:
                    # Chạy callback bất đồng bộ giả lập mạng phân tán
                    threading.Thread(target=callback, args=(message,), daemon=True).start()

    def subscribe(self, topic_name, callback):
        with self.lock:
            if topic_name not in self.topics:
                self.topics[topic_name] = []
            self.topics[topic_name].append(callback)

# Khởi tạo instance Broker giả lập toàn cục
mock_broker = SimpleMessageBroker()
KAFKA_AVAILABLE = False
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

try:
    from kafka import KafkaProducer, KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    pass

# ---------------- DETAILED SERVICES IMPLEMENTATIONS ----------------

class ConcertBookingService:
    """
    Dịch vụ ConcertBookingService:
    Nhận yêu cầu đặt vé của khách hàng và khởi tạo sự kiện Saga đầu tiên
    """
    def __init__(self, use_kafka=False):
        self.use_kafka = use_kafka and KAFKA_AVAILABLE
        if self.use_kafka:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        else:
            self.producer = None

    def create_booking(self, correlation_id, concert_code, customer_email, ticket_quantity):
        event = {
            "correlationId": correlation_id,
            "concertCode": concert_code,
            "customerEmail": customer_email,
            "ticketQuantity": ticket_quantity
        }
        if self.use_kafka and self.producer:
            self.producer.send('concert-events', value=event)
            self.producer.flush()
        else:
            mock_broker.publish('concert-events', event)


class SeatAssignmentService:
    """
    Dịch vụ SeatAssignmentService:
    Lắng nghe event từ topic 'concert-events'.
    Trích xuất 'correlationId' để xử lý giữ chỗ và chuyển tiếp qua topic 'seat-events'.
    """
    def __init__(self, use_kafka=False):
        self.use_kafka = use_kafka and KAFKA_AVAILABLE
        if self.use_kafka:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        else:
            self.producer = None

    def reserve_seat(self, event):
        # Mô phỏng quá trình ghi nhận và giữ chỗ trong Database
        time.sleep(0.3)

    def handle_event(self, event):
        correlation_id = event.get("correlationId")
        customer_email = event.get("customerEmail")

        # Ghi log đúng định dạng yêu cầu
        print(f"[SeatService] Received event with correlationId: {correlation_id}")
        
        # Tiến hành lưu database/giữ chỗ
        self.reserve_seat(event)
        print(f"[SeatService] Seat reserved successfully for correlationId: {correlation_id}")

        # Tạo sự kiện tiếp theo trong chuỗi Saga, giữ nguyên vẹn Correlation ID
        seat_event = {
            "correlationId": correlation_id,
            "customerEmail": customer_email
        }

        print(f"[SeatService] Publishing SeatReserved event with correlationId: {correlation_id} to topic: seat-events")
        
        if self.use_kafka and self.producer:
            self.producer.send('seat-events', value=seat_event)
            self.producer.flush()
        else:
            mock_broker.publish('seat-events', seat_event)

    def start(self):
        if self.use_kafka:
            consumer = KafkaConsumer(
                'concert-events',
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id='seat-assignment-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest'
            )
            for message in consumer:
                self.handle_event(message.value)
        else:
            mock_broker.subscribe('concert-events', self.handle_event)


class NotificationService:
    """
    Dịch vụ NotificationService:
    Lắng nghe event 'seat-events' để thực hiện gửi mail thông báo cho khách hàng
    """
    def __init__(self, use_kafka=False):
        self.use_kafka = use_kafka and KAFKA_AVAILABLE

    def handle_event(self, event):
        correlation_id = event.get("correlationId")
        email = event.get("customerEmail")
        print(f"[NotifyService] Received confirmation for correlationId: {correlation_id} - Sending email to {email}")

    def start(self):
        if self.use_kafka:
            consumer = KafkaConsumer(
                'seat-events',
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id='notification-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest'
            )
            for message in consumer:
                self.handle_event(message.value)
        else:
            mock_broker.subscribe('seat-events', self.handle_event)


# ---------------- RUNNER SCRIPT ----------------

def run_simulation():
    print("=== CHOREOGRAPHY SAGA DEMONSTRATION (IN-MEMORY SIMULATION) ===")
    print("Starting background services...")

    seat_service = SeatAssignmentService(use_kafka=False)
    notify_service = NotificationService(use_kafka=False)
    booking_service = ConcertBookingService(use_kafka=False)

    # Khởi động listeners
    seat_service.start()
    notify_service.start()

    time.sleep(0.5)

    print("\n--- Publishing Input Event ---\n")
    booking_service.create_booking(
        correlation_id="CONCERT-2024-999",
        concert_code="LIVE-HCM-2024",
        customer_email="nguyenvanA@email.com",
        ticket_quantity=3
    )

    # Đợi xử lý bất đồng bộ in log hoàn tất
    time.sleep(1.5)
    print("\n================ DEMONSTRATION FINISHED ================")


def run_real_kafka():
    if not KAFKA_AVAILABLE:
        print("Lỗi: Thư viện 'kafka-python' chưa được cài đặt. Hãy cài qua 'pip install -r requirements.txt'")
        sys.exit(1)
        
    print("=== RUNNING WITH REAL APACHE KAFKA ===")
    print("Starting services threads...")

    seat_service = SeatAssignmentService(use_kafka=True)
    notify_service = NotificationService(use_kafka=True)

    t1 = threading.Thread(target=seat_service.start, daemon=True)
    t2 = threading.Thread(target=notify_service.start, daemon=True)
    
    t1.start()
    t2.start()
    
    time.sleep(2)  # Đợi consumer kết nối thành công vào broker

    booking_service = ConcertBookingService(use_kafka=True)
    print("\nPublishing message to topic 'concert-events'...")
    booking_service.create_booking(
        correlation_id="CONCERT-2024-999",
        concert_code="LIVE-HCM-2024",
        customer_email="nguyenvanA@email.com",
        ticket_quantity=3
    )

    print("Giữ tiến trình sống trong 5 giây để xem logs xử lý sự kiện. Nhấn Ctrl+C để thoát...")
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--kafka':
        run_real_kafka()
    else:
        run_simulation()
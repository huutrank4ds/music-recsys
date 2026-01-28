# app/core/kafka_client.py
import socket
import os
from typing import Optional
from confluent_kafka import Producer #type: ignore
from confluent_kafka.admin import AdminClient, NewTopic #type: ignore
from common.logger import get_logger

logger = get_logger("Core KafkaClient")

class KafkaClient:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        # Type Hint: Khai báo rõ biến này có thể là Producer hoặc None
        self._producer: Optional[Producer] = None

    def start(self):
        """Khởi tạo Producer"""
        try:
            # Nếu đã có producer rồi thì không tạo lại
            if self._producer is not None:
                return

            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': socket.gethostname(),
                'acks': '1',
            }
            self._producer = Producer(conf)
            logger.info(f"Kafka Producer kết nối đến {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Kafka Producer: {e}")
            # Đảm bảo reset về None nếu khởi tạo thất bại
            self._producer = None

    def stop(self):
        """Flush và đóng kết nối"""
        if self._producer:
            logger.info("Đang tắt Kafka Producer...")
            self._producer.flush(timeout=5)
            # Opsional: Reset về None sau khi stop
            # self._producer = None 

    def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
        """Hàm tiện ích để tạo topic"""
        try:
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
            fs = admin_client.create_topics([
                NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            ])
            
            # Chờ kết quả
            for topic, f in fs.items():
                try:
                    f.result() # Block wait
                    logger.info(f"Topic '{topic}' được tạo thành công.")
                except Exception as e:
                    # Mã lỗi 36 là TopicAlreadyExists
                    if "TOPIC_ALREADY_EXISTS" in str(e) or getattr(e, 'code', None) == 36:
                        logger.info(f"Topic '{topic}' đã tồn tại.")
                    else:
                        logger.error(f"Lỗi khi tạo topic '{topic}': {e}")
        except Exception as e:
            logger.error(f"Lỗi AdminClient: {e}")
    def produce(self, topic: str, value: bytes):
        """Gửi message (Wrapper)"""
        
        # 1. Lazy Load: Nếu chưa có producer thì thử khởi tạo
        if self._producer is None:
            self.start()
        
        # 2. TYPE GUARD: Kiểm tra lại một lần nữa.
        # Nếu start() thất bại (do lỗi mạng...), _producer vẫn sẽ là None.
        # Dòng này giúp Pylance hiểu: "Nếu code chạy qua dòng này, _producer chắc chắn không phải None"
        if self._producer is None:
            logger.error("Producer chưa sẵn sàng (Init failed). Bỏ qua tin nhắn.")
            return

        # Hàm callback để log lỗi delivery (nếu có)
        def delivery_report(err, msg):
            if err is not None:
                logger.error(f"Lỗi khi gửi message: {err}")

        try:
            # Lúc này Pylance đã yên tâm self._producer là object Producer thật
            self._producer.produce(topic, value, callback=delivery_report)
            self._producer.poll(0) # Trigger gửi ngay
        except Exception as e:
             logger.error(f"Exception khi produce: {e}")

# Singleton Instance cho Core
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_manager = KafkaClient(BOOTSTRAP_SERVERS)
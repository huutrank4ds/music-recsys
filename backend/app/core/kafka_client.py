# app/core/kafka_client.py
import socket
import os
from typing import Optional
from confluent_kafka import Producer #type: ignore
from confluent_kafka.admin import AdminClient, NewTopic #type: ignore
from common.logger import get_logger
import config as cfg

logger = get_logger("Core KafkaClient")

class KafkaClient:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._producer: Optional[Producer] = None
        self._counter = 0

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
                'linger.ms': 5,             
                'batch.size': 16384,
                'queue.buffering.max.messages': 50000,
                'queue.buffering.max.kbytes': 512000, # 512MB
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
            self._producer = None 

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
                    f.result()
                    logger.info(f"Topic '{topic}' được tạo thành công.")
                except Exception as e:
                    if "TOPIC_ALREADY_EXISTS" in str(e) or getattr(e, 'code', None) == 36:
                        logger.info(f"Topic '{topic}' đã tồn tại.")
                    else:
                        logger.error(f"Lỗi khi tạo topic '{topic}': {e}")
        except Exception as e:
            logger.error(f"Lỗi AdminClient: {e}")

    def produce(self, topic: Optional[str], value: bytes):
        """Gửi message (Wrapper)"""
        
        # Chưa có producer thì khởi tạo
        if self._producer is None:
            self.start()
        
        # Kiểm tra lại lần nữa 
        if self._producer is None:
            logger.error("Producer chưa sẵn sàng (Init failed). Bỏ qua tin nhắn.")
            return

        # Hàm callback để log lỗi delivery (nếu có)
        def delivery_report(err, msg):
            if err is not None:
                logger.error(f"Lỗi khi gửi message: {err}")

        try:
            self._producer.produce(topic, value, callback=delivery_report)
            self._counter += 1
            if self._counter % 100 == 0:
                logger.info(f"Đã gửi {self._counter} messages đến topic '{topic}'")
                self._producer.poll(0)  # Xử lý callback
                self._counter = 0
        except BufferError:
            logger.warning("Bộ đệm đầy, đang tạm dừng 1 giây...")
            self._producer.poll(1)
            self._producer.produce(topic, value, callback=delivery_report)

# Singleton Instance cho Core
kafka_manager = KafkaClient(cfg.KAFKA_BOOTSTRAP_SERVERS) #type: ignore
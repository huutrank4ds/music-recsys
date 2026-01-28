# app/services/logging_service.py
import os
import json
import time
from common.logger import get_logger
from common.schemas.log_schemas import UserLogRequest, get_spark_msg
from app.core.kafka_client import kafka_manager # Import từ Core
import config as cfg

logger = get_logger("Logging Service")

class LoggingService:
    def __init__(self):
        self.topic = cfg.KAFKA_TOPIC
        self.partitions = cfg.KAFKA_NUM_PARTITIONS

    def initialize(self):
        """
        Nghiệp vụ khởi động: Yêu cầu Core tạo topic music_log
        """
        kafka_manager.start()
        kafka_manager.create_topic(self.topic, num_partitions=self.partitions)

    def send_log(self, log_data: UserLogRequest):
        """
        Nghiệp vụ: Convert Pydantic -> Spark JSON Schema -> Gửi Kafka
        """
        # 1. Mapping Logic
        spark_msg = get_spark_msg(log_data)
        # 2. Gửi thông qua Core Manager
        kafka_manager.produce(self.topic, json.dumps(spark_msg).encode('utf-8'))
        
        return spark_msg

# Singleton Service
logging_service = LoggingService()
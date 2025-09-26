import traceback
import pika
import json
from route_events import LRSRoute
import base64
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Literal, List
from logger import setup_logger, get_job_logger
from handler import (
    PayloadSMD, 
    RNIValidation, 
    IRIValidation, 
    SMDValidationHandler, 
    PCIValidation,
    DefectValidation
)
from typing import Dict


load_dotenv(os.path.dirname(__file__) + '/.env')

RMQ_HOST = os.getenv('RMQ_HOST')
RMQ_PORT = os.getenv('RMQ_PORT')
WRITE_VERIFIED_DATA = int(os.getenv('WRITE_VERIFIED_DATA'))

def generate_generic_event(job_id: str, event_type: Literal['executed', 'failed']) -> str:
    """
    Generate Job Executed event.
    """
    payload = {
        "job_id": job_id,  # The Job ID
        "occurred_at": int(datetime.now().timestamp()*1000),  # UNIX timestamp in miliseconds
    }

    envelope = {
        "type": event_type,
        "payload": payload
    }

    return json.dumps(envelope)


worker_logger = setup_logger("system")

if WRITE_VERIFIED_DATA:
    worker_logger.warning("verified data will be written to GeoDatabase.")
else:
    worker_logger.warning("verified data will NOT be written to GeoDatabase.")

class ValidationWorker:
    def __init__(self):
        self._rmq_url = f"amqp://{RMQ_HOST}:{RMQ_PORT}"
        self._rmq_conn = None
        self._rmq_channel = None
        self.job_queue = "validation_queue"
        self.job_event_queue = "job_event_queue"
        self._handler: Dict[str, SMDValidationHandler] = {}  # Empty dicitionary for handler class

        # Create handler for SMD
        self._smd_supported_data_type = ['ROUGHNESS', 'RNI', 'PCI', 'DEFECTS']  # Please update if more handlers are added.
        self._handler['RNI'] = RNIValidation
        self._handler['ROUGHNESS'] = IRIValidation
        self._handler['PCI'] = PCIValidation
        self._handler['DEFECTS'] = DefectValidation

    def connect(self):
        worker_logger.info(f"connecting to RabbitMQ on {self._rmq_url}")

        self._rmq_conn = pika.BlockingConnection(
            pika.URLParameters(self._rmq_url)
        )
        
        worker_logger.info(f"connected to RabbitMQ")
        self._rmq_channel = self._rmq_conn.channel()

        # Declare queues
        self._rmq_channel.queue_declare(queue=self.job_queue, durable=True)
        self._rmq_channel.queue_declare(queue=self.job_event_queue, durable=True)

        self._rmq_channel.basic_qos(prefetch_count=1)

    def start_listening(self):
        try:
            self.connect()

            worker_logger.info(f"start listening on {self.job_queue}")
            self._rmq_channel.basic_consume(
                queue=self.job_queue,
                on_message_callback=self.handle_job
            )

            self._rmq_channel.start_consuming()
        
        except KeyboardInterrupt:
            self._rmq_channel.stop_consuming()
            raise KeyboardInterrupt
        
    def publish_executed_event(self, job_id: str):
        """
        Publish Job Executed event.
        """
        self._rmq_channel.basic_publish(
            "",
            routing_key=self.job_event_queue,
            body=generate_generic_event(job_id, 'executed'),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        return
    
    def publish_failed_event(self, job_id: str):
        """
        Publish Job Failed event.
        """
        self._rmq_channel.basic_publish(
            "",
            routing_key=self.job_event_queue,
            body=generate_generic_event(job_id, 'failed'),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        return
    
    def smd_validate(self, data_type: str, payload: PayloadSMD, job_id: str, validate: bool=True)->str:
        """
        SMD validation handler
        """
        check = self._handler[data_type](
            payload,
            job_id,
            validate
        )

        self.publish_executed_event(job_id)

        return check.validate()

    def handle_job(self, ch, methods, properties, body):
        try:
            job_data = json.loads(body.decode('utf-8'))

            if type(job_data) is str:
                job_data = json.loads(job_data)
            
            job_id = job_data.get("job_id")
            data_type = job_data.get("data_type")
            validate = job_data.get("validate")

            payload_str = base64.b64decode(job_data.get("details"))
            job_logger = get_job_logger(job_id)

            if data_type in self._smd_supported_data_type:
                payload = PayloadSMD(**json.loads(payload_str))
                job_logger.info(f"processing {data_type} validation, validate: {validate}")
                event = self.smd_validate(data_type, payload, job_id, validate)
                job_logger.info(f"finished executing {data_type} validation.")
            else:
                job_logger.warning(f"{data_type} is unhandled")  # Temporary, just for the lulz
                return

            ch.basic_ack(methods.delivery_tag)

            self._rmq_channel.basic_publish(
                "",
                routing_key=self.job_event_queue,
                body=event,
                properties=pika.BasicProperties(delivery_mode=2)
            )

            job_logger.info("job succeeded event published.")
 
        except Exception:
            trace = traceback.format_exc()
            job_logger.error(trace)
            self.publish_failed_event(job_id)
            ch.basic_ack(methods.delivery_tag)


if __name__ == '__main__':
    worker = ValidationWorker()
    worker.start_listening()
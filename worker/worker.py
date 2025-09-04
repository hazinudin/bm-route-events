import traceback
import pika
import json
from route_events_service import (
    BridgeMasterValidation,
    BridgeInventoryValidation,
    RouteRNIValidation,
    RouteRoughnessValidation,
    RouteDefectsValidation,
    RoutePCIValidation,
)
from route_events import LRSRoute
import base64
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Literal, List
from logger import setup_logger, get_job_logger


TEST_MSG = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. In semper vitae sem sit amet lobortis. Morbi vulputate ut tellus eu mattis. Mauris eget tellus sit amet libero pretium dictum vel ac tortor. Ut efficitur lectus sapien, nec elementum ante gravida consectetur. Donec sagittis eu velit quis vulputate. Suspendisse viverra odio malesuada lobortis elementum. Suspendisse sed tortor dui. Proin ac euismod diam, in aliquet justo."
load_dotenv(os.path.dirname(__file__) + '/.env')

RMQ_HOST = os.getenv('RMQ_HOST')
RMQ_PORT = os.getenv('RMQ_PORT')
DB_HOST = os.getenv('DB_HOST')
SMD_USER = os.getenv('SMD_USER')
SMD_PWD = os.getenv('SMD_PWD')

MISC_USER = os.getenv('MISC_USER')
MISC_PWD = os.getenv('MISC_PWD')

LRS_HOST = os.getenv('LRS_HOST')

SERVICE_ACCOUNT_JSON = os.getenv('GCLOUD_SERVICE_ACCOUNT_JSON')

SMD_ENGINE = create_engine(f"oracle+oracledb://{SMD_USER}:{SMD_PWD}@{DB_HOST}:1521/geodbbm")
MISC_ENGINE = create_engine(f"oracle+oracledb://{MISC_USER}:{MISC_PWD}@{DB_HOST}:1521/geodbbm")

# Pydantic model
class PayloadSMD(BaseModel):
    file_name: str
    balai: str
    year: int
    semester: Optional[Literal[1,2]]
    routes: List[str]
    show_all_msg: Optional[bool] = False
  
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

def validate_rni(payload: PayloadSMD, job_id: str) -> str:
    """
    RNI validation handler function.
    """
    lrs = LRSRoute.from_feature_service(
            LRS_HOST, 
            payload.routes[0]
        )

    check = RouteRNIValidation.validate_excel(
        excel_path=payload.file_name,
        route=payload.routes[0],
        survey_year=payload.year,
        sql_engine=SMD_ENGINE,
        lrs=lrs,
        ignore_review=False,
        force_write=False
    )

    if check.get_status() == 'rejected':
        return check._result.to_job_event(job_id)

    check.base_validation()

    if (check.get_status() == 'verified'):
        check.put_data(semester=payload.semester)

    return check._result.to_job_event(job_id)

worker_logger = setup_logger("system")

class ValidationWorker:
    def __init__(self):
        self._rmq_url = f"amqp://{RMQ_HOST}:{RMQ_PORT}"
        self._rmq_conn = None
        self._rmq_channel = None
        self.job_queue = "validation_queue"
        self.job_event_queue = "job_event_queue"

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

    def handle_job(self, ch, methods, properties, body):
        try:
            job_data = json.loads(body.decode('utf-8'))

            if type(job_data) is str:
                job_data = json.loads(job_data)
            
            job_id = job_data.get("job_id")
            data_type = job_data.get("data_type")
            payload_str = base64.b64decode(job_data.get("details"))
            job_logger = get_job_logger(job_id)

            if data_type == 'RNI':
                payload = PayloadSMD(**json.loads(payload_str))

                job_logger.info("processing RNI validation.")

                self.publish_executed_event(job_id)
                event = validate_rni(payload, job_id)

                job_logger.info("finished executing RNI validation.")
            else:
                print("Unhandled data")  # Temporary, just for the lulz
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
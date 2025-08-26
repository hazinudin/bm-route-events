import traceback
import pika
import json
from route_events_service.validation_result.result import ValidationResult
from route_events_service import (
    BridgeMasterValidation,
    BridgeInventoryValidation,
    RouteRNIValidation,
    RouteRoughnessValidation,
    RouteDefectsValidation,
    RoutePCIValidation,
)
from route_events import LRSRoute
import polars as pl
import pyarrow as pa
import base64
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pydantic import BaseModel
from typing import Optional, Literal, List


TEST_MSG = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. In semper vitae sem sit amet lobortis. Morbi vulputate ut tellus eu mattis. Mauris eget tellus sit amet libero pretium dictum vel ac tortor. Ut efficitur lectus sapien, nec elementum ante gravida consectetur. Donec sagittis eu velit quis vulputate. Suspendisse viverra odio malesuada lobortis elementum. Suspendisse sed tortor dui. Proin ac euismod diam, in aliquet justo."
load_dotenv(os.path.dirname(__file__) + '/.env')

HOST = os.getenv('DB_HOST')
SMD_USER = os.getenv('SMD_USER')
SMD_PWD = os.getenv('SMD_PWD')

MISC_USER = os.getenv('MISC_USER')
MISC_PWD = os.getenv('MISC_PWD')

LRS_HOST = os.getenv('LRS_HOST')

SERVICE_ACCOUNT_JSON = os.getenv('GCLOUD_SERVICE_ACCOUNT_JSON')

SMD_ENGINE = create_engine(f"oracle+oracledb://{SMD_USER}:{SMD_PWD}@{HOST}:1521/geodbbm")
MISC_ENGINE = create_engine(f"oracle+oracledb://{MISC_USER}:{MISC_PWD}@{HOST}:1521/geodbbm")

# Pydantic model
class PayloadSMD(BaseModel):
    file_name: str
    balai: str
    year: int
    semester: Optional[Literal[1,2]]
    routes: List[str]
    show_all_msg: Optional[bool] = False
  

def validate_rni(payload: PayloadSMD) -> str:
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
        return check._result.to_arrow_base64()

    check.base_validation()

    if (check.get_status() == 'verified'):
        check.put_data(semester=payload.semester)

    return check._result.to_arrow_base64()


class ValidationWorker:
    def __init__(self, rabbitmq_url: str):
        self._rmq_url = rabbitmq_url
        self._rmq_conn = None
        self._rmq_channel = None
        self.job_queue = "validation_queue"
        self.result_queue = "result_queue"

    def connect(self):
        self._rmq_conn = pika.BlockingConnection(
            pika.URLParameters(self._rmq_url)
        )
        self._rmq_channel = self._rmq_conn.channel()

        # Declare queues
        self._rmq_channel.queue_declare(queue=self.job_queue, durable=True)
        self._rmq_channel.queue_declare(queue=self.result_queue, durable=True)

        self._rmq_channel.basic_qos(prefetch_count=1)

    def start_listening(self):
        try:
            self.connect()

            self._rmq_channel.basic_consume(
                queue=self.job_queue,
                on_message_callback=self.handle_job
            )

            self._rmq_channel.start_consuming()
        
        except KeyboardInterrupt:
            self._rmq_channel.stop_consuming()
            raise KeyboardInterrupt

    def handle_job(self, ch, methods, properties, body):
        try:
            job_data = json.loads(body.decode('utf-8'))

            if type(job_data) is str:
                job_data = json.loads(job_data)
            
            job_id = job_data.get("job_id")
            data_type = job_data.get("data_type")
            payload_str = base64.b64decode(job_data.get("details"))

            if data_type == 'RNI':
                payload = PayloadSMD(**json.loads(payload_str))
                arrow_b64 = validate_rni(payload)
            else:
                # Testing purpose
                result = ValidationResult(job_id)

                messages = pl.DataFrame([{"msg": TEST_MSG} for _ in range(10000)])
                result.add_messages(messages, 'error', 'force')
                sink = pa.BufferOutputStream()

                with pa.ipc.new_stream(sink, result._msg.df.to_arrow().schema) as writer:
                    for batch in result.get_all_messages().to_arrow().to_batches():
                        writer.write_batch(batch)

                arrow_b64 = base64.b64encode(sink.getvalue().to_pybytes()).decode('utf-8')

            print(job_data)  # Should be replaced by logger

            ch.basic_ack(methods.delivery_tag)

            self._rmq_channel.basic_publish(
                "",
                routing_key=self.result_queue,
                body=json.dumps({
                    "jobid": job_id, 
                    "status": "SUCCESS", 
                    "result": arrow_b64, 
                }),
                properties=pika.BasicProperties(delivery_mode=2)
            )
 
        except Exception as e:
            trace = traceback.format_exc()
            error_msg = e
            print(trace)
            ch.basic_ack(methods.delivery_tag)


if __name__ == '__main__':
    worker = ValidationWorker("amqp://localhost:5672")
    worker.start_listening()
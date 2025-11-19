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
    RTCValidation,
    RNIValidation, 
    IRIValidation, 
    ValidationHandler, 
    PCIValidation,
    DefectValidation,
    BridgeInventoryValidation_,
    BridgePopUpInventoryValidation,
    BridgeMasterValidation_,
    BridgeValidationPayloadFormat,
)
from typing import Dict

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider, StatusCode, Status
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


load_dotenv(os.path.dirname(__file__) + '/.env')

RMQ_HOST = os.getenv('RMQ_HOST')
RMQ_PORT = os.getenv('RMQ_PORT')
OTLP_EXPORTER_HOST = os.getenv('OTLP_EXPORTER_HOST')
OTLP_EXPORTER_PORT = os.getenv('OTLP_EXPORTER_PORT')
WRITE_VERIFIED_DATA = int(os.getenv('WRITE_VERIFIED_DATA'))

# Opentelemetry resource
resource = Resource.create({
    "service.name": "validation-worker",
    "environment": "production"
})

# Set the tracer
provider = TracerProvider(resource=resource)

# Configure exporter
otlp_exporter = OTLPSpanExporter(
    endpoint=f"{OTLP_EXPORTER_HOST}:{OTLP_EXPORTER_PORT}",
    insecure=True
)

# Create span processor
processor = BatchSpanProcessor(otlp_exporter, schedule_delay_millis=1000)
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)


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
        self._handler: Dict[str, ValidationHandler] = {}  # Empty dicitionary for handler class

        # Create handler for SMD
        self._smd_supported_data_type = [
            'ROUGHNESS', 
            'RNI', 
            'PCI', 
            'DEFECTS', 
            'RTC',
        ]  # Please update if more handlers are added.

        self._invij_supported_data_type = [
            'INVENTORY',
            'POPUP_INVENTORY',
        ]  # Please update if more handlers are added.
        
        # Road
        self._handler['RNI'] = RNIValidation
        self._handler['ROUGHNESS'] = IRIValidation
        self._handler['PCI'] = PCIValidation
        self._handler['DEFECTS'] = DefectValidation
        self._handler['RTC'] = RTCValidation
        
        # Bridge
        self._handler['INVENTORY'] = BridgeInventoryValidation_
        self._handler['POPUP_INVENTORY'] = BridgePopUpInventoryValidation
        self._handler['MASTER'] = BridgeMasterValidation_

    def connect(self):
        worker_logger.info(f"connecting to RabbitMQ on {self._rmq_url}")

        params = pika.ConnectionParameters(
            host=RMQ_HOST,
            port=int(RMQ_PORT),
            heartbeat=1200,
            blocked_connection_timeout=1200,
        )

        self._rmq_conn = pika.BlockingConnection(
            params,
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
    
    def invij_validate(self, data_type: str, payload: dict, job_id: str, validate: bool=True)->str:
        """
        INVIJ validation handler
        """
        check = self._handler[data_type](
            payload,
            job_id,
            validate
        )

        self.publish_executed_event(job_id)

        return check.validate()

    def handle_job(self, ch, methods, properties, body):
        # The headers, check if it contains the OpenTelemetry Trace ID
        if properties.headers:
            ctx = TraceContextTextMapPropagator().extract(carrier=properties.headers)
        else:
            properties.headers = {}
            ctx = None
        
        worker_logger.info(f"received headers: {properties.headers}")
        with tracer.start_as_current_span("handle_validation_job", context=ctx) as span:
            try:
                # The message body
                job_data = json.loads(body.decode('utf-8'))

                if type(job_data) is str:
                    job_data = json.loads(job_data)
                
                job_id = job_data.get("job_id")
                data_type = job_data.get("data_type")
                validate = job_data.get("validate")

                payload_str = base64.b64decode(job_data.get("details"))
                job_logger = get_job_logger(job_id)

                # Set span attribute
                span.set_attribute("job_id", job_id)
                span.set_attribute("payload", payload_str)
                span.set_attribute("data_type", data_type)
                span.set_attribute("validate", validate)

                if data_type in self._smd_supported_data_type:
                    payload = PayloadSMD(**json.loads(payload_str))
                    job_logger.info(f"processing {data_type} validation, validate: {validate}")
                    event = self.smd_validate(data_type, payload, job_id, validate)
                    job_logger.info(f"finished executing {data_type} validation.")

                elif data_type in self._invij_supported_data_type:
                    payload = BridgeValidationPayloadFormat.model_validate_json(payload_str)
                    job_logger.info(f"processing {data_type} validation, validate: {validate}")
                    event = self.invij_validate(data_type, payload, job_id, validate)
                    job_logger.info(f"finished executing {data_type} validation.")
                
                else:
                    job_logger.warning(f"{data_type} is unhandled")  # Temporary, just for the lulz
                    ch.basic_ack(methods.delivery_tag)  # Acknowledged to clear the queue
                    span.set_status(StatusCode.ERROR)
                    return

                ch.basic_ack(methods.delivery_tag)

                self._rmq_channel.basic_publish(
                    "",
                    routing_key=self.job_event_queue,
                    body=event,
                    properties=pika.BasicProperties(delivery_mode=2, headers=properties.headers)
                )

                span.set_status(Status(StatusCode.OK))
                job_logger.info("job succeeded event published.")
    
            except Exception as e:
                trace = traceback.format_exc()
                job_logger.error(trace)
                self.publish_failed_event(job_id)
                ch.basic_ack(methods.delivery_tag)

                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR), str(e))


if __name__ == '__main__':
    worker = ValidationWorker()
    worker.start_listening()
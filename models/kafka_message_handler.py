import json
import socket
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from odoo import api, fields, models


class MSKTokenProvider(AbstractTokenProvider):
    def __init__(self, region):
        self.region = region
    
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token


class KafkaMessageHandler(models.Model):
    _name = 'kafka.message.handler'
    _description = 'Kafka Message Handler'

    _cached_kafka = None 
    def _get_kafka(self):
        if KafkaMessageHandler._cached_kafka is None:
            KafkaMessageHandler._cached_kafka = self.config()
            _logger.info("Kafka instance initialized and cached.")
        else:
            if KafkaMessageHandler._cached_kafka['producer'] is None:
                KafkaMessageHandler._cached_kafka = self.config()
        return KafkaMessageHandler._cached_kafka

    message = fields.Text(
        'Message'
    )

    topic = fields.Char(
        'Topic'
    )

    operation_type = fields.Selection(
        [
            ('create', 'Create'), 
            ('update', 'Update'), 
            ('delete', 'Delete')
        ], 
        'Operation Type'
    )

    sent_status = fields.Selection(
        [
            ('sent', 'Sent'), 
            ('failed', 'Failed'),
            ('pending', 'Pending'), 
            ('not_configured', 'Not Configured')
        ], 
        'Sent Status'
    )

    sent_date = fields.Datetime(
        'Sent Date'
    )

    error_message = fields.Text(
        'Error Message'
    )

    def config():
        path = os.path.abspath(__file__)
        env_path = os.path.join(os.path.dirname(path), '.env')
        env_exists = os.path.isfile(env_path)

        if env_exists:
            with open(env_path, 'r') as f:
                for line in f:
                    name, value = line.strip().split('=', 1)
                    os.environ[name] = value
                
            required_vars = ['BROKER_SERVERS', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION']
            for var in required_vars:
                if var not in os.environ:
                    return {
                        "producer": None,
                        "error": True,
                        "message": f"Environment variable {var} not found"
                    }

            tp = MSKTokenProvider(os.environ['AWS_REGION'])
    
            producer = KafkaProducer(
                bootstrap_servers=os.environ['BROKER_SERVERS'].split(','),
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=tp,
                client_id=f"{socket.gethostname()}-producer",
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks=1,
                retries=3,
                batch_size=1024,
                linger_ms=5,
            )
            
            return {
                "producer": producer,
                "error": False,
                "message": None
            }

        else:
            return {
                "producer": None,
                "error": True,
                "message": "Environment file not found"
            }

    
    def create(self, vals_list):
        records = super().create(vals_list)
        kafka = self._get_kafka()
        _logger.info(f"Kafka: {kafka}")

        for record in records:
            record.sent_status = 'pending'

            if kafka and kafka['producer'] is not None:
                try:
                    kafka['producer'].send(
                        topic=record.topic,
                        value=record.message,
                        key=record.operation_type
                    )
                    kafka['producer'].flush()
                    record.sent_status = 'sent'
                    record.sent_date = datetime.now()
                except Exception as e:
                    record.sent_status = 'failed'
                    record.error_message = str(e)
            
            else:
                record.sent_status = 'not_configured'
                if kafka:
                    record.error_message = kafka['message']

        return records

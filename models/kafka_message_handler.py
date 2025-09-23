import json
import socket
import os
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
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

    message = fields.Text('Message')
    topic = fields.Char('Topic')
    operation_type = fields.Selection(
        [('create', 'Create'), ('update', 'Update'), ('delete', 'Delete')],
        'Operation Type'
    )
    sent_status = fields.Selection(
        [('sent', 'Sent'), ('failed', 'Failed'),
         ('pending', 'Pending'), ('not_configured', 'Not Configured')],
        'Sent Status'
    )
    sent_date = fields.Datetime('Sent Date')
    error_message = fields.Text('Error Message')

    def _get_kafka(self, topic_name='sale.order', partitions=1, replication_factor=None):
        """Return cached producer or create a new one with topic creation."""
        if KafkaMessageHandler._cached_kafka is not None:
            return KafkaMessageHandler._cached_kafka

        path = os.path.abspath(__file__)
        env_path = os.path.join(os.path.dirname(path), '.env')
        if not os.path.isfile(env_path):
            KafkaMessageHandler._cached_kafka = {
                'producer': None,
                'error': True,
                'message': "Environment file not found"
            }
            return KafkaMessageHandler._cached_kafka

        # Load .env variables
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and '=' in line:
                    name, value = line.split('=', 1)
                    os.environ[name] = value

        required_vars = ['BROKER_SERVERS', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION']
        for var in required_vars:
            if var not in os.environ:
                KafkaMessageHandler._cached_kafka = {
                    'producer': None,
                    'error': True,
                    'message': f"Environment variable {var} not found"
                }
                return KafkaMessageHandler._cached_kafka

        brokers = os.environ['BROKER_SERVERS'].split(',')
        region = os.environ['AWS_REGION']
        tp = MSKTokenProvider(region)

        # --- Try to create topic if it doesn't exist ---
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=brokers,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=tp,
                client_id=f"{socket.gethostname()}-admin"
            )

            existing_topics = admin_client.list_topics()

            if topic_name not in existing_topics:
                # Determine safe replication factor
                broker_count = len(brokers)
                if not replication_factor:
                    replication_factor = broker_count  # default to max safe
                safe_rf = min(replication_factor, broker_count)

                topic = NewTopic(
                    name=topic_name,
                    num_partitions=partitions,
                    replication_factor=safe_rf
                )
                admin_client.create_topics([topic])
                print(f"✅ Topic '{topic_name}' created (partitions={partitions}, replication_factor={safe_rf})")
            admin_client.close()
        except Exception as e:
            print(f"⚠️ Could not create topic '{topic_name}': {e}")

        # --- Create Kafka producer ---
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers,
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
            KafkaMessageHandler._cached_kafka = {
                'producer': producer,
                'error': False,
                'message': None
            }
        except Exception as e:
            KafkaMessageHandler._cached_kafka = {
                'producer': None,
                'error': True,
                'message': f"Error creating producer: {e}"
            }

        return KafkaMessageHandler._cached_kafka

    def create(self, vals_list):
        records = super().create(vals_list)
        for record in records:
            kafka = self._get_kafka(topic_name=record.topic or 'sale.order')
            record.sent_status = 'pending'

            if kafka and kafka['producer']:
                try:
                    kafka['producer'].send(
                        topic=record.topic or 'sale.order',
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

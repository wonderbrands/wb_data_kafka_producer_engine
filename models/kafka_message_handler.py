import json
import socket
import os
import logging
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from odoo import api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

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
        [('create', 'Create'), ('update', 'Update'), ('delete', 'Delete'), ('dump', 'Dump')],
        'Operation Type'
    )
    sent_status = fields.Selection(
        [('sent', 'Sent'), ('failed', 'Failed'),
         ('pending', 'Pending'), ('not_configured', 'Not Configured')],
        'Sent Status'
    )
    sent_date = fields.Datetime('Sent Date')
    error_message = fields.Text('Error Message')
    data_like = fields.Char('Data Like')

    def _get_kafka(self, topic_name, partitions=1, replication_factor=None, model_info=None):
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
        if model_info:
            if model_info.use_uniques_bss:
                brokers = [b['bootstrap_server'] for b in model_info.bootstrap_servers]
            else:
                brokers = brokers + [b['bootstrap_server'] for b in model_info.bootstrap_servers]

        if len(brokers) == 0:
            KafkaMessageHandler._cached_kafka = {
                'producer': None,
                'error': True,
                'message': "No bootstrap servers found"
            }
            return KafkaMessageHandler._cached_kafka

        region = os.environ['AWS_REGION']
        tp = MSKTokenProvider(region)

        # --- Create topic if it doesn't exist and WAIT for it to be ready ---
        topic_created = False
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=brokers,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=tp,
                client_id=f"{socket.gethostname()}-admin",
                request_timeout_ms=10000
            )

            existing_topics = admin_client.list_topics()

            if topic_name not in existing_topics:
                broker_count = len(brokers)
                if not replication_factor:
                    replication_factor = min(3, broker_count)
                safe_rf = min(replication_factor, broker_count)

                topic = NewTopic(
                    name=topic_name,
                    num_partitions=partitions,
                    replication_factor=safe_rf
                )
                admin_client.create_topics([topic], timeout_ms=10000)
                #_logger.info(f"Topic '{topic_name}' created (partitions={partitions}, replication_factor={safe_rf})")
                topic_created = True
            
            admin_client.close()

            # CRITICAL: If topic was just created, wait for it to be ready
            if topic_created:
                #_logger.info(f"Waiting for topic '{topic_name}' to be ready...")
                max_wait = 30
                start = time.time()
                topic_ready = False
                
                while time.time() - start < max_wait:
                    try:
                        check_admin = KafkaAdminClient(
                            bootstrap_servers=brokers,
                            security_protocol='SASL_SSL',
                            sasl_mechanism='OAUTHBEARER',
                            sasl_oauth_token_provider=MSKTokenProvider(region),
                            client_id=f"{socket.gethostname()}-check",
                            request_timeout_ms=5000
                        )
                        topics = check_admin.list_topics()
                        check_admin.close()
                        
                        if topic_name in topics:
                            #_logger.info(f"Topic '{topic_name}' is ready")
                            topic_ready = True
                            break
                    except:
                        pass
                    
                    time.sleep(1)
                
                if not topic_ready:
                    _logger.warning(f"Topic '{topic_name}' may not be fully ready yet")

        except TopicAlreadyExistsError:
            _logger.info(f"Topic '{topic_name}' already exists")
        except Exception as e:
            _logger.error(f"Could not create topic '{topic_name}': {e}")

        #_logger.info("================================")
        #_logger.info("Creating Kafka producer")
        #_logger.info(f"Brokers: {brokers}")
        
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
                max_block_ms=30000,  # Reduced from default 60s
                request_timeout_ms=25000,
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
        #_logger.info("================================")
        #_logger.info("Creating Kafka messages")
        records = super().create(vals_list)
        for record in records:
            kafka = self._get_kafka(
                topic_name=f"{record.topic}",
                model_info=self.env["followed.model"].search([("model", "=", self._name)], limit=1))
            record.sent_status = 'pending'
            #_logger.info(f"Creating Kafka message for {record.topic} with {record.message}")
            data_like = record.data_like

            if kafka and kafka['producer']:
                try:
                    #_logger.info(f"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                    #_logger.info(f"the topic is {record.topic}")
                    #_logger.info(f"Sending Kafka message into topic {record.topic}-_-{data_like}")
                    kafka['producer'].send(
                        topic=f"{record.topic}",
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

    def action_retry(self):
        records_not_sent = self.search([('sent_status', '!=', 'sent')])
        if records_not_sent:
            for record in records_not_sent:
                kafka = self._get_kafka(
                    topic_name=f"{record.topic}",
                    model_info=self.env["followed.model"].search([("model", "=", self._name)], limit=1))
                data_like = record.data_like
                if kafka and kafka['producer']:
                    try:
                        kafka['producer'].send(
                            topic=f"{record.topic}",
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
            raise UserError("No records to retry")
import json
import socket
import os
import logging
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer, TopicPartition
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
    _order = 'sent_date desc, id desc'

    _cached_producer = None
    _known_topics = set()

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

    def _load_env(self):
        """Load environment variables from .env file."""
        path = os.path.abspath(__file__)
        env_path = os.path.join(os.path.dirname(path), '.env')
        if not os.path.isfile(env_path):
            _logger.warning("Environment file not found at %s", env_path)
            return False

        # Load .env variables
        try:
            with open(env_path, 'r') as f:
                loaded_vars = []
                for line in f:
                    line = line.strip()
                    if line and '=' in line and not line.startswith('#'):
                        name, value = line.split('=', 1)
                        # Remove possible quotes
                        value = value.strip('"').strip("'")
                        os.environ[name] = value
                        loaded_vars.append(name)
            _logger.info("Loaded environment variables from .env: %s", ", ".join(loaded_vars))
            return True
        except Exception as e:
            _logger.error("Failed to read .env file at %s: %s", env_path, e)
            return False

    def _get_producer(self, model_info=None):
        """Return cached producer or create a new one."""
        if KafkaMessageHandler._cached_producer is not None:
            return {'producer': KafkaMessageHandler._cached_producer, 'error': False}

        if not os.environ.get('BROKER_SERVERS'):
            _logger.info("BROKER_SERVERS not in environment, attempting to load .env")
            self._load_env()

        required_vars = ['BROKER_SERVERS', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION']
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        if missing_vars:
            msg = f"Missing environment variables: {', '.join(missing_vars)}"
            _logger.error(msg)
            return {'producer': None, 'error': True, 'message': msg}

        brokers = os.environ['BROKER_SERVERS'].split(',')
        if model_info:
            if model_info.use_uniques_bss:
                brokers = [b['bootstrap_server'] for b in model_info.bootstrap_servers if b['bootstrap_server']]
            else:
                # Filter out empty strings if any
                extra_brokers = [b['bootstrap_server'] for b in model_info.bootstrap_servers if b['bootstrap_server']]
                brokers = brokers + extra_brokers

        if not brokers or brokers == ['']:
            return {'producer': None, 'error': True, 'message': "No bootstrap servers found"}

        region = os.environ['AWS_REGION']
        tp = MSKTokenProvider(region)

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
                max_block_ms=30000,
                request_timeout_ms=25000,
                batch_size=1024,
                linger_ms=5,
            )
            KafkaMessageHandler._cached_producer = producer
            return {'producer': producer, 'error': False}
        except Exception as e:
            _logger.error("Error creating Kafka producer: %s", e)
            return {'producer': None, 'error': True, 'message': f"Error creating producer: {e}"}

    def _ensure_topic_exists(self, topic_name, model_info=None):
        """Ensure topic exists, creating it if necessary."""
        if topic_name in KafkaMessageHandler._known_topics:
            return True

        if not os.environ.get('BROKER_SERVERS'):
            _logger.info("BROKER_SERVERS not in environment during topic check, attempting to load .env")
            self._load_env()

        brokers = os.environ.get('BROKER_SERVERS', '').split(',')
        if model_info:
            if model_info.use_uniques_bss:
                brokers = [b['bootstrap_server'] for b in model_info.bootstrap_servers if b['bootstrap_server']]
            else:
                extra_brokers = [b['bootstrap_server'] for b in model_info.bootstrap_servers if b['bootstrap_server']]
                if brokers == ['']:
                    brokers = extra_brokers
                else:
                    brokers = brokers + extra_brokers
        
        # Filter out empty strings from brokers list
        brokers = [b for b in brokers if b]
        
        if not brokers:
            _logger.error("No brokers configured for Kafka. Cannot ensure topic %s exists.", topic_name)
            return False

        region = os.environ.get('AWS_REGION')
        if not region:
            _logger.error("AWS_REGION not configured. Cannot ensure topic %s exists.", topic_name)
            return False

        _logger.info("Ensuring topic %s exists using brokers: %s", topic_name, brokers)
        tp = MSKTokenProvider(region)

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=brokers,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=tp,
                client_id=f"{socket.gethostname()}-admin-check",
                request_timeout_ms=10000
            )

            existing_topics = admin_client.list_topics()
            if topic_name in existing_topics:
                _logger.info("Topic %s already exists in Kafka", topic_name)
                KafkaMessageHandler._known_topics.add(topic_name)
                admin_client.close()
                return True

            # Create topic
            _logger.info("Topic %s does not exist, attempting to create it", topic_name)
            broker_count = len(brokers)
            replication_factor = min(3, broker_count)
            
            try:
                topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=replication_factor)
                admin_client.create_topics([topic], timeout_ms=10000)
                KafkaMessageHandler._known_topics.add(topic_name)
                _logger.info("Successfully created Kafka topic: %s with rf=%s", topic_name, replication_factor)
            except Exception as e:
                _logger.warning("Failed to create topic %s with rf=%s: %s. Retrying with rf=1", 
                               topic_name, replication_factor, e)
                try:
                    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
                    admin_client.create_topics([topic], timeout_ms=10000)
                    KafkaMessageHandler._known_topics.add(topic_name)
                    _logger.info("Successfully created Kafka topic %s with rf=1", topic_name)
                except Exception as e2:
                    _logger.error("Failed to create topic %s even with rf=1: %s", topic_name, e2)
                    admin_client.close()
                    return False
            
            admin_client.close()
            return True
        except Exception as e:
            _logger.error("Error in _ensure_topic_exists for %s: %s", topic_name, e)
            return False

    def create(self, vals_list):
        records = super().create(vals_list)
        for record in records:
            # Derive model name from topic (format is model_name-_-api_like or model_name-_-schema_like)
            model_name = record.topic.split('-_-')[0] if record.topic else False
            model_info = self.env["followed.model"].search([("model.model", "=", model_name)], limit=1)
            
            # Ensure topic exists before sending
            self._ensure_topic_exists(record.topic, model_info=model_info)
            
            kafka = self._get_producer(model_info=model_info)
            record.sent_status = 'pending'

            if kafka and kafka['producer']:
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
                    record.error_message = kafka.get('message')

        return records

    def action_retry(self):
        records_not_sent = self.search([('sent_status', '!=', 'sent')])
        if records_not_sent:
            for record in records_not_sent:
                # Derive model name from topic
                model_name = record.topic.split('-_-')[0] if record.topic else False
                model_info = self.env["followed.model"].search([("model.model", "=", model_name)], limit=1)
                
                self._ensure_topic_exists(record.topic, model_info=model_info)
                kafka = self._get_producer(model_info=model_info)
                if kafka and kafka['producer']:
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
            raise UserError("No records to retry")

    @api.model
    def get_kafka_topics(self):
        """Fetch all topics from Kafka with message counts"""
        # Trigger cached setup or get config
        kafka_info = self._get_producer() 
        if kafka_info.get('error') and not os.environ.get('BROKER_SERVERS'):
            return {'error': kafka_info.get('message')}

        try:
            brokers = os.environ['BROKER_SERVERS'].split(',')
            region = os.environ['AWS_REGION']
            tp = MSKTokenProvider(region)

            admin_client = KafkaAdminClient(
                bootstrap_servers=brokers,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=tp,
                client_id=f"{socket.gethostname()}-admin-query"
            )
            topic_names = admin_client.list_topics()
            admin_client.close()

            # Now get counts for each topic using a consumer
            consumer = KafkaConsumer(
                bootstrap_servers=brokers,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=tp,
                client_id=f"{socket.gethostname()}-count-query"
            )

            result = []
            for name in sorted(topic_names):
                total_messages = 0
                try:
                    partitions = consumer.partitions_for_topic(name)
                    if partitions:
                        tps = [TopicPartition(name, p) for p in partitions]
                        beg_offsets = consumer.beginning_offsets(tps)
                        end_offsets = consumer.end_offsets(tps)
                        for tp_obj in tps:
                            total_messages += end_offsets[tp_obj] - beg_offsets[tp_obj]
                except Exception as e:
                    _logger.warning("Could not get count for topic %s: %s", name, e)

                result.append({
                    'name': name,
                    'total_messages': total_messages
                })

            consumer.close()
            return result
        except Exception as e:
            _logger.error("Failed to get topics with counts: %s", e, exc_info=True)
            return {'error': str(e)}

    @api.model
    def get_kafka_messages(self, topic_name, count=4):
        """Fetch the last N messages from a topic"""
        kafka_info = self._get_producer()
        if kafka_info.get('error') and not os.environ.get('BROKER_SERVERS'):
            return {'error': kafka_info.get('message')}

        try:
            brokers = os.environ['BROKER_SERVERS'].split(',')
            region = os.environ['AWS_REGION']
            tp = MSKTokenProvider(region)

            consumer = KafkaConsumer(
                bootstrap_servers=brokers,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=tp,
                client_id=f"{socket.gethostname()}-consumer",
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda v: v.decode('utf-8') if v else None
            )

            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                consumer.close()
                return []

            messages = []
            for p in partitions:
                tp_obj = TopicPartition(topic_name, p)
                consumer.assign([tp_obj])
                
                end_offsets = consumer.end_offsets([tp_obj])
                end_offset = end_offsets[tp_obj]
                
                start_offset = max(0, end_offset - count)
                consumer.seek(tp_obj, start_offset)
                
                batch = consumer.poll(timeout_ms=5000)
                if tp_obj in batch:
                    for msg in batch[tp_obj]:
                        messages.append({
                            'offset': msg.offset,
                            'timestamp': datetime.fromtimestamp(msg.timestamp/1000.0).isoformat(),
                            'key': msg.key.decode('utf-8') if msg.key else None,
                            'value': msg.value
                        })

            consumer.close()
            messages.sort(key=lambda x: x['offset'], reverse=True)
            return messages[:count]
        except Exception as e:
            _logger.error("Failed to get messages: %s", e, exc_info=True)
            return {'error': str(e)}

    @api.model
    def delete_kafka_topic(self, topic_name):
        """Delete a topic from Kafka"""
        kafka_info = self._get_producer()
        if kafka_info.get('error') and not os.environ.get('BROKER_SERVERS'):
            return {'error': kafka_info.get('message')}

        try:
            brokers = os.environ['BROKER_SERVERS'].split(',')
            admin_client = KafkaAdminClient(
                bootstrap_servers=brokers,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=MSKTokenProvider(os.environ['AWS_REGION']),
                client_id=f"{socket.gethostname()}-admin-delete"
            )
            admin_client.delete_topics([topic_name])
            admin_client.close()
            KafkaMessageHandler._cached_kafka = None
            return True
        except Exception as e:
            return {'error': str(e)}

    @api.model
    def get_kafka_consumer_groups(self):
        """Fetch all consumer groups from Kafka"""
        kafka_info = self._get_producer()
        if kafka_info.get('error') and not os.environ.get('BROKER_SERVERS'):
            return {'error': kafka_info.get('message')}

        try:
            brokers = os.environ['BROKER_SERVERS'].split(',')
            region = os.environ['AWS_REGION']
            tp = MSKTokenProvider(region)

            admin_client = KafkaAdminClient(
                bootstrap_servers=brokers,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=tp,
                client_id=f"{socket.gethostname()}-admin-groups"
            )
            groups = admin_client.list_consumer_groups()
            admin_client.close()
            
            return sorted([g[0] for g in groups])
        except Exception as e:
            _logger.error("Failed to get consumer groups: %s", e, exc_info=True)
            return {'error': str(e)}

    @api.model
    def get_kafka_consumer_group_details(self, group_id):
        """Fetch details (offsets, lag) for a consumer group"""
        kafka_info = self._get_producer()
        if kafka_info.get('error') and not os.environ.get('BROKER_SERVERS'):
            return {'error': kafka_info.get('message')}

        try:
            brokers = os.environ['BROKER_SERVERS'].split(',')
            region = os.environ['AWS_REGION']
            tp = MSKTokenProvider(region)

            admin_client = KafkaAdminClient(
                bootstrap_servers=brokers,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=tp,
                client_id=f"{socket.gethostname()}-admin-group-details"
            )

            offsets = admin_client.list_consumer_group_offsets(group_id)
            
            # Use a consumer to get end offsets
            consumer = KafkaConsumer(
                bootstrap_servers=brokers,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=tp,
                client_id=f"{socket.gethostname()}-lag-query"
            )

            details = []
            for tp_obj, offset_and_metadata in offsets.items():
                current_offset = offset_and_metadata.offset
                
                # Get end offset for this partition
                end_offsets = consumer.end_offsets([tp_obj])
                log_end_offset = end_offsets.get(tp_obj, 0)
                
                lag = max(0, log_end_offset - current_offset) if current_offset is not None else log_end_offset

                details.append({
                    'topic': tp_obj.topic,
                    'partition': tp_obj.partition,
                    'current_offset': current_offset,
                    'log_end_offset': log_end_offset,
                    'lag': lag
                })

            admin_client.close()
            consumer.close()
            
            # Sort by topic and partition
            details.sort(key=lambda x: (x['topic'], x['partition']))
            return details
        except Exception as e:
            _logger.error("Failed to get group details for %s: %s", group_id, e, exc_info=True)
            return {'error': str(e)}
import json
import socket
import os
import uuid
import logging
import time
import threading
from datetime import datetime
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
try:
    from confluent_kafka.admin import ConsumerGroupTopicPartitions
except ImportError:
    from confluent_kafka.admin import _ConsumerGroupTopicPartitions as ConsumerGroupTopicPartitions
from confluent_kafka import KafkaException
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from odoo import api, fields, models
from odoo.exceptions import UserError
import odoo

_logger = logging.getLogger(__name__)


class KafkaMessageHandler(models.Model):
    _name = 'kafka.message.handler'
    _description = 'Kafka Message Handler'
    _order = 'sent_date desc, id desc'

    _cached_producer = None
    _known_topics = set()
    _broker_status = {
        'available': True,
        'last_failure': 0.0,
        'cooldown_seconds': 30,
        'failure_message': ''
    }

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

    # ---------------------------------------------------------
    # Circuit Breaker Helpers
    # ---------------------------------------------------------
    @classmethod
    def _is_circuit_open(cls):
        """Returns True if the broker is currently in cooldown due to previous failures."""
        if not cls._broker_status['available']:
            elapsed = time.time() - cls._broker_status['last_failure']
            if elapsed < cls._broker_status['cooldown_seconds']:
                return True
            # Cooldown expired, allow single trial attempt
        return False

    @classmethod
    def _record_broker_failure(cls, err_msg):
        """Trip circuit breaker on connection failure."""
        cls._broker_status['available'] = False
        cls._broker_status['last_failure'] = time.time()
        cls._broker_status['failure_message'] = str(err_msg)
        cls._cached_producer = None
        _logger.warning("Kafka Circuit Breaker OPEN (cooldown %ss): %s", cls._broker_status['cooldown_seconds'], err_msg)

    @classmethod
    def _record_broker_success(cls):
        """Reset circuit breaker on successful operation."""
        if not cls._broker_status['available']:
            _logger.info("Kafka Circuit Breaker CLOSED: broker connection restored.")
        cls._broker_status['available'] = True
        cls._broker_status['failure_message'] = ''

    # ---------------------------------------------------------
    # Configuration & Client Factory
    # ---------------------------------------------------------
    def _get_kafka_config(self):
        """Retrieve Kafka configuration and ensure os.environ is set for AWS."""
        Config = self.env['ir.config_parameter'].sudo()
        brokers = Config.get_param('kafka_producer.broker_servers', '')
        auth_type = Config.get_param('kafka_producer.auth_type', '')
        
        # Backward compatibility fallback
        if not auth_type:
            use_sasl_iam = Config.get_param('kafka_producer.use_sasl_iam', 'True') == 'True'
            auth_type = 'aws_iam' if use_sasl_iam else 'plaintext'

        aws_access_key = Config.get_param('kafka_producer.aws_access_key_id', '')
        aws_secret_key = Config.get_param('kafka_producer.aws_secret_access_key', '')
        aws_region = Config.get_param('kafka_producer.aws_region', 'us-east-1')

        sasl_username = Config.get_param('kafka_producer.sasl_username', '')
        sasl_password = Config.get_param('kafka_producer.sasl_password', '')
        sasl_mechanism = Config.get_param('kafka_producer.sasl_mechanism', 'SCRAM-SHA-256')
        security_protocol = Config.get_param('kafka_producer.security_protocol', 'SASL_SSL')

        if aws_access_key:
            os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key
        if aws_secret_key:
            os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_key
        if aws_region:
            os.environ['AWS_REGION'] = aws_region

        return {
            'brokers': [b.strip() for b in brokers.split(',') if b.strip()],
            'auth_type': auth_type,
            'region': aws_region,
            'aws_access_key': aws_access_key,
            'aws_secret_key': aws_secret_key,
            'sasl_username': sasl_username,
            'sasl_password': sasl_password,
            'sasl_mechanism': sasl_mechanism,
            'security_protocol': security_protocol,
        }

    def _get_client_conf(self, client_type='producer', model_info=None, extra_conf=None):
        """Generate unified configuration for Producer, Consumer, or AdminClient."""
        config = self._get_kafka_config()
        brokers = list(config['brokers'])

        if model_info:
            if model_info.use_uniques_bss:
                brokers = [b.bootstrap_server for b in model_info.bootstrap_servers if b.bootstrap_server]
            else:
                extra_brokers = [b.bootstrap_server for b in model_info.bootstrap_servers if b.bootstrap_server]
                brokers = brokers + extra_brokers

        brokers = [b.strip() for b in brokers if b and b.strip()]
        if not brokers:
            return {'conf': None, 'error': True, 'message': "No bootstrap servers found"}

        unique_suffix = uuid.uuid4().hex[:8]
        conf = {
            'bootstrap.servers': ','.join(brokers),
            'client.id': f"{socket.gethostname()}-{client_type}-{unique_suffix}",
            'socket.timeout.ms': 3000,
            'request.timeout.ms': 5000,
        }

        auth_type = config.get('auth_type', 'sasl_scram')
        if auth_type == 'aws_iam':
            region = config['region']
            if not config['aws_access_key'] or not config['aws_secret_key'] or not region:
                return {'conf': None, 'error': True, 'message': "Missing AWS credentials for MSK IAM authentication."}
            conf.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'OAUTHBEARER',
                'oauth_cb': lambda x: (MSKAuthTokenProvider.generate_auth_token(region)[0], MSKAuthTokenProvider.generate_auth_token(region)[1] / 1000.0),
            })
        elif auth_type == 'sasl_scram':
            conf.update({
                'security.protocol': config.get('security_protocol') or 'SASL_SSL',
                'sasl.mechanism': config.get('sasl_mechanism') or 'SCRAM-SHA-256',
                'sasl.username': config.get('sasl_username') or '',
                'sasl.password': config.get('sasl_password') or '',
            })
        elif auth_type == 'plaintext':
            conf.update({
                'security.protocol': 'PLAINTEXT',
            })

        if extra_conf:
            conf.update(extra_conf)

        return {'conf': conf, 'error': False, 'brokers': brokers}

    def _get_producer(self, model_info=None):
        """Return cached producer or create a new one."""
        if KafkaMessageHandler._cached_producer is not None:
            return {'producer': KafkaMessageHandler._cached_producer, 'error': False}

        if self._is_circuit_open():
            return {'producer': None, 'error': True, 'message': f"Broker unavailable (Circuit Breaker active: {self._broker_status['failure_message']})"}

        client_info = self._get_client_conf(
            client_type='producer',
            model_info=model_info,
            extra_conf={
                'acks': '1',
                'retries': 3,
                'linger.ms': 5,
            }
        )
        if client_info.get('error'):
            return {'producer': None, 'error': True, 'message': client_info.get('message')}

        try:
            producer = Producer(client_info['conf'])
            KafkaMessageHandler._cached_producer = producer
            return {'producer': producer, 'error': False}
        except Exception as e:
            self._record_broker_failure(e)
            _logger.error("Error creating Kafka producer: %s", e)
            return {'producer': None, 'error': True, 'message': f"Error creating producer: {e}"}

    # ---------------------------------------------------------
    # Topic Management (Synchronous & Asynchronous)
    # ---------------------------------------------------------
    def _ensure_topic_exists_async(self, topic_name, model_info=None):
        """Ensure topic exists without blocking caller execution."""
        if not topic_name or topic_name in KafkaMessageHandler._known_topics:
            return True

        if self._is_circuit_open():
            return False

        dbname = self.env.cr.dbname
        model_info_id = model_info.id if model_info else False

        def worker():
            try:
                with odoo.registry(dbname).cursor() as cr:
                    env = api.Environment(cr, odoo.SUPERUSER_ID, {})
                    m_info = env['followed.model'].browse(model_info_id) if model_info_id else None
                    env['kafka.message.handler']._ensure_topic_exists(topic_name, model_info=m_info)
            except Exception as e:
                _logger.warning("Background topic check failed for %s: %s", topic_name, e)

        threading.Thread(target=worker, daemon=True, name=f"kafka-topic-check-{topic_name}").start()
        return True

    def _ensure_topic_exists(self, topic_name, model_info=None):
        """Ensure topic exists, creating it if necessary. Fail-fast if broker is unavailable."""
        if not topic_name or topic_name in KafkaMessageHandler._known_topics:
            return True

        if self._is_circuit_open():
            _logger.warning("Skipping topic existence check for %s: Circuit Breaker active", topic_name)
            return False

        client_info = self._get_client_conf(client_type='admin-check', model_info=model_info)
        if client_info.get('error'):
            _logger.error("No brokers configured for Kafka. Cannot ensure topic %s exists: %s", topic_name, client_info.get('message'))
            return False

        conf = client_info['conf']
        brokers = client_info['brokers']

        try:
            admin_client = AdminClient(conf)
            topics_metadata = admin_client.list_topics(timeout=3.0)
            self._record_broker_success()

            existing_topics = topics_metadata.topics
            if topic_name in existing_topics:
                KafkaMessageHandler._known_topics.add(topic_name)
                return True

            # Create topic
            _logger.info("Topic %s does not exist, attempting to create it", topic_name)
            broker_count = len(brokers)
            replication_factor = min(3, broker_count) if broker_count > 0 else 1

            try:
                topic = NewTopic(topic_name, num_partitions=1, replication_factor=replication_factor)
                futures = admin_client.create_topics([topic], operation_timeout=5.0)
                for t, future in futures.items():
                    future.result()
                KafkaMessageHandler._known_topics.add(topic_name)
                _logger.info("Successfully created Kafka topic: %s with rf=%s", topic_name, replication_factor)
                return True
            except Exception as e:
                _logger.warning("Failed to create topic %s with rf=%s: %s. Retrying with rf=1", topic_name, replication_factor, e)
                try:
                    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
                    futures = admin_client.create_topics([topic], operation_timeout=5.0)
                    for t, future in futures.items():
                        future.result()
                    KafkaMessageHandler._known_topics.add(topic_name)
                    _logger.info("Successfully created Kafka topic %s with rf=1", topic_name)
                    return True
                except Exception as e2:
                    _logger.error("Failed to create topic %s with rf=1: %s", topic_name, e2)
                    return False
        except Exception as e:
            self._record_broker_failure(e)
            _logger.error("Error in _ensure_topic_exists for %s: %s", topic_name, e)
            return False

    # ---------------------------------------------------------
    # Message Dispatch & Retry
    # ---------------------------------------------------------
    def create(self, vals_list):
        records = super().create(vals_list)

        # Fast-fail if Circuit Breaker is open
        if self._is_circuit_open():
            reason = f"Broker unavailable (Circuit Breaker active: {self._broker_status['failure_message']})"
            for record in records:
                record.sent_status = 'pending'
                record.error_message = reason
            return records

        producers_to_flush = set()
        for record in records:
            model_name = record.topic.split('-_-')[0] if record.topic else False
            model_info = self.env["followed.model"].sudo().search([("model.model", "=", model_name)], limit=1)

            kafka = self._get_producer(model_info=model_info)
            record.sent_status = 'pending'

            if kafka and kafka['producer']:
                try:
                    kafka['producer'].produce(
                        topic=record.topic,
                        value=record.message.encode('utf-8') if record.message else b'',
                        key=record.operation_type.encode('utf-8') if record.operation_type else None
                    )
                    producers_to_flush.add(kafka['producer'])
                    record.sent_status = 'sent'
                    record.sent_date = datetime.now()

                    # Overwrite message to only contain the odoo_internal_id on success
                    try:
                        if record.message:
                            msg_data = json.loads(record.message)
                            if isinstance(msg_data, str):
                                msg_data = json.loads(msg_data)
                            if isinstance(msg_data, dict) and 'odoo_internal_id' in msg_data:
                                record.message = str(msg_data['odoo_internal_id'])
                    except Exception as json_err:
                        _logger.warning("Could not parse message or find odoo_internal_id on creation success: %s", json_err)
                except Exception as e:
                    self._record_broker_failure(e)
                    record.sent_status = 'failed'
                    record.error_message = str(e)
            else:
                record.sent_status = 'not_configured'
                if kafka:
                    record.error_message = kafka.get('message')

        for producer in producers_to_flush:
            try:
                producer.flush(timeout=3.0)
                self._record_broker_success()
            except Exception as flush_err:
                self._record_broker_failure(flush_err)
                _logger.error("Error flushing Kafka producer: %s", flush_err)

        return records

    def action_retry(self):
        records_not_sent = self.search([('sent_status', '!=', 'sent')])
        if not records_not_sent:
            raise UserError("No records to retry")

        if self._is_circuit_open():
            raise UserError(f"Cannot retry now: Kafka Broker is unreachable (Circuit Breaker active: {self._broker_status['failure_message']}). Please retry later.")

        for record in records_not_sent:
            model_name = record.topic.split('-_-')[0] if record.topic else False
            model_info = self.env["followed.model"].sudo().search([("model.model", "=", model_name)], limit=1)

            kafka = self._get_producer(model_info=model_info)
            if kafka and kafka['producer']:
                try:
                    kafka['producer'].produce(
                        topic=record.topic,
                        value=record.message.encode('utf-8') if record.message else b'',
                        key=record.operation_type.encode('utf-8') if record.operation_type else None
                    )
                    kafka['producer'].flush(timeout=3.0)
                    self._record_broker_success()
                    record.sent_status = 'sent'
                    record.sent_date = datetime.now()

                    current_error = record.error_message or ''
                    log_msg = f"Reenviado con éxito el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    record.error_message = f"{current_error}\n{log_msg}" if current_error else log_msg

                    try:
                        if record.message:
                            msg_data = json.loads(record.message)
                            if isinstance(msg_data, str):
                                msg_data = json.loads(msg_data)
                            if isinstance(msg_data, dict) and 'odoo_internal_id' in msg_data:
                                record.message = str(msg_data['odoo_internal_id'])
                    except Exception as json_err:
                        _logger.warning("Could not parse message or find odoo_internal_id on retry success: %s", json_err)
                except Exception as e:
                    self._record_broker_failure(e)
                    record.sent_status = 'failed'
                    current_error = record.error_message or ''
                    log_msg = f"Intento de reenvío fallido el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {e}"
                    record.error_message = f"{current_error}\n{log_msg}" if current_error else log_msg

    # ---------------------------------------------------------
    # Frontend Owl Component Queries
    # ---------------------------------------------------------
    @api.model
    def get_kafka_topics(self):
        """Fetch all topics from Kafka with message counts"""
        if self._is_circuit_open():
            return {'error': f"Broker unavailable (Circuit Breaker active: {self._broker_status['failure_message']})"}

        client_info = self._get_client_conf(client_type='topics-query')
        if client_info.get('error'):
            return {'error': client_info.get('message')}

        conf = client_info['conf']
        try:
            admin_client = AdminClient(conf)
            topics_metadata = admin_client.list_topics(timeout=5.0)
            self._record_broker_success()
            topic_names = list(topics_metadata.topics.keys())

            consumer_conf = dict(conf)
            consumer_conf.update({
                'client.id': f"{conf.get('client.id')}-count-consumer",
                'group.id': f"odoo-viewer-count-{uuid.uuid4().hex[:8]}",
            })

            consumer = Consumer(consumer_conf)
            try:
                result = []
                for name in sorted(topic_names):
                    total_messages = 0
                    try:
                        topic_meta = topics_metadata.topics.get(name)
                        if topic_meta:
                            for p_id in topic_meta.partitions.keys():
                                tp = TopicPartition(name, p_id)
                                low, high = consumer.get_watermark_offsets(tp, timeout=3.0)
                                total_messages += (high - low)
                    except Exception as e:
                        _logger.warning("Could not get count for topic %s: %s", name, e)

                    result.append({
                        'name': name,
                        'total_messages': total_messages
                    })

                return result
            finally:
                consumer.close()
        except Exception as e:
            self._record_broker_failure(e)
            _logger.error("Failed to get topics with counts: %s", e, exc_info=True)
            return {'error': str(e)}

    @api.model
    def get_kafka_messages(self, topic_name, count=4):
        """Fetch the last N messages from a topic"""
        if self._is_circuit_open():
            return {'error': f"Broker unavailable (Circuit Breaker active: {self._broker_status['failure_message']})"}

        client_info = self._get_client_conf(client_type='msg-query')
        if client_info.get('error'):
            return {'error': client_info.get('message')}

        conf = client_info['conf']
        consumer_conf = dict(conf)
        consumer_conf.update({
            'group.id': f"odoo-viewer-msg-{uuid.uuid4().hex[:8]}",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })

        try:
            consumer = Consumer(consumer_conf)
            try:
                topics_metadata = consumer.list_topics(topic_name, timeout=5.0)
                self._record_broker_success()
                topic_meta = topics_metadata.topics.get(topic_name)
                if not topic_meta:
                    return []

                messages = []
                for p_id in topic_meta.partitions.keys():
                    tp = TopicPartition(topic_name, p_id)
                    try:
                        low, high = consumer.get_watermark_offsets(tp, timeout=3.0)
                        start_offset = max(low, high - count)

                        tp.offset = start_offset
                        consumer.assign([tp])

                        num_to_read = high - start_offset
                        read_count = 0
                        while read_count < num_to_read:
                            msg = consumer.poll(timeout=1.0)
                            if msg is None or msg.error():
                                break

                            ts_type, ts_val = msg.timestamp()
                            ts_dt = datetime.fromtimestamp(ts_val / 1000.0) if ts_val else datetime.now()

                            messages.append({
                                'offset': msg.offset(),
                                'timestamp': ts_dt.isoformat(),
                                'key': msg.key().decode('utf-8') if msg.key() else None,
                                'value': msg.value().decode('utf-8') if msg.value() else None
                            })
                            read_count += 1
                    except Exception as p_err:
                        _logger.warning("Error reading partition %s: %s", p_id, p_err)

                messages.sort(key=lambda x: x['offset'], reverse=True)
                return messages[:count]
            finally:
                consumer.close()
        except Exception as e:
            self._record_broker_failure(e)
            _logger.error("Failed to get messages: %s", e, exc_info=True)
            return {'error': str(e)}

    @api.model
    def delete_kafka_topic(self, topic_name):
        """Delete a topic from Kafka"""
        if self._is_circuit_open():
            return {'error': f"Broker unavailable (Circuit Breaker active: {self._broker_status['failure_message']})"}

        client_info = self._get_client_conf(client_type='admin-delete')
        if client_info.get('error'):
            return {'error': client_info.get('message')}

        try:
            admin_client = AdminClient(client_info['conf'])
            futures = admin_client.delete_topics([topic_name], operation_timeout=5.0)
            for topic, future in futures.items():
                future.result()
            self._record_broker_success()
            KafkaMessageHandler._cached_producer = None
            if topic_name in KafkaMessageHandler._known_topics:
                KafkaMessageHandler._known_topics.remove(topic_name)
            return True
        except Exception as e:
            self._record_broker_failure(e)
            return {'error': str(e)}

    @api.model
    def get_kafka_consumer_groups(self):
        """Fetch all consumer groups from Kafka"""
        if self._is_circuit_open():
            return {'error': f"Broker unavailable (Circuit Breaker active: {self._broker_status['failure_message']})"}

        client_info = self._get_client_conf(client_type='admin-groups')
        if client_info.get('error'):
            return {'error': client_info.get('message')}

        try:
            admin_client = AdminClient(client_info['conf'])
            future = admin_client.list_consumer_groups()
            groups_result = future.result()
            self._record_broker_success()
            return sorted([g.group_id for g in groups_result.valid])
        except Exception as e:
            self._record_broker_failure(e)
            _logger.error("Failed to get consumer groups: %s", e, exc_info=True)
            return {'error': str(e)}

    @api.model
    def get_kafka_consumer_group_details(self, group_id):
        """Fetch details (offsets, lag) for a consumer group"""
        if self._is_circuit_open():
            return {'error': f"Broker unavailable (Circuit Breaker active: {self._broker_status['failure_message']})"}

        client_info = self._get_client_conf(client_type='group-details')
        if client_info.get('error'):
            return {'error': client_info.get('message')}

        conf = client_info['conf']
        try:
            admin_client = AdminClient(conf)
            future = admin_client.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group_id)])
            res = future.result()
            self._record_broker_success()
            group_partition_offsets = res.get(group_id)

            consumer_conf = dict(conf)
            consumer_conf.update({
                'group.id': f"odoo-lag-group-{uuid.uuid4().hex[:8]}",
            })

            consumer = Consumer(consumer_conf)
            try:
                details = []
                if group_partition_offsets:
                    for tp in group_partition_offsets.topic_partitions:
                        current_offset = tp.offset

                        try:
                            low, high = consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=3.0)
                            log_end_offset = high
                        except Exception:
                            log_end_offset = 0

                        offset_val = current_offset if current_offset >= 0 else 0
                        lag = max(0, log_end_offset - offset_val)

                        details.append({
                            'topic': tp.topic,
                            'partition': tp.partition,
                            'current_offset': current_offset if current_offset >= 0 else None,
                            'log_end_offset': log_end_offset,
                            'lag': lag
                        })
            finally:
                consumer.close()

            details.sort(key=lambda x: (x['topic'], x['partition']))
            return details
        except Exception as e:
            self._record_broker_failure(e)
            _logger.error("Failed to get group details for %s: %s", group_id, e, exc_info=True)
            return {'error': str(e)}

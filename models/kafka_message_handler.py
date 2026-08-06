import json
import socket
import os
import uuid
import logging
import time
from datetime import datetime
import odoo
from odoo import api, fields, models, SUPERUSER_ID
from odoo.exceptions import UserError
import queue
import threading

_logger = logging.getLogger(__name__)

# Send Queue and Workers
_send_queue = queue.Queue()
_workers = []
_workers_lock = threading.Lock()


class KafkaMessageHandler(models.Model):
    _name = 'kafka.message.handler'
    _description = 'Kafka Message Handler'
    _order = 'sent_date desc, id desc'

    _cached_producers = {}
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

    def _get_kafka_config(self):
        """Retrieve Kafka configuration and ensure os.environ is set for AWS."""
        Config = self.env['ir.config_parameter'].sudo()
        brokers = Config.get_param('kafka_producer.broker_servers', '')
        aws_access_key = Config.get_param('kafka_producer.aws_access_key_id', '')
        aws_secret_key = Config.get_param('kafka_producer.aws_secret_access_key', '')
        aws_region = Config.get_param('kafka_producer.aws_region', 'us-east-1')

        if aws_access_key:
            os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key
        if aws_secret_key:
            os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_key
        if aws_region:
            os.environ['AWS_REGION'] = aws_region

        return {
            'brokers': [b.strip() for b in brokers.split(',') if b.strip()],
            'region': aws_region,
            'aws_access_key': aws_access_key,
            'aws_secret_key': aws_secret_key,
        }

    def _get_producer(self, model_info=None):
        """Return cached producer or create a new one."""
        from confluent_kafka import Producer
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        config = self._get_kafka_config()
        brokers = config['brokers']
        region = config['region']

        if not brokers:
            msg = "Missing configuration: BROKER_SERVERS"
            _logger.error(msg)
            return {'producer': None, 'error': True, 'message': msg}

        if model_info:
            if model_info.use_uniques_bss:
                brokers = [b['bootstrap_server'] for b in model_info.bootstrap_servers if b['bootstrap_server']]
            else:
                extra_brokers = [b['bootstrap_server'] for b in model_info.bootstrap_servers if b['bootstrap_server']]
                brokers = brokers + extra_brokers

        if not brokers:
            return {'producer': None, 'error': True, 'message': "No bootstrap servers found"}

        brokers_key = ','.join(sorted(brokers))
        if brokers_key in KafkaMessageHandler._cached_producers:
            return {'producer': KafkaMessageHandler._cached_producers[brokers_key], 'error': False}

        Config = self.env['ir.config_parameter'].sudo()
        use_sasl_iam = Config.get_param('kafka_producer.use_sasl_iam', 'True') == 'True'

        conf = {
            'bootstrap.servers': ','.join(brokers),
            'client.id': f"{socket.gethostname()}-producer",
            'acks': '1',
            'retries': 3,
            'request.timeout.ms': 25000,
            'linger.ms': 5,
        }

        if use_sasl_iam:
            if not config['aws_access_key'] or not config['aws_secret_key'] or not region:
                msg = "Missing AWS credentials for MSK IAM authentication."
                _logger.error(msg)
                return {'producer': None, 'error': True, 'message': msg}
            
            conf.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'OAUTHBEARER',
                'oauth_cb': lambda x: (MSKAuthTokenProvider.generate_auth_token(region)[0], MSKAuthTokenProvider.generate_auth_token(region)[1] / 1000.0),
            })

        try:
            producer = Producer(conf)
            KafkaMessageHandler._cached_producers[brokers_key] = producer
            return {'producer': producer, 'error': False}
        except Exception as e:
            _logger.error("Error creating Kafka producer: %s", e)
            return {'producer': None, 'error': True, 'message': f"Error creating producer: {e}"}

    def _ensure_topic_exists(self, topic_name, model_info=None):
        """Ensure topic exists, creating it if necessary."""
        from confluent_kafka.admin import AdminClient, NewTopic
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        if topic_name in KafkaMessageHandler._known_topics:
            return True

        config = self._get_kafka_config()
        brokers = config['brokers']
        region = config['region']

        if model_info:
            if model_info.use_uniques_bss:
                brokers = [b['bootstrap_server'] for b in model_info.bootstrap_servers if b['bootstrap_server']]
            else:
                extra_brokers = [b['bootstrap_server'] for b in model_info.bootstrap_servers if b['bootstrap_server']]
                brokers = brokers + extra_brokers
        
        # Filter out empty strings from brokers list
        brokers = [b for b in brokers if b]
        
        if not brokers:
            _logger.error("No brokers configured for Kafka. Cannot ensure topic %s exists.", topic_name)
            return False

        Config = self.env['ir.config_parameter'].sudo()
        use_sasl_iam = Config.get_param('kafka_producer.use_sasl_iam', 'True') == 'True'

        _logger.debug("Ensuring topic %s exists using brokers: %s", topic_name, brokers)

        conf = {
            'bootstrap.servers': ','.join(brokers),
            'client.id': f"{socket.gethostname()}-admin-check",
            'request.timeout.ms': 10000
        }

        if use_sasl_iam:
            if not region:
                _logger.error("AWS_REGION not configured. Cannot ensure topic %s exists.", topic_name)
                return False
            conf.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'OAUTHBEARER',
                'oauth_cb': lambda x: (MSKAuthTokenProvider.generate_auth_token(region)[0], MSKAuthTokenProvider.generate_auth_token(region)[1] / 1000.0),
            })

        try:
            admin_client = AdminClient(conf)
            topics_metadata = admin_client.list_topics(timeout=10.0)
            existing_topics = topics_metadata.topics
            if topic_name in existing_topics:
                _logger.debug("Topic %s already exists in Kafka", topic_name)
                KafkaMessageHandler._known_topics.add(topic_name)
                return True

            # Create topic
            _logger.info("Topic %s does not exist, attempting to create it", topic_name)
            broker_count = len(brokers)
            replication_factor = min(3, broker_count)
            
            try:
                topic = NewTopic(topic_name, num_partitions=1, replication_factor=replication_factor)
                futures = admin_client.create_topics([topic], operation_timeout=10.0)
                for topic, future in futures.items():
                    future.result()
                KafkaMessageHandler._known_topics.add(topic_name)
                _logger.info("Successfully created Kafka topic: %s with rf=%s", topic_name, replication_factor)
            except Exception as e:
                _logger.warning("Failed to create topic %s with rf=%s: %s. Retrying with rf=1", 
                               topic_name, replication_factor, e)
                try:
                    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
                    futures = admin_client.create_topics([topic], operation_timeout=10.0)
                    for topic, future in futures.items():
                        future.result()
                    KafkaMessageHandler._known_topics.add(topic_name)
                    _logger.info("Successfully created Kafka topic %s with rf=1", topic_name)
                except Exception as e2:
                    _logger.error("Failed to create topic %s even with rf=1: %s", topic_name, e2)
                    return False
            
            return True
        except Exception as e:
            _logger.error("Error in _ensure_topic_exists for %s: %s", topic_name, e)
            return False

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if 'sent_status' not in vals:
                vals['sent_status'] = 'pending'
        records = super().create(vals_list)
        
        self._ensure_workers_started()
        dbname = self.env.cr.dbname
        record_ids = records.ids
        
        self.env.cr.postcommit.add(
            lambda: self._queue_message_ids(dbname, record_ids)
        )
        return records

    def action_retry(self):
        records_not_sent = self.filtered(lambda r: r.sent_status != 'sent')
        if not records_not_sent:
            records_not_sent = self.search([('sent_status', '!=', 'sent')])
            
        if records_not_sent:
            records_not_sent.write({
                'sent_status': 'pending',
                'error_message': False,
            })
            self._ensure_workers_started()
            dbname = self.env.cr.dbname
            record_ids = records_not_sent.ids
            self.env.cr.postcommit.add(
                lambda: self._queue_message_ids(dbname, record_ids)
            )
        else:
            raise UserError("No records to retry")

    @classmethod
    def _queue_message_ids(cls, dbname, ids):
        for rec_id in ids:
            _send_queue.put((dbname, rec_id))

    def _ensure_workers_started(self):
        global _workers
        Config = self.env['ir.config_parameter'].sudo()
        try:
            max_workers = int(Config.get_param('kafka_producer.max_workers', '4'))
        except ValueError:
            max_workers = 4
        if max_workers <= 0:
            max_workers = 1
            
        with _workers_lock:
            current_worker_count = len(_workers)
            if current_worker_count < max_workers:
                needed = max_workers - current_worker_count
                _logger.info("Starting %d additional Kafka sender worker threads (total %d)", needed, max_workers)
                for i in range(needed):
                    t = threading.Thread(
                        target=self._queue_worker_loop,
                        daemon=True,
                        name=f"KafkaSenderWorker-{current_worker_count + i}"
                    )
                    t.start()
                    _workers.append(t)

    @classmethod
    def _queue_worker_loop(cls):
        while True:
            try:
                dbname, record_id = _send_queue.get()
                items = [(dbname, record_id)]
                try:
                    while len(items) < 100:
                        db, rec_id = _send_queue.get_nowait()
                        items.append((db, rec_id))
                except queue.Empty:
                    pass
                
                by_db = {}
                for db, rec_id in items:
                    by_db.setdefault(db, []).append(rec_id)
                
                for db, rec_ids in by_db.items():
                    cls._process_queue_batch(db, rec_ids)
                    
                for _ in range(len(items)):
                    _send_queue.task_done()
            except Exception as e:
                _logger.error("Exception in Kafka sender worker loop: %s", e, exc_info=True)
                time.sleep(1)

    @classmethod
    def _process_queue_batch(cls, dbname, record_ids):
        from confluent_kafka import Producer
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        try:
            with odoo.modules.registry.Registry(dbname).cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                records = env['kafka.message.handler'].browse(record_ids).exists()
                if not records:
                    return
                
                records_to_send = records.filtered(lambda r: r.sent_status in ('pending', 'failed'))
                if not records_to_send:
                    return
                
                producers_to_flush = set()
                
                for record in records_to_send:
                    model_name = record.topic.split('-_-')[0] if record.topic else False
                    model_info = env["followed.model"].sudo().search([("model.model", "=", model_name)], limit=1)
                    
                    record._ensure_topic_exists(record.topic, model_info=model_info)
                    kafka = record._get_producer(model_info=model_info)
                    
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
                            
                            try:
                                if record.message:
                                    msg_data = json.loads(record.message)
                                    if isinstance(msg_data, str):
                                        msg_data = json.loads(msg_data)
                                    if isinstance(msg_data, dict) and 'odoo_internal_id' in msg_data:
                                        record.message = str(msg_data['odoo_internal_id'])
                            except Exception as json_err:
                                _logger.warning("Could not parse message or find odoo_internal_id on queue success: %s", json_err)
                        except Exception as e:
                            record.sent_status = 'failed'
                            record.error_message = str(e)
                    else:
                        record.sent_status = 'not_configured'
                        if kafka:
                            record.error_message = kafka.get('message')
                            
                for producer in producers_to_flush:
                    try:
                        producer.flush()
                    except Exception as flush_err:
                        _logger.error("Error flushing Kafka producer in worker: %s", flush_err)
                cr.commit()
        except Exception as e:
            _logger.error("Error in _process_queue_batch for DB %s, records %s: %s", dbname, record_ids, e, exc_info=True)

    @api.model
    def get_kafka_topics(self):
        """Fetch all topics from Kafka with message counts"""
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        # Trigger cached setup or get config
        kafka_info = self._get_producer() 
        if kafka_info.get('error'):
            return {'error': kafka_info.get('message')}

        try:
            config = self._get_kafka_config()
            brokers = config['brokers']
            region = config['region']
            Config = self.env['ir.config_parameter'].sudo()
            use_sasl_iam = Config.get_param('kafka_producer.use_sasl_iam', 'True') == 'True'

            tp_lambda = lambda x: (MSKAuthTokenProvider.generate_auth_token(region)[0], MSKAuthTokenProvider.generate_auth_token(region)[1] / 1000.0)

            unique_suffix = uuid.uuid4().hex[:8]

            conf = {
                'bootstrap.servers': ','.join(brokers),
                'client.id': f"{socket.gethostname()}-admin-query-{unique_suffix}",
                'request.timeout.ms': 10000
            }
            if use_sasl_iam:
                conf.update({
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'OAUTHBEARER',
                    'oauth_cb': tp_lambda,
                })

            admin_client = AdminClient(conf)
            topics_metadata = admin_client.list_topics(timeout=10.0)
            topic_names = list(topics_metadata.topics.keys())

            # Now get counts for each topic using a consumer
            consumer_conf = {
                'bootstrap.servers': ','.join(brokers),
                'client.id': f"{socket.gethostname()}-count-query-{unique_suffix}",
                'group.id': f"{socket.gethostname()}-count-group-{unique_suffix}",
            }
            if use_sasl_iam:
                consumer_conf.update({
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'OAUTHBEARER',
                    'oauth_cb': tp_lambda,
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
                                low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
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
            _logger.error("Failed to get topics with counts: %s", e, exc_info=True)
            return {'error': str(e)}

    @api.model
    def get_kafka_messages(self, topic_name, count=4):
        """Fetch the last N messages from a topic"""
        from confluent_kafka import Consumer, TopicPartition
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        kafka_info = self._get_producer()
        if kafka_info.get('error'):
            return {'error': kafka_info.get('message')}

        try:
            config = self._get_kafka_config()
            brokers = config['brokers']
            region = config['region']
            Config = self.env['ir.config_parameter'].sudo()
            use_sasl_iam = Config.get_param('kafka_producer.use_sasl_iam', 'True') == 'True'

            tp_lambda = lambda x: (MSKAuthTokenProvider.generate_auth_token(region)[0], MSKAuthTokenProvider.generate_auth_token(region)[1] / 1000.0)

            unique_suffix = uuid.uuid4().hex[:8]

            consumer_conf = {
                'bootstrap.servers': ','.join(brokers),
                'client.id': f"{socket.gethostname()}-consumer-{unique_suffix}",
                'group.id': f"{socket.gethostname()}-consumer-group-{unique_suffix}",
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
            if use_sasl_iam:
                consumer_conf.update({
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'OAUTHBEARER',
                    'oauth_cb': tp_lambda,
                })

            consumer = Consumer(consumer_conf)
            try:
                topics_metadata = consumer.list_topics(topic_name, timeout=5.0)
                topic_meta = topics_metadata.topics.get(topic_name)
                if not topic_meta:
                    return []

                messages = []
                for p_id in topic_meta.partitions.keys():
                    tp = TopicPartition(topic_name, p_id)
                    try:
                        low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
                        start_offset = max(low, high - count)
                        
                        tp.offset = start_offset
                        consumer.assign([tp])
                        
                        num_to_read = high - start_offset
                        read_count = 0
                        while read_count < num_to_read:
                            msg = consumer.poll(timeout=1.0)
                            if msg is None:
                                break
                            if msg.error():
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
            _logger.error("Failed to get messages: %s", e, exc_info=True)
            return {'error': str(e)}

    @api.model
    def delete_kafka_topic(self, topic_name):
        """Delete a topic from Kafka"""
        from confluent_kafka.admin import AdminClient
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        kafka_info = self._get_producer()
        if kafka_info.get('error'):
            return {'error': kafka_info.get('message')}

        try:
            config = self._get_kafka_config()
            brokers = config['brokers']
            region = config['region']
            Config = self.env['ir.config_parameter'].sudo()
            use_sasl_iam = Config.get_param('kafka_producer.use_sasl_iam', 'True') == 'True'

            conf = {
                'bootstrap.servers': ','.join(brokers),
                'client.id': f"{socket.gethostname()}-admin-delete",
                'request.timeout.ms': 10000
            }
            if use_sasl_iam:
                conf.update({
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'OAUTHBEARER',
                    'oauth_cb': lambda x: (MSKAuthTokenProvider.generate_auth_token(region)[0], MSKAuthTokenProvider.generate_auth_token(region)[1] / 1000.0),
                })

            admin_client = AdminClient(conf)
            futures = admin_client.delete_topics([topic_name], operation_timeout=10.0)
            for topic, future in futures.items():
                future.result()
            KafkaMessageHandler._cached_producers.clear()
            return True
        except Exception as e:
            return {'error': str(e)}

    @api.model
    def get_kafka_consumer_groups(self):
        """Fetch all consumer groups from Kafka"""
        from confluent_kafka.admin import AdminClient
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        kafka_info = self._get_producer()
        if kafka_info.get('error'):
            return {'error': kafka_info.get('message')}

        try:
            config = self._get_kafka_config()
            brokers = config['brokers']
            region = config['region']
            Config = self.env['ir.config_parameter'].sudo()
            use_sasl_iam = Config.get_param('kafka_producer.use_sasl_iam', 'True') == 'True'

            unique_suffix = uuid.uuid4().hex[:8]

            conf = {
                'bootstrap.servers': ','.join(brokers),
                'client.id': f"{socket.gethostname()}-admin-groups-{unique_suffix}",
                'request.timeout.ms': 10000
            }
            if use_sasl_iam:
                conf.update({
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'OAUTHBEARER',
                    'oauth_cb': lambda x: (MSKAuthTokenProvider.generate_auth_token(region)[0], MSKAuthTokenProvider.generate_auth_token(region)[1] / 1000.0),
                })

            admin_client = AdminClient(conf)
            future = admin_client.list_consumer_groups()
            groups_result = future.result()
            return sorted([g.group_id for g in groups_result.valid])
        except Exception as e:
            _logger.error("Failed to get consumer groups: %s", e, exc_info=True)
            return {'error': str(e)}

    @api.model
    def get_kafka_consumer_group_details(self, group_id):
        """Fetch details (offsets, lag) for a consumer group"""
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient
        try:
            from confluent_kafka.admin import ConsumerGroupTopicPartitions
        except ImportError:
            from confluent_kafka.admin import _ConsumerGroupTopicPartitions as ConsumerGroupTopicPartitions
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        kafka_info = self._get_producer()
        if kafka_info.get('error'):
            return {'error': kafka_info.get('message')}

        try:
            config = self._get_kafka_config()
            brokers = config['brokers']
            region = config['region']
            Config = self.env['ir.config_parameter'].sudo()
            use_sasl_iam = Config.get_param('kafka_producer.use_sasl_iam', 'True') == 'True'

            tp_lambda = lambda x: (MSKAuthTokenProvider.generate_auth_token(region)[0], MSKAuthTokenProvider.generate_auth_token(region)[1] / 1000.0)

            unique_suffix = uuid.uuid4().hex[:8]

            conf = {
                'bootstrap.servers': ','.join(brokers),
                'client.id': f"{socket.gethostname()}-admin-group-details-{unique_suffix}",
                'request.timeout.ms': 10000
            }
            if use_sasl_iam:
                conf.update({
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'OAUTHBEARER',
                    'oauth_cb': tp_lambda,
                })

            admin_client = AdminClient(conf)
            
            futures_dict = admin_client.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group_id)])
            future = futures_dict.get(group_id)
            group_partition_offsets = future.result() if future else None

            # Use a consumer to get end offsets
            consumer_conf = {
                'bootstrap.servers': ','.join(brokers),
                'client.id': f"{socket.gethostname()}-lag-query-{unique_suffix}",
                'group.id': f"{socket.gethostname()}-lag-group-{unique_suffix}",
            }
            if use_sasl_iam:
                consumer_conf.update({
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'OAUTHBEARER',
                    'oauth_cb': tp_lambda,
                })

            consumer = Consumer(consumer_conf)
            try:
                details = []
                if group_partition_offsets:
                    for tp in group_partition_offsets.topic_partitions:
                        current_offset = tp.offset
                        
                        try:
                            low, high = consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=5.0)
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
            
            # Sort by topic and partition
            details.sort(key=lambda x: (x['topic'], x['partition']))
            return details
        except Exception as e:
            _logger.error("Failed to get group details for %s: %s", group_id, e, exc_info=True)
            return {'error': str(e)}
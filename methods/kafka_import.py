import json
import traceback
from odoo import fields, models, api
from . import utilities
import logging

_logger = logging.getLogger(__name__)

class KafkaImport(models.AbstractModel):
    _inherit = 'base'

    def kafka_instance(self):
        kafka_config = self.env['kafka.config'].search([], limit=1)
        _logger.info(f"Kafka Config: {kafka_config}")
        if not kafka_config:
            return False
        kafka = utilities.KafkaProducerUtilities(
            odoo_env = self.env, 
            message = None,
            topic = None,
            server = kafka_config.bootstrap_server, 
            is_ssl = kafka_config.is_ssl, 
            ssl_cert_attachment_id = kafka_config.ssl_cert_attachment_id,
            ssl_key_attachment_id = kafka_config.ssl_key_attachment_id,
            ssl_ca_attachment_id = kafka_config.ssl_ca_attachment_id
        )
        kafka.defineProducer()
        return kafka
        
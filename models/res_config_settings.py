# -*- coding: utf-8 -*-
from odoo import fields, models

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    kafka_producer_broker_servers = fields.Char(
        string="Kafka Broker Servers",
        config_parameter='kafka_producer.broker_servers',
        help="Comma-separated list of Kafka broker servers (e.g., broker1:9098,broker2:9098)"
    )
    kafka_producer_aws_access_key_id = fields.Char(
        string="AWS Access Key ID",
        config_parameter='kafka_producer.aws_access_key_id',
        help="AWS Access Key for IAM user with MSK permissions"
    )
    kafka_producer_aws_secret_access_key = fields.Char(
        string="AWS Secret Access Key",
        config_parameter='kafka_producer.aws_secret_access_key',
        help="AWS Secret Access Key for IAM user with MSK permissions"
    )
    kafka_producer_aws_region = fields.Char(
        string="AWS Region",
        config_parameter='kafka_producer.aws_region',
        default="us-east-1",
        help="AWS Region where the MSK cluster is located"
    )

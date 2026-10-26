# -*- coding: utf-8 -*-
from odoo import fields, models

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    kafka_producer_broker_servers = fields.Char(
        string="Kafka Broker Servers",
        config_parameter='kafka_producer.broker_servers',
        help="Comma-separated list of Kafka broker servers (e.g., broker1:9098,broker2:9098)"
    )
    kafka_producer_accepted_origin = fields.Char(
        string="Accepted Origin URLs",
        config_parameter='kafka_producer.accepted_origin',
        help="Comma-separated list of allowed origins (e.g., https://somosreyes.odoo.com, http://localhost:8070). Publishing is blocked if origin does not match."
    )
    kafka_producer_auth_type = fields.Selection([
        ('aws_iam', 'AWS MSK IAM (OAUTHBEARER)'),
        ('sasl_scram', 'SASL / SCRAM-SHA-256 (Redpanda / Confluent)'),
        ('plaintext', 'Plaintext (Sin autenticación)')
    ], string="Authentication Mechanism", config_parameter='kafka_producer.auth_type', default='sasl_scram')

    # AWS MSK IAM Credentials
    kafka_producer_use_sasl_iam = fields.Boolean(
        string="Use AWS IAM Authentication (Legacy)",
        config_parameter='kafka_producer.use_sasl_iam',
        default=False,
        help="Legacy parameter. Recommended to use kafka_producer_auth_type."
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

    # Redpanda / Confluent SASL Credentials
    kafka_producer_sasl_username = fields.Char(
        string="SASL Username",
        config_parameter='kafka_producer.sasl_username',
        help="Username for SASL authentication (e.g. admin)"
    )
    kafka_producer_sasl_password = fields.Char(
        string="SASL Password",
        config_parameter='kafka_producer.sasl_password',
        help="Password for SASL authentication"
    )
    kafka_producer_sasl_mechanism = fields.Selection([
        ('SCRAM-SHA-256', 'SCRAM-SHA-256'),
        ('SCRAM-SHA-512', 'SCRAM-SHA-512'),
        ('PLAIN', 'PLAIN')
    ], string="SASL Mechanism", config_parameter='kafka_producer.sasl_mechanism', default='SCRAM-SHA-256')
    kafka_producer_security_protocol = fields.Selection([
        ('SASL_SSL', 'SASL_SSL (TLS Cifrado)'),
        ('SASL_PLAINTEXT', 'SASL_PLAINTEXT'),
        ('PLAINTEXT', 'PLAINTEXT')
    ], string="Security Protocol", config_parameter='kafka_producer.security_protocol', default='SASL_SSL')

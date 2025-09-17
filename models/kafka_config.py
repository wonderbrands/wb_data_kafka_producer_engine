from odoo import models, fields, api


class KafkaConfig(models.Model):
    _name = 'kafka.config'
    _description = 'Kafka Configuration'
    _rec_name = 'bootstrap_server'


    bootstrap_server = fields.Char(string='Bootstrap Servers', required=True)
    description = fields.Text(string='Description')
    is_ssl = fields.Boolean(string='Use SSL', default=False)
    ssl_ca_attachment_id = fields.Binary(
        string="SSL CA Certificate File"
    )
    ssl_cert_attachment_id = fields.Binary(
        string="SSL Client Certificate File"
    )
    ssl_key_attachment_id = fields.Binary(
        string="SSL Client Private Key File"
    )
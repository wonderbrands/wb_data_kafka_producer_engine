from odoo import models, fields, api


class KafkaConfig(models.Model):
    _name = 'kafka.config'
    _description = 'Kafka Configuration'
    _rec_name = 'bootstrap_server'


    bootstrap_server = fields.Char(string='Bootstrap Servers', required=True)
    description = fields.Text(string='Description')
    is_ssl = fields.Boolean(string='Use SSL', default=False)
    cert = fields.Binary(string='Certificate')
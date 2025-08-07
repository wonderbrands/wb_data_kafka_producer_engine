from odoo import fields, models, api

class KafkaError(models.Model):
    _name = 'kafka.error'
    _description = 'Kafka Error'

    error = fields.Char(
        string = "Error",
    )

    at = fields.Datetime(
        string = "At",
    )
    
    msg = fields.Text(
        string = "Message",
    )
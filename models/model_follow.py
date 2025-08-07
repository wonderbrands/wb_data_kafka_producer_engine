from odoo import models, fields, api

class FieldFollowed(models.Model):
    _inherit = 'ir.model.fields'

    model_follow_id = fields.Many2one(
        comodel_name = "model.follow",
        string = "Model follow",
    )

    follow_on_create = fields.Boolean(
        string = "Follow on Create",
    )

    follow_on_update = fields.Boolean(
        string = "Follow on Update",
    )

    follow_on_delete = fields.Boolean(
        string = "Follow on Delete",
    )

class Modelfollow(models.Model):
    _name = 'model.follow'
    _description = 'Model follow'
    _rec_name = 'model'

    model = fields.Many2one(
        comodel_name = "ir.model",
        string = "Model",
        unique = True
    )

    fields_related = fields.One2many(
        comodel_name = "ir.model.fields",
        inverse_name = "model_follow_id",
        string = "Fields",
    )

    topic = fields.Char(
        string = "Topic",
    )

    kafka_config_id = fields.Many2one(
        comodel_name = "kafka.config",
        string = "Kafka Config",
    )

    @api.onchange("model") 
    def _onchange_(self):
        self.fields_related = False

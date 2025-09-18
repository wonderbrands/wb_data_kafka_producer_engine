from odoo import models, fields, api


class FollowedModel(models.Model):
    _name = 'followed.model'
    _description = 'Followed Model'

    model = fields.Many2one('ir.model', 'Model')
from odoo import models, fields, api

class BootstrapServers(models.Model):
    _name = 'bootstrap.servers'
    _description = 'Bootstrap Servers'

    bootstrap_server = fields.Char('Bootstrap Servers')
    followed_model = fields.Many2one('followed.model', 'Followed Model')

class FollowedModel(models.Model):
    _name = 'followed.model'
    _description = 'Followed Model'

    model = fields.Many2one('ir.model', 'Model')
    bootstrap_servers = fields.One2many('bootstrap.servers', 'followed_model', 'Bootstrap Servers')
    use_uniques_bss = fields.Boolean('Use Uniques Bootstrap Servers')
    api_like = fields.Boolean('API Like')
    schema_like = fields.Boolean('Schema Like')


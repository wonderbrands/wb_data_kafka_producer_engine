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

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        for record in records:
            record._create_kafka_topics()
        return records

    def write(self, vals):
        res = super().write(vals)
        if any(field in vals for field in ['api_like', 'schema_like', 'model', 'bootstrap_servers', 'use_uniques_bss']):
            for record in self:
                record._create_kafka_topics()
        return res

    def _create_kafka_topics(self):
        for record in self:
            if not record.model:
                continue
            
            handler = self.env['kafka.message.handler']
            if record.api_like:
                topic_name = f"{record.model.model}-_-api_like"
                handler._ensure_topic_exists(topic_name, model_info=record)
            
            if record.schema_like:
                topic_name = f"{record.model.model}-_-schema_like"
                handler._ensure_topic_exists(topic_name, model_info=record)


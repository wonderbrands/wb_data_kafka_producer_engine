from odoo import models, fields, api
import logging
import datetime
import json

_logger = logging.getLogger(__name__)

class Dump(models.Model):
    _name = 'dump'
    _description = 'Dump'

    model = fields.Many2one('ir.model', 'Model', required=True, ondelete='cascade')
    since = fields.Datetime(required=True)
    progress = fields.Integer(default=0, readonly=True)
    total = fields.Integer(default=0, readonly=True)

    @api.model
    def create(self, vals):
        record = super().create(vals)
        record._process_dump()
        return record

    def _process_dump(self):
        """Process dump and create Kafka messages"""
        self.ensure_one()
        
        try:
            model_name = self.model.model
            to_dump = self.env[model_name].search([
                ('create_date', '>=', self.since)
            ])
            
            self.write({'total': len(to_dump)})
            _logger.info("Starting dump of %s records from %s", len(to_dump), model_name)
            
            # Process in batches
            batch_size = 100
            progress = 0
            
            for i in range(0, len(to_dump), batch_size):
                batch = to_dump[i:i + batch_size]
                
                for rec in batch:
                    try:
                        # Read all fields from the record
                        message_data = rec.read()[0]
                        
                        # Create Kafka message directly in kafka.message.handler
                        self.env['kafka.message.handler'].create({
                            'message': json.dumps(message_data, default=str),
                            'topic': model_name,
                            'operation_type': 'dump',
                            'sent_status': 'pending'
                        })
                        
                        progress += 1
                        
                    except Exception as e:
                        _logger.error("Failed to dump record %s.%s: %s", 
                                    rec._name, rec.id, e, exc_info=True)
                        continue
                
                # Update progress and commit after each batch
                self.write({'progress': progress})
                self.env.cr.commit()
            
            _logger.info("Dump completed: %s/%s records", progress, self.total)
            
        except Exception as e:
            _logger.error("Dump failed: %s", e, exc_info=True)
            raise
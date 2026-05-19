from odoo import models, fields, api
import logging
import json
import odoo
from concurrent.futures import ThreadPoolExecutor

_logger = logging.getLogger(__name__)

# Global thread pool for dumps
dump_executor = ThreadPoolExecutor(max_workers=2)

class Dump(models.Model):
    _name = 'dump'
    _description = 'Dump'

    model = fields.Many2one('ir.model', 'Model', required=True, ondelete='cascade')
    start_id = fields.Integer(string='Start ID', required=True, default=0)
    end_id = fields.Integer(string='End ID', required=True, default=0)
    progress = fields.Integer(default=0, readonly=True)
    total = fields.Integer(default=0, readonly=True)

    @api.model
    def create(self, vals):
        record = super().create(vals)
        # Process in background
        dump_executor.submit(record._process_dump_job, self.env.cr.dbname, record.id)
        return record

    def _process_dump_job(self, dbname, dump_id):
        """Worker executed in a thread with a fresh DB cursor"""
        try:
            with odoo.registry(dbname).cursor() as cr:
                env = api.Environment(cr, odoo.SUPERUSER_ID, {})
                dump_record = env['dump'].browse(dump_id)
                dump_record._process_dump()
                cr.commit()
        except Exception as e:
            _logger.error("Dump background job failed: %s", e, exc_info=True)

    def _process_dump(self):
        """Process dump and create Kafka messages - should be called within a background job context"""
        self.ensure_one()
        
        try:
            model_name = self.model.model
            # Search by ID range
            domain = [('id', '>=', self.start_id), ('id', '<=', self.end_id)]
            # Use search to get IDs, then browse in batches to avoid loading all records at once if many
            to_dump_ids = self.env[model_name].search(domain, order='id asc').ids
            
            total_count = len(to_dump_ids)
            self.write({'total': total_count})
            self.env.cr.commit() # Update total immediately so progress bar knows the limit
            
            _logger.info("Starting dump of %s records from %s (IDs %s to %s)", 
                         total_count, model_name, self.start_id, self.end_id)
            
            # Process in batches
            batch_size = 100
            progress = 0
            
            for i in range(0, total_count, batch_size):
                batch_ids = to_dump_ids[i:i + batch_size]
                batch_records = self.env[model_name].browse(batch_ids)
                
                for rec in batch_records:
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
            
            _logger.info("Dump completed: %s/%s records", progress, total_count)
            
        except Exception as e:
            _logger.error("Dump failed: %s", e, exc_info=True)

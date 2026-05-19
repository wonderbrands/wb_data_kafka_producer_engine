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
    api_like = fields.Boolean('API Like', default=True)
    schema_like = fields.Boolean('Schema Like', default=False)
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
            
            # Check topic existence for enabled modes
            model_info = self.env["followed.model"].search([("model", "=", self.model.id)], limit=1)
            
            api_topic_ok = False
            if self.api_like:
                api_topic_ok = self.env['kafka.message.handler']._ensure_topic_exists(f"{model_name}-_-api_like", model_info=model_info)
                if not api_topic_ok:
                    _logger.error("Topic %s-_-api_like does not exist and could not be created.", model_name)

            schema_topic_ok = False
            if self.schema_like:
                schema_topic_ok = self.env['kafka.message.handler']._ensure_topic_exists(f"{model_name}-_-schema_like", model_info=model_info)
                if not schema_topic_ok:
                    _logger.error("Topic %s-_-schema_like does not exist and could not be created.", model_name)

            if not api_topic_ok and not schema_topic_ok:
                _logger.warning("No valid topics found for dump. Aborting.")
                return

            # Process in batches
            batch_size = 100
            progress = 0
            
            for i in range(0, total_count, batch_size):
                batch_ids = to_dump_ids[i:i + batch_size]
                batch_records = self.env[model_name].browse(batch_ids)
                
                for rec in batch_records:
                    try:
                        # Process based on flags
                        if self.api_like and api_topic_ok:
                            data = self._prepare_vals(rec, rec.read()[0])
                            self.env['kafka.message.handler'].create({
                                'message': json.dumps(data, default=self._convert),
                                'topic': f"{model_name}-_-api_like",
                                'operation_type': 'dump',
                                'sent_status': 'pending',
                                'data_like': 'api_like'
                            })

                        if self.schema_like and schema_topic_ok:
                            data = self.query_id(rec.id, rec._name)
                            self.env['kafka.message.handler'].create({
                                'message': json.dumps(data, default=self._convert),
                                'topic': f"{model_name}-_-schema_like",
                                'operation_type': 'dump',
                                'sent_status': 'pending',
                                'data_like': 'schema_like'
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

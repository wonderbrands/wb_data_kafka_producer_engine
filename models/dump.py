from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import logging
import json
import odoo
import datetime
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

    @api.constrains('model', 'start_id', 'end_id')
    def _check_ids_exist(self):
        for record in self:
            if not record.model:
                continue
            model_name = record.model.model
            
            if record.start_id <= 0:
                raise ValidationError(_("El ID de inicio debe ser mayor a 0."))
            if record.end_id <= 0:
                raise ValidationError(_("El ID del final debe ser mayor a 0."))
            if record.start_id > record.end_id:
                raise ValidationError(_("El ID de inicio no puede ser mayor que el ID del final."))
            
            # Check existence of start_id
            start_record = self.env[model_name].search([('id', '=', record.start_id)], limit=1)
            if not start_record:
                raise ValidationError(_("El ID de inicio (%s) no existe en el modelo %s.") % (record.start_id, model_name))
            
            # Check existence of end_id
            end_record = self.env[model_name].search([('id', '=', record.end_id)], limit=1)
            if not end_record:
                raise ValidationError(_("El ID del final (%s) no existe en el modelo %s.") % (record.end_id, model_name))

    @api.model
    def create(self, vals):
        record = super().create(vals)
        # Process in background after the transaction has successfully committed
        self.env.cr.postcommit.add(
            lambda: dump_executor.submit(record._process_dump_job, self.env.cr.dbname, record.id)
        )
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
            
            # Check topic existence for enabled modes (once per dump for efficiency)
            model_info = self.env["followed.model"].sudo().search([("model", "=", self.model.id)], limit=1)
            
            if self.api_like:
                if not self.env['kafka.message.handler']._ensure_topic_exists(f"{model_name}-_-api_like", model_info=model_info):
                    _logger.warning("Topic %s-_-api_like does not exist and could not be created. Records will be created as pending.", model_name)

            if self.schema_like:
                if not self.env['kafka.message.handler']._ensure_topic_exists(f"{model_name}-_-schema_like", model_info=model_info):
                    _logger.warning("Topic %s-_-schema_like does not exist and could not be created. Records will be created as pending.", model_name)

            # Process in batches
            batch_size = 100
            progress = 0
            
            for i in range(0, total_count, batch_size):
                batch_ids = to_dump_ids[i:i + batch_size]
                batch_records = self.env[model_name].browse(batch_ids)
                
                handlers_to_create = []

                if self.api_like:
                    records_data = batch_records.read()
                    records_dict = {r['id']: r for r in records_data}

                if self.schema_like:
                    db_rows = self.query_ids(batch_ids, model_name)
                    db_rows_dict = {row['id']: row for row in db_rows}

                for rec in batch_records:
                    try:
                        # Process based on flags
                        if self.api_like and rec.id in records_dict:
                            data = self._prepare_vals(rec, records_dict[rec.id])
                            data.update({
                                "odoo_internal_id": rec.id,
                                "operation": "dump",
                                "by": self.env.user.name or "Odoo System",
                                "timestamp": datetime.datetime.utcnow().isoformat(),
                            })
                            handlers_to_create.append({
                                'message': json.dumps(data, default=self._convert),
                                'topic': f"{model_name}-_-api_like",
                                'operation_type': 'dump',
                                'sent_status': 'pending',
                                'data_like': 'api_like'
                            })

                        if self.schema_like and rec.id in db_rows_dict:
                            data = db_rows_dict[rec.id]
                            data.update({
                                "odoo_internal_id": rec.id,
                                "operation": "dump",
                                "by": self.env.user.name or "Odoo System",
                                "timestamp": datetime.datetime.utcnow().isoformat(),
                            })
                            handlers_to_create.append({
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
                
                if handlers_to_create:
                    self.env['kafka.message.handler'].create(handlers_to_create)

                # Update progress and commit after each batch
                self.write({'progress': progress})
                self.env.cr.commit()
            
            _logger.info("Dump completed: %s/%s records", progress, total_count)
            
        except Exception as e:
            _logger.error("Dump failed: %s", e, exc_info=True)

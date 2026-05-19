import json
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from odoo import models, api
import odoo

_logger = logging.getLogger(__name__)

# Global thread pool
executor = ThreadPoolExecutor(max_workers=4)


class KafkaAsyncMixin(models.AbstractModel):
    _inherit = "base"

    # ------------------------------
    # Helpers
    # ------------------------------
    def _convert(self, o):
        """Convert datetime to ISO string"""
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        return str(o)

    def _record_label(self, record):
        """
        Return a human-readable label for any record or list of records.
        Handles single record, recordset, list, or nested list.
        """
        if record is None:
            return "None"

        # If it's iterable (list, tuple, recordset), join recursively
        if isinstance(record, (list, tuple)):
            return ", ".join([self._record_label(r) for r in record])

        # Try display_name
        try:
            return record.display_name
        except Exception:
            # Try id
            try:
                return str(record.id)
            except Exception:
                # fallback to str
                return str(record)

    def _prepare_vals(self, record, vals):
        """
        Prepare a dictionary of vals, resolving relational fields to labels
        """
        processed = {}
        for field, value in vals.items():
            field_obj = record._fields.get(field)
            if not field_obj:
                processed[field] = value
                continue

            if field_obj.type == "many2one":
                if isinstance(value, int):
                    rec = record.env[field_obj.comodel_name].browse(value)
                    processed[field] = self._record_label(rec) if rec.exists() else value
                elif hasattr(value, "display_name"):
                    processed[field] = self._record_label(value)
                else:
                    processed[field] = value

            elif field_obj.type in ("one2many", "many2many"):
                # Handle lists of IDs or recordsets
                if isinstance(value, list) and value and isinstance(value[0], int):
                    recs = record.env[field_obj.comodel_name].browse(value)
                    processed[field] = [self._record_label(r) for r in recs if r.exists()]
                elif hasattr(value, "__iter__"):
                    processed[field] = [self._record_label(r) for r in value]
                else:
                    processed[field] = value

            else:
                processed[field] = value

        return processed

    def _background_job(self, messages, topic, operation_type, data_like):
        """Worker executed in a thread with a fresh DB cursor"""
        if not messages:
            return
        
        dbname = self.env.cr.dbname
        
        try:
            # Correct way to get registry
            with odoo.registry(dbname).cursor() as cr:
                # Use SUPERUSER_ID to avoid permission issues
                env = api.Environment(cr, odoo.SUPERUSER_ID, {})
                
                #_logger.info("========================================")
                #_logger.info("Starting background job with %d messages", len(messages))
                
                for msg in messages:
                    try:
                        #_logger.info("Processing message: %s", msg)
                        send = {
                            "message": msg,
                            "topic": f"{topic}-_-{data_like}",
                            "operation_type": operation_type,
                            "sent_status": "pending",
                            "data_like": data_like,
                        }
                        #_logger.info("Data to create: %s", send)
                        
                        record = env["kafka.message.handler"].create(send)
                        #_logger.info("Created record with ID: %s", record.id)
                        
                    except Exception as e:
                        _logger.error("Failed to create kafka.message.handler record: %s", e, exc_info=True)
                
                cr.commit()
                #_logger.info("Committed %d records successfully", len(messages))
                #_logger.info("========================================")
                
        except Exception as e:
            _logger.error("Background job failed completely: %s", e, exc_info=True)



    def _create_kafka_message_async(self, records, vals_list, operation_type, data_like):
        """
        Prepare messages for records and submit to executor.
        For create/write, vals_list is required. For delete, pass None.
        """
        messages = []
        if vals_list:
            for record, vals in zip(records, vals_list):
                if data_like == 'api_like':
                    processed_vals = self._prepare_vals(record, vals)
                else:
                    processed_vals = vals
                processed_vals.update({
                    "odoo_internal_id": record.id,
                    "operation": operation_type,
                    "by": self.env.user.name or "Odoo System",
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                })
                messages.append(json.dumps(processed_vals, default=self._convert))
        else:
            # For delete, use record info directly
            for record in records:
                data = {
                    "odoo_internal_id": record.id,
                    "record_name": self._record_label(record),
                    "model_name": record._name,
                    "operation": operation_type,
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                    "by": self.env.user.name or "Odoo System",
                }
                messages.append(json.dumps(data, default=self._convert))

        #_logger.info("========================================")
        #_logger.info("Sending Kafka messages for %s: %s", records)

        executor.submit(self._background_job, messages, records[0]._name if records else "unknown", operation_type, data_like)

    def query_id(self, rec_id, model_name, fields=None):
        model = self.env[model_name]
        table = model._table
        cr = self.env.cr
        
        if not fields:
            cr.execute(f"SELECT * FROM {table} WHERE id = %s", (rec_id,))
        else:
            cr.execute(f"SELECT {', '.join(fields)} FROM {table} WHERE id = %s", (rec_id,))
        row = cr.fetchone()
        
        if row is None:
            return None
        
        # Get column names from cursor description
        colnames = [desc[0] for desc in cr.description]
        result = dict(zip(colnames, row))
        #_logger.info("========================================")
        #_logger.info("getting query: %s", result)        
        return result



    def write(self, vals):
        is_followed = self.env["followed.model"].search([("model", "=", self._name)], limit=1)
        res = super().write(vals) 
        
        if is_followed and res:
            self.env.cr.flush()
            self.env.cr.commit()
            
            if is_followed.api_like:
                try:
                    vals_list = [vals] * len(self)  
                    self._create_kafka_message_async(self, vals_list=vals_list, operation_type="update", data_like="api_like")
                except Exception:
                    _logger.error("Error preparing Kafka messages for write", exc_info=True)

            if is_followed.schema_like:
                try:
                    # Now the SQL query will see the updated data
                    self._create_kafka_message_async(self, vals_list=[self.query_id(record.id, record._name) for record in self], operation_type="update", data_like="schema_like")
                except Exception:
                    _logger.error("Error preparing Kafka messages for write", exc_info=True)
        
        return res


    @api.model
    def create(self, vals):
        record = super().create(vals)
        is_followed = self.env["followed.model"].search([("model", "=", self._name)], limit=1)
        if is_followed: 
            if is_followed.api_like:
                try:
                    self._create_kafka_message_async(record, vals_list=[vals], operation_type="create", data_like="api_like")
                except Exception:
                    _logger.error("Error preparing Kafka messages for create", exc_info=True)

            if is_followed.schema_like:
                try:
                    self._create_kafka_message_async(record, vals_list=[self.query_id(record.id, record._name)], operation_type="create", data_like="schema_like")
                except Exception:
                    _logger.error("Error preparing Kafka messages for create", exc_info=True)
        return record


    # ------------------------------
    # Overridden unlink
    # ------------------------------
    def unlink(self):
        is_followed = self.env["followed.model"].search([("model", "=", self._name)], limit=1)
        if is_followed:
            records_to_notify = list(self)  # copy before deletion
            result = super().unlink()
            if records_to_notify:
                if is_followed.api_like:
                    try:
                        self._create_kafka_message_async(records_to_notify, vals_list=None, operation_type="delete", data_like="api_like")
                    except Exception:
                        _logger.error("Error preparing Kafka messages for delete", exc_info=True)

                if is_followed.schema_like:
                    try:
                        self._create_kafka_message_async(records_to_notify, vals_list=None, operation_type="delete", data_like="schema_like")
                    except Exception:
                        _logger.error("Error preparing Kafka messages for delete", exc_info=True)

            return result
        else:
            return super().unlink()

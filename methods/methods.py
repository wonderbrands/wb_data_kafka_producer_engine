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
                # Leverage Odoo's batch prefetching
                relation_record = record[field]
                if relation_record:
                    processed[field] = self._record_label(relation_record)
                else:
                    processed[field] = value

            elif field_obj.type in ("one2many", "many2many"):
                relation_records = record[field]
                if relation_records:
                    processed[field] = [self._record_label(r) for r in relation_records]
                else:
                    processed[field] = []

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
                
                # Check if topic exists/can be created (once per batch for efficiency)
                full_topic = f"{topic}-_-{data_like}"
                model_info = env["followed.model"].search([("model.model", "=", topic)], limit=1)
                if not env["kafka.message.handler"]._ensure_topic_exists(full_topic, model_info=model_info):
                    _logger.warning("Topic %s does not exist and could not be created. Proceeding to create records for later retry.", full_topic)

                vals_list = []
                for msg in messages:
                    vals_list.append({
                        "message": msg,
                        "topic": full_topic,
                        "operation_type": operation_type,
                        "sent_status": "pending",
                        "data_like": data_like,
                    })
                
                try:
                    env["kafka.message.handler"].create(vals_list)
                    cr.commit()
                except Exception as e:
                    cr.rollback()
                    _logger.error("Failed to batch create kafka.message.handler records: %s", e, exc_info=True)
                
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

    def query_ids(self, rec_ids, model_name, fields=None):
        if not rec_ids:
            return []
        model = self.env[model_name]
        table = model._table
        cr = self.env.cr
        
        ids_tuple = tuple(rec_ids)
        if len(ids_tuple) == 1:
            query_placeholder = "= %s"
            query_val = ids_tuple[0]
        else:
            query_placeholder = "IN %s"
            query_val = ids_tuple

        if not fields:
            cr.execute(f"SELECT * FROM {table} WHERE id {query_placeholder}", (query_val,))
        else:
            cols = [f'"{f}"' for f in fields]
            cr.execute(f"SELECT {', '.join(cols)} FROM {table} WHERE id {query_placeholder}", (query_val,))
            
        rows = cr.fetchall()
        if not rows:
            return []
            
        colnames = [desc[0] for desc in cr.description]
        return [dict(zip(colnames, row)) for row in rows]

    def query_id(self, rec_id, model_name, fields=None):
        res = self.query_ids([rec_id], model_name, fields)
        return res[0] if res else None


    def write(self, vals):
        is_followed = self.env["followed.model"].sudo().search([("model.model", "=", self._name)], limit=1)
        res = super().write(vals) 
        
        if is_followed and res:
            self.env.cr.flush()
            
            if is_followed.api_like:
                try:
                    vals_list = [vals] * len(self)  
                    self._create_kafka_message_async(self, vals_list=vals_list, operation_type="update", data_like="api_like")
                except Exception:
                    _logger.error("Error preparing Kafka messages for write", exc_info=True)

            if is_followed.schema_like:
                try:
                    # Batch fetch all database rows in one SQL query instead of N queries in a loop!
                    ids = self.ids
                    rows = self.query_ids(ids, self._name)
                    rows_by_id = {row['id']: row for row in rows}
                    vals_list = [rows_by_id[record.id] for record in self if record.id in rows_by_id]
                    if vals_list:
                        self._create_kafka_message_async(self, vals_list=vals_list, operation_type="update", data_like="schema_like")
                except Exception:
                    _logger.error("Error preparing Kafka messages for write", exc_info=True)
        
        return res


    @api.model
    def create(self, vals):
        record = super().create(vals)
        is_followed = self.env["followed.model"].sudo().search([("model.model", "=", self._name)], limit=1)
        if is_followed: 
            if is_followed.api_like:
                try:
                    self._create_kafka_message_async(record, vals_list=[vals], operation_type="create", data_like="api_like")
                except Exception:
                    _logger.error("Error preparing Kafka messages for create", exc_info=True)

            if is_followed.schema_like:
                try:
                    rows = self.query_ids([record.id], record._name)
                    if rows:
                        self._create_kafka_message_async(record, vals_list=[rows[0]], operation_type="create", data_like="schema_like")
                except Exception:
                    _logger.error("Error preparing Kafka messages for create", exc_info=True)
        return record


    # ------------------------------
    # Overridden unlink
    # ------------------------------
    def unlink(self):
        is_followed = self.env["followed.model"].sudo().search([("model.model", "=", self._name)], limit=1)
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

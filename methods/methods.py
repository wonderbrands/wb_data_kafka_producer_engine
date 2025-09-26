import json
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from odoo import models, api, registry

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

    def _background_job(self, messages, topic, operation_type):
        """Worker executed in a thread with a fresh DB cursor"""
        if not messages:
            return
        dbname = self.env.cr.dbname
        with registry(dbname).cursor() as cr:
            env = api.Environment(cr, self.env.uid, self.env.context)
            for msg in messages:
                env["kafka.message.handler"].create({
                    "message": msg,
                    "topic": topic,
                    "operation_type": operation_type,
                    "sent_status": "pending"
                })
            cr.commit()

    def _create_kafka_message_async(self, records, vals_list=None, operation_type="update"):
        """
        Prepare messages for records and submit to executor.
        For create/write, vals_list is required. For delete, pass None.
        """
        messages = []
        if vals_list:
            for record, vals in zip(records, vals_list):
                processed_vals = self._prepare_vals(record, vals)
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

        executor.submit(self._background_job, messages, records[0]._name if records else "unknown", operation_type)

    # ------------------------------
    # Overridden write
    # ------------------------------
    def write(self, vals):
        is_followed = self.env["followed.model"].search([("model", "=", self._name)], limit=1)
        res = super().write(vals) 
        if is_followed and res:
            try:
                vals_list = [vals] * len(self)  
                self._create_kafka_message_async(self, vals_list=vals_list, operation_type="update")
            except Exception:
                _logger.error("Error preparing Kafka messages for write", exc_info=True)
        return res


    @api.model
    def create(self, vals):
        record = super().create(vals)
        is_followed = self.env["followed.model"].search([("model", "=", self._name)], limit=1)
        if is_followed:
            try:
                self._create_kafka_message_async(record, vals_list=[vals], operation_type="create")
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
                try:
                    self._create_kafka_message_async(records_to_notify, vals_list=None, operation_type="delete")
                except Exception:
                    _logger.error("Error preparing Kafka messages for delete", exc_info=True)
            return result
        else:
            return super().unlink()

import json
import datetime
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from odoo import models, api
import odoo

_logger = logging.getLogger(__name__)

# Global thread pool helper
_executor = None
_executor_workers = 0
_executor_lock = threading.Lock()

def get_executor(env):
    global _executor, _executor_workers
    Config = env['ir.config_parameter'].sudo()
    try:
        max_workers = int(Config.get_param('kafka_producer.max_workers', '4'))
    except ValueError:
        max_workers = 4
    if max_workers <= 0:
        max_workers = 1
        
    with _executor_lock:
        if _executor is None or _executor_workers != max_workers:
            if _executor is not None:
                _executor.shutdown(wait=False)
            _executor = ThreadPoolExecutor(max_workers=max_workers)
            _executor_workers = max_workers
        return _executor


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
        res = super().write(vals)
        if self.env.registry.ready:
            self._event("on_records_write").notify(self, vals)
        return res

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        if self.env.registry.ready:
            self._event("on_records_create").notify(records, vals_list)
        return records

    def unlink(self):
        if self.env.registry.ready:
            self._event("on_records_unlink").notify(self)
        return super().unlink()

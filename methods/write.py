import json
import traceback
import datetime
from odoo import fields, models, api
import logging

_logger = logging.getLogger(__name__)

class WriteInhereit(models.AbstractModel):
    _inherit = 'base'

    def write(self, vals):
        is_followed = self.env["followed.model"].search([("model.name", "=", self._name)], limit=1)
        if is_followed:
            try:
                _logger.info("*************************************")
                _logger.info(f"Model: {self._name}")
                _logger.info(f"Values: {vals}")

                def convert(o):
                    if isinstance(o, (datetime.date, datetime.datetime)):
                        return o.isoformat()
                    return str(o)

                def prepare_vals(vals):
                    processed = {}
                    for field, value in vals.items():
                        field_obj = self._fields.get(field)
                        if not field_obj:
                            processed[field] = value
                            continue

                        if field_obj.type == 'many2one':
                            # If it's an int, fetch the record first
                            if isinstance(value, int):
                                record = self.env[field_obj.comodel_name].browse(value)
                                processed[field] = record.name if record.exists() else value
                            elif hasattr(value, 'name'):
                                processed[field] = value.name
                            else:
                                processed[field] = value

                        elif field_obj.type in ('one2many', 'many2many'):
                            # Usually these are lists of (6, 0, [ids]) or recordsets
                            if isinstance(value, list) and value and isinstance(value[0], int):
                                records = self.env[field_obj.comodel_name].browse(value)
                                processed[field] = [rec.name for rec in records if rec.exists()]
                            elif hasattr(value, '__iter__'):
                                processed[field] = [rec.name for rec in value]
                            else:
                                processed[field] = value

                        else:
                            processed[field] = value

                    return processed


                    processed_vals = prepare_vals(vals)
                    processed_vals["odoo_internal_id"] = self.id
                    processed_vals["operation"] = "write"
                    processed_vals["by"] = self.env.user.name
                    processed_vals["timestamp"] = datetime.datetime.utcnow().isoformat()

                    message = json.dumps(processed_vals, default=convert)
                    self.env["kafka.message.handler"].create({
                        "message": message,
                        "topic": self._name,
                        "operation_type": "update",
                        "sent_status": "pending"
                    })


                _logger.info("*************************************")
                return super().write(vals)
            except Exception as e:
                _logger.error(traceback.format_exc())
                return super().write(vals)
        else:
            return super().write(vals)


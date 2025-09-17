import json
import traceback
import datetime
from odoo import fields, models, api
from . import utilities
import logging

_logger = logging.getLogger(__name__)

class CreateInhereit(models.AbstractModel):
    _inherit = 'base'

    _cached_kafka = None  # Class-level cache (per model, per worker)

    def _get_kafka(self):
        """Return a cached Kafka instance if available, else create one."""
        if CreateInhereit._cached_kafka is None:
            CreateInhereit._cached_kafka = self.kafka_instance()
            _logger.info("Kafka instance initialized and cached.")
        return CreateInhereit._cached_kafka

    
    def create(self, vals_list):
        _logger.info(f"------------------------")
        _logger.info("creation")
        _logger.info(vals_list)
        _logger.info(f"------------------------")
        if self._name == "kafka.error":
            return super().create(vals_list)
        try:
            _logger.info("*************************************")
            _logger.info(f"Creating records.")
            _logger.info(f"Model: {self._name}")
            _logger.info(f"Values: {vals_list}")

            records = super().create(vals_list)  # First create all records
            kafka = self._get_kafka()
            _logger.info(f"Kafka: {kafka}")

            if not kafka:
                return records

            def convert(o):
                if isinstance(o, (datetime.date, datetime.datetime)):
                    return o.isoformat()
                return str(o)

            def prepare_vals(record, vals):
                processed = {}
                for field, value in vals.items():
                    field_obj = record._fields.get(field)
                    if not field_obj:
                        processed[field] = value
                        continue

                    if field_obj.type == 'many2one':
                        if isinstance(value, int):
                            rec = record.env[field_obj.comodel_name].browse(value)
                            processed[field] = rec.name if rec.exists() else value
                        elif isinstance(value, (tuple, list)) and len(value) >= 1 and isinstance(value[0], int):
                            rec = record.env[field_obj.comodel_name].browse(value[0])
                            processed[field] = rec.name if rec.exists() else value
                        else:
                            processed[field] = str(value)

                    elif field_obj.type in ('one2many', 'many2many'):
                        names = []
                        if isinstance(value, list):
                            for command in value:
                                if isinstance(command, (list, tuple)):
                                    if len(command) == 3 and isinstance(command[2], list):
                                        ids = command[2]
                                        recs = record.env[field_obj.comodel_name].browse(ids)
                                        names.extend([r.name for r in recs if r.exists()])
                                    elif len(command) >= 2 and isinstance(command[1], int):
                                        rec = record.env[field_obj.comodel_name].browse(command[1])
                                        if rec.exists():
                                            names.append(rec.name)
                                elif isinstance(command, int):
                                    rec = record.env[field_obj.comodel_name].browse(command)
                                    if rec.exists():
                                        names.append(rec.name)
                        processed[field] = names

                    else:
                        processed[field] = value

                return processed


                # Now vals_list and records are same-length, matched by index
            for record, vals in zip(records, vals_list):
                processed_vals = prepare_vals(record, vals)
                processed_vals["odoo_internal_id"] = record.id
                processed_vals["operation"] = "create"
                processed_vals["by"] = self.env.user.name
                processed_vals["timestamp"] = datetime.datetime.utcnow().isoformat()

                message = json.dumps(processed_vals, default=convert)
                kafka.set_message(message)
                kafka.set_topic(record._name)
                kafka.sendMessage()
                _logger.info(f"Message sent to Kafka: {message}")

            _logger.info("*************************************")
            return records

        except Exception as e:
            _logger.error(traceback.format_exc())
            _logger.error(f"Error sending message to Kafka: {e}")
            self.env["kafka.error"].create(
                {
                    "error": f"{str(e)} - {traceback.format_exc()}",
                    "at": datetime.datetime.utcnow(),
                    "msg": f"{self._name} - {vals_list}", 
                }
            )
            return super().create(vals_list)

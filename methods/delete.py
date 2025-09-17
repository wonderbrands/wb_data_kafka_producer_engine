import json
import traceback
import datetime
from odoo import fields, models, api
from . import utilities
import logging
import traceback

_logger = logging.getLogger(__name__)

class DeleteInhereit(models.AbstractModel):
    _inherit = 'base'

    _cached_kafka = None 

    def _get_kafka(self):
        """Return a cached Kafka instance if available, else create one."""
        if DeleteInhereit._cached_kafka is None:
            DeleteInhereit._cached_kafka = self.kafka_instance()
            _logger.info("Kafka instance initialized and cached.")
        return DeleteInhereit._cached_kafka

    def unlink(self):
        try:
            """
            Overrides unlink to send deletion details to Kafka.
            Handles multiple records and ensures the message is sent only after
            a successful database transaction.
            """
            kafka = self._get_kafka()

            # If Kafka isn't configured, just perform the original unlink and exit early.
            if not kafka:
                return super().unlink()

            # --- Step 1: Prepare all data BEFORE deleting ---
            records_to_notify = []
            for record in self:
                records_to_notify.append({
                    'odoo_internal_id': record.id,
                    'model_name': record._name,
                    'record_name': record[record._rec_name] if record._rec_name in record else f"ID: {record.id}",
                    'operation': 'delete',
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'by': self.env.user.name or 'Odoo System'
                })

            _logger.info("*************************************")
            _logger.info(f"Preparing to delete {len(self)} records.")
            _logger.info(f"Records to notify: {records_to_notify}")

            # --- Step 2: Perform the actual deletion ---
            result = super().unlink()
            
            # --- Step 3: Send notifications AFTER successful deletion ---
            # The 'result' check confirms the super call was successful.
            if result and records_to_notify:
                _logger.info(f"Deletion successful. Sending {len(records_to_notify)} messages to Kafka.")
                
                # --- THE FIX: Define the helper function ONCE, outside the loop ---
                def convert_to_iso(o):
                    if isinstance(o, (datetime.date, datetime.datetime)):
                        return o.isoformat()
                    # It's good practice for a converter to raise an error for unhandled types.
                    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

                for data in records_to_notify:
                    try:
                        message = json.dumps(data, default=convert_to_iso)
                        kafka.set_message(message)
                        kafka.set_topic(data['model_name'])
                        kafka.sendMessage()
                    except Exception as e:
                        _logger.error(f"Failed to send Kafka message for deleted record {data.get('odoo_internal_id')}: {e}")
            
            return result
        except Exception as e:
            _logger.error(f"Failed to delete records: {e}")
            _logger.error(traceback.format_exc())
            return super().unlink()
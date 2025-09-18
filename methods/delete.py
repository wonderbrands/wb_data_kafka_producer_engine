import json
import traceback
import datetime
from odoo import fields, models, api
import logging
import traceback

_logger = logging.getLogger(__name__)

class DeleteInhereit(models.AbstractModel):
    _inherit = 'base'

    def unlink(self):
        is_followed = self.env["followed.model"].search([("model.name", "=", self._name)], limit=1)
        if is_followed:
            try:
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

                result = super().unlink()
                
                
                if result and records_to_notify:
                    _logger.info(f"Deletion successful. Sending {len(records_to_notify)} messages to Kafka.")
                    
                   
                    def convert_to_iso(o):
                        if isinstance(o, (datetime.date, datetime.datetime)):
                            return o.isoformat()
                        # It's good practice for a converter to raise an error for unhandled types.
                        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

                    for data in records_to_notify:
                        try:
                            message = json.dumps(data, default=convert_to_iso)
                            self.env['kafka.message.handler'].create({
                                'message': message, 
                                'operation_type': data['operation'],
                                'topic': data['model_name'],
                                'sent_status': 'pending'
                            })
                        except Exception as e:
                            _logger.error(f"Failed to send Kafka message for deleted record {data.get('odoo_internal_id')}: {e}")
                
                return result
            except Exception as e:
                _logger.error(f"Failed to delete records: {e}")
                _logger.error(traceback.format_exc())
                return super().unlink()
        else:
            return super().unlink()




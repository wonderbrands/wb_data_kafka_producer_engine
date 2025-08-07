import json
import asyncio
import traceback
from odoo import fields, models, api
from . import utilities
import logging

_logger = logging.getLogger(__name__)

class WriteInhereit(models.AbstractModel):
    _inherit = 'base'

    def write(self, vals):
        
        result = None
        _logger.info(self._name)
        if self._name == 'ir.asset':
            _logger.info("******************IGNORE ASSETS*******************")
            return super().write(vals)
        if self.env.registry.ready:
            try:
                _logger.info("******************START WRITE METHOD*******************")
                _logger.info(f"Vals: {vals}")
                prev_record = self.read(list(vals.keys()))
                _logger.info(f"Previous record: {prev_record}")
                result = super().write(vals)
                new_record = self.read(list(vals.keys()))
                """
                _logger.info(f"New record: {new_record}")
                _logger.info(f"Vals: {vals}")
                watched_fields = self.env["model.follow"].search(
                    [
                        ("model.model", "=", self._name),
                        ("fields_related.name", "in", list(vals.keys()))
                    ],
                    limit=1
                )
                _logger.info(f"Watched fields: {watched_fields}")
                if watched_fields:
                    server = watched_fields.kafka_config_id
                    
                    _logger.info(f"Server configuration: {server.read()}")
                    logger = utilities.KafkaProducerUtilities(
                        self.env, 
                        "", 
                        watched_fields.topic,
                        server.bootstrap_server,
                        server.is_ssl,
                        server.cert
                    )
                    _logger.info(f"Logger configuration: {logger}")
                    for index, rec in enumerate(prev_record):
                        record_store = []
                        log = {
                            "model": field.model.name,
                            "field": field.field.name,
                            "user": env.user.name,
                            "at": datetime.now(),
                            "records": [],
                            "movment_type": "updated",
                            "rec_id": new_record[index]["id"]
                        }
                        _logger.info(f"log: {log}")
                        for field in watched_fields.fields:
                            if field.follow_on_update:
                                record_store.append(
                                    {
                                        "field": field.field.name,
                                        "old_value": rec[field.field.name],
                                        "new_value": new_record[index][field.field.name],
                                    }
                                )
                        _logger.info(f"record_store: {record_store}")
                        logger.set_message(
                            json.dumps(log)
                        )
                        asyncio.run(logger.sendMessage())
                        _logger.info(f"message sent to kafka: {log}")
                            
                    logger.kill()
                    _logger.info(f"Kafka producer killed: {logger}")
                """
                _logger.info("******************END WRITE METHOD*******************")
            except Exception as e:  
                _logger.info("******************ERROR ON READ*******************")
                _logger.info(traceback.format_exc())
                _logger.info(f"Error in write method: {e}")
                _logger.info("******************ERROR ON READ*******************")
                if not result:
                    result = super().write(vals)
                    return result
        else:
            result = super().write(vals)
        return result
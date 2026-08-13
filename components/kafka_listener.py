import json
import datetime
import logging
from odoo.addons.component.core import Component
from odoo import tools, api, SUPERUSER_ID
import odoo

_logger = logging.getLogger(__name__)

class KafkaEventListener(Component):
    _name = "kafka.event.listener"
    _inherit = "base.event.listener"

    def _convert(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        return str(o)

    def _record_label(self, record):
        if record is None:
            return "None"
        if isinstance(record, (list, tuple)):
            return ", ".join([self._record_label(r) for r in record])
        try:
            return record.display_name
        except Exception:
            try:
                return str(record.id)
            except Exception:
                return str(record)

    def _prepare_vals(self, record, vals):
        processed = {}
        for field, value in vals.items():
            field_obj = record._fields.get(field)
            if not field_obj:
                processed[field] = value
                continue
            if field_obj.type == "many2one":
                relation_record = record[field]
                processed[field] = self._record_label(relation_record) if relation_record else value
            elif field_obj.type in ("one2many", "many2many"):
                relation_records = record[field]
                processed[field] = [self._record_label(r) for r in relation_records] if relation_records else []
            else:
                processed[field] = value
        return processed

    @classmethod
    def _process_kafka_postcommit_job(cls, dbname, vals_list):
        """Worker executed to bulk create handler records with a fresh DB cursor"""
        if not vals_list:
            return
        
        try:
            with odoo.modules.registry.Registry(dbname).cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                
                # Check topic existence (cached in class variable _known_topics)
                topics = {v['topic'] for v in vals_list}
                for full_topic in topics:
                    model_name = full_topic.split('-_-')[0]
                    model_info = env["followed.model"].sudo().get_followed_model(model_name)
                    if model_info:
                        model_info = model_info.with_env(env)
                    env["kafka.message.handler"]._ensure_topic_exists(full_topic, model_info=model_info)
                
                try:
                    env["kafka.message.handler"].create(vals_list)
                    cr.commit()
                except Exception as e:
                    cr.rollback()
                    _logger.error("Failed to batch create kafka.message.handler records: %s", e, exc_info=True)
                
        except Exception as e:
            _logger.error("Postcommit Kafka job failed completely: %s", e, exc_info=True)

    def _create_kafka_message_async(self, records, vals_list, operation_type, data_like):
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

        cr = self.env.cr
        callback_exists = False
        if hasattr(cr, '_kafka_callback'):
            try:
                if cr._kafka_callback in cr.postcommit._funcs:
                    callback_exists = True
            except Exception:
                pass

        if not callback_exists:
            cr._kafka_pending_messages = []
            dbname = cr.dbname
            
            def postcommit_callback():
                pending_messages = list(cr._kafka_pending_messages)
                cr._kafka_pending_messages.clear()
                if pending_messages:
                    self._process_kafka_postcommit_job(
                        dbname,
                        pending_messages
                    )
            
            cr._kafka_callback = postcommit_callback
            cr.postcommit.add(postcommit_callback)

        for msg in messages:
            cr._kafka_pending_messages.append({
                "message": msg,
                "topic": f"{records[0]._name if records else 'unknown'}-_-{data_like}",
                "operation_type": operation_type,
                "sent_status": "pending",
                "data_like": data_like,
            })

    def on_records_create(self, records, vals_list):
        if not records:
            return
        followed = self.env["followed.model"].sudo().get_followed_model(records._name)
        if followed:
            followed = followed.with_env(self.env)
            binary_fields = [f for f, field in records._fields.items() if field.type == 'binary'] if followed.exclude_binary else []
            computed_fields = [f for f in followed.computed_fields.mapped('name') if f and f not in binary_fields]
            
            if followed.api_like:
                api_vals_list = []
                for record, vals in zip(records, vals_list):
                    record_vals = {k: v for k, v in vals.items() if k not in binary_fields}
                    for f in computed_fields:
                        try:
                            record_vals[f] = record[f]
                        except Exception:
                            pass
                    api_vals_list.append(record_vals)
                self._create_kafka_message_async(records, vals_list=api_vals_list, operation_type="create", data_like="api_like")

            if followed.schema_like:
                ids = records.ids
                rows = self.env['base'].query_ids(ids, records._name)
                rows_by_id = {row['id']: row for row in rows}
                schema_vals_list = []
                for record in records:
                    if record.id in rows_by_id:
                        row_vals = {k: v for k, v in rows_by_id[record.id].items() if k not in binary_fields}
                        for f in computed_fields:
                            try:
                                row_vals[f] = record[f]
                            except Exception:
                                pass
                        schema_vals_list.append(row_vals)
                if schema_vals_list:
                    self._create_kafka_message_async(records, vals_list=schema_vals_list, operation_type="create", data_like="schema_like")

    def on_records_write(self, records, vals):
        if not records:
            return
        followed = self.env["followed.model"].sudo().get_followed_model(records._name)
        if followed:
            followed = followed.with_env(self.env)
            binary_fields = [f for f, field in records._fields.items() if field.type == 'binary'] if followed.exclude_binary else []
            computed_fields = [f for f in followed.computed_fields.mapped('name') if f and f not in binary_fields]

            if followed.api_like:
                vals_list = []
                for record in records:
                    record_vals = {k: v for k, v in vals.items() if k not in binary_fields}
                    for f in computed_fields:
                        try:
                            record_vals[f] = record[f]
                        except Exception:
                            pass
                    vals_list.append(record_vals)
                self._create_kafka_message_async(records, vals_list=vals_list, operation_type="update", data_like="api_like")

            if followed.schema_like:
                ids = records.ids
                rows = self.env['base'].query_ids(ids, records._name)
                rows_by_id = {row['id']: row for row in rows}
                vals_list = []
                for record in records:
                    if record.id in rows_by_id:
                        row_vals = {k: v for k, v in rows_by_id[record.id].items() if k not in binary_fields}
                        for f in computed_fields:
                            try:
                                row_vals[f] = record[f]
                            except Exception:
                                pass
                        vals_list.append(row_vals)
                if vals_list:
                    self._create_kafka_message_async(records, vals_list=vals_list, operation_type="update", data_like="schema_like")

    def on_records_unlink(self, records):
        if not records:
            return
        followed = self.env["followed.model"].sudo().get_followed_model(records._name)
        if followed:
            followed = followed.with_env(self.env)
            # Prefetch display_name for all records in batch to prevent O(N) database queries
            try:
                records.mapped('display_name')
            except Exception:
                pass
            if followed.api_like:
                self._create_kafka_message_async(records, vals_list=None, operation_type="delete", data_like="api_like")
            if followed.schema_like:
                self._create_kafka_message_async(records, vals_list=None, operation_type="delete", data_like="schema_like")

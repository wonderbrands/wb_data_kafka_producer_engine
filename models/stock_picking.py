# -*- coding: utf-8 -*-
import logging
from odoo import models

_logger = logging.getLogger(__name__)


class StockPicking(models.Model):
    _inherit = "stock.picking"

    def _notify_kafka_picking_state(self):
        """
        Ensures state transitions of stock.picking are emitted to Kafka.
        Since stock.picking.state is a computed stored field, state transitions
        bypass standard write(vals) and write directly to the database via cache flushes.
        """
        try:
            is_followed = self.env["followed.model"].sudo().search([
                ("model.model", "=", self._name),
                ("active", "=", True)
            ], limit=1)
            if not is_followed or not self:
                return

            self.env.cr.flush()

            if is_followed.api_like:
                vals_list = []
                for rec in self:
                    vals_list.append({
                        "state": rec.state,
                        "date_done": rec.date_done,
                        "name": rec.name,
                        "origin": rec.origin or False,
                    })
                self._create_kafka_message_async(
                    self, vals_list=vals_list, operation_type="update", data_like="api_like"
                )

            if is_followed.schema_like:
                ids = self.ids
                rows = self.query_ids(ids, self._name)
                rows_by_id = {row["id"]: row for row in rows}
                vals_list = [rows_by_id[rec.id] for rec in self if rec.id in rows_by_id]
                if vals_list:
                    self._create_kafka_message_async(
                        self, vals_list=vals_list, operation_type="update", data_like="schema_like"
                    )
        except Exception:
            _logger.error("Error notifying Kafka of stock.picking state transition", exc_info=True)

    def _action_done(self):
        res = super()._action_done()
        self._notify_kafka_picking_state()
        return res

    def action_cancel(self):
        res = super().action_cancel()
        self._notify_kafka_picking_state()
        return res

    def action_confirm(self):
        res = super().action_confirm()
        self._notify_kafka_picking_state()
        return res

    def action_assign(self):
        res = super().action_assign()
        self._notify_kafka_picking_state()
        return res

    def button_validate(self):
        res = super().button_validate()
        self._notify_kafka_picking_state()
        return res

import logging
import json
from datetime import datetime
from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def hook(cr, registry):
    """
    Hook executed after any module install/upgrade/uninstall.
    Creates a kafka.message.handler record logging the event and any field changes.
    """
    try:
        env = api.Environment(cr, SUPERUSER_ID, {})
        _logger.info("🔄 Kafka system hook triggered after module operation")

        # Identify installed/updated modules
        mods = env["ir.module.module"].search([("state", "=", "installed")])
        installed = [m.name for m in mods]
        _logger.info("Modules currently installed: %s", installed)

        # Track recently changed models or fields
        changed_fields = env["ir.model.fields"].search(
            [("write_date", "!=", False)], order="write_date desc", limit=10
        )
        changed_models = env["ir.model"].search(
            [("write_date", "!=", False)], order="write_date desc", limit=10
        )

        # Collect event details
        field_changes = [
            {
                "model": f.model,
                "field": f.name,
                "ttype": f.ttype,
                "label": f.field_description,
                "write_date": f.write_date.isoformat() if f.write_date else None,
            }
            for f in changed_fields
        ]
        model_changes = [
            {
                "model": m.model,
                "name": m.name,
                "write_date": m.write_date.isoformat() if m.write_date else None,
            }
            for m in changed_models
        ]

        event_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "by": "Odoo System",
            "installed_modules": installed,
            "recent_models": model_changes,
            "recent_fields": field_changes,
        }

        # Create a record in kafka.message.handler
        handler_model = env.get("kafka.message.handler")
        if not handler_model:
            _logger.warning("Model kafka.message.handler not found — skipping Kafka record creation.")
            return

        handler_model.create({
            "message": json.dumps(event_data, indent=2, ensure_ascii=False),
            "topic": "odoo.system.metadata",
            "operation_type": "system_hook",
            "sent_status": "pending",
        })
        cr.commit()

        _logger.info("✅ Kafka message handler record created for system hook event")

    except Exception as e:
        _logger.error("❌ Error executing Kafka system hook: %s", e, exc_info=True)

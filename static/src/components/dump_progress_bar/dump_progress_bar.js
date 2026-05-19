/** @odoo-module **/
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { Component, onWillStart, onWillDestroy, useState } from "@odoo/owl";
import { standardFieldProps } from "@web/views/fields/standard_field_props";

export class DumpProgressBar extends Component {
    static template = "wb_data_kafka_producer_engine.DumpProgressBar";
    static props = {
        ...standardFieldProps,
    };

    setup() {
        this.orm = useService("orm");
        this.state = useState({
            progress: this.props.record.data.progress || 0,
            total: this.props.record.data.total || 0,
        });

        this.pollingInterval = null;

        onWillStart(async () => {
            this.startPolling();
        });

        onWillDestroy(() => {
            this.stopPolling();
        });
    }

    get percentage() {
        if (!this.state.total || this.state.total === 0) {
            return 0;
        }
        return Math.min(100, Math.round((this.state.progress / this.state.total) * 100));
    }

    startPolling() {
        if (this.pollingInterval) return;

        this.pollingInterval = setInterval(async () => {
            const dumpId = this.props.record.resId;
            if (!dumpId) return;

            try {
                const [data] = await this.orm.read("dump", [dumpId], ["progress", "total"]);
                if (data) {
                    this.state.progress = data.progress;
                    this.state.total = data.total;

                    if (this.state.progress >= this.state.total && this.state.total > 0) {
                        this.stopPolling();
                    }
                }
            } catch (e) {
                console.error("Error polling dump progress:", e);
                this.stopPolling();
            }
        }, 3000);
    }

    stopPolling() {
        if (this.pollingInterval) {
            clearInterval(this.pollingInterval);
            this.pollingInterval = null;
        }
    }
}

registry.category("fields").add("dump_progress_bar", {
    component: DumpProgressBar,
    supportedTypes: ["integer"],
});

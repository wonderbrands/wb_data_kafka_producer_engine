/** @odoo-module **/
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { Component, onWillStart, useState } from "@odoo/owl";

export class KafkaManager extends Component {
    static template = "wb_data_kafka_producer_engine.KafkaManager";

    setup() {
        this.rpc = useService("rpc");
        this.notification = useService("notification");
        this.state = useState({
            topics: [],
            loading: true,
            messages: {},
            loadingMessages: {},
        });

        onWillStart(async () => {
            await this.loadTopics();
        });
    }

    async loadTopics() {
        this.state.loading = true;
        try {
            const result = await this.rpc("/web/dataset/call_kw/kafka.message.handler/get_kafka_topics", {
                model: "kafka.message.handler",
                method: "get_kafka_topics",
                args: [],
                kwargs: {},
            });
            if (result && result.error) {
                this.notification.add(result.error, { type: "danger" });
                this.state.topics = [];
            } else {
                this.state.topics = result || [];
            }
        } catch (e) {
            this.notification.add("Could not load Kafka topics", { type: "danger" });
        } finally {
            this.state.loading = false;
        }
    }

    async loadMessages(topic) {
        this.state.loadingMessages[topic] = true;
        try {
            const result = await this.rpc("/web/dataset/call_kw/kafka.message.handler/get_kafka_messages", {
                model: "kafka.message.handler",
                method: "get_kafka_messages",
                args: [topic, 4],
                kwargs: {},
            });
            if (result && result.error) {
                this.notification.add(result.error, { type: "danger" });
            } else {
                this.state.messages[topic] = result || [];
            }
        } catch (e) {
            this.notification.add(`Could not load messages for ${topic}`, { type: "danger" });
        } finally {
            this.state.loadingMessages[topic] = false;
        }
    }

    async deleteTopic(topic) {
        const confirmed = confirm(`Are you sure you want to delete topic "${topic}"? This will erase all its messages.`);
        if (!confirmed) return;

        try {
            const result = await this.rpc("/web/dataset/call_kw/kafka.message.handler/delete_kafka_topic", {
                model: "kafka.message.handler",
                method: "delete_kafka_topic",
                args: [topic],
                kwargs: {},
            });
            if (result === true) {
                this.notification.add(`Topic "${topic}" deleted successfully`, { type: "success" });
                await this.loadTopics();
            } else if (result && result.error) {
                this.notification.add(result.error, { type: "danger" });
            }
        } catch (e) {
            this.notification.add(`Could not delete topic "${topic}"`, { type: "danger" });
        }
    }

    async deleteAllTopics() {
        const confirmed = confirm("Are you sure you want to delete ALL topics listed? This is destructive and irreversible.");
        if (!confirmed) return;

        this.state.loading = true;
        try {
            for (const topic of this.state.topics) {
                await this.rpc("/web/dataset/call_kw/kafka.message.handler/delete_kafka_topic", {
                    model: "kafka.message.handler",
                    method: "delete_kafka_topic",
                    args: [topic],
                    kwargs: {},
                });
            }
            this.notification.add("All topics deletion triggered", { type: "info" });
        } catch (e) {
            console.error("Batch deletion error", e);
        } finally {
            await this.loadTopics();
        }
    }
}

registry.category("actions").add("kafka_manager_action", KafkaManager);

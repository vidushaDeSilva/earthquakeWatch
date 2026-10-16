"""Reset only explicitly allowlisted earthquake topics and inactive project groups.

Deletion/recreation is journaled independently from Databricks DDL. A new stream
generation prevents recreated offsets from colliding with prior delivery IDs.
"""

from .errors import ConfigurationError, ContractError
from .kafka_client import security

TOPIC = "earthquake_events_dev"
SMOKE_TOPIC = "earthquake_compat_smoke"


class KafkaReset:
    """Advance a destructive reset by one verified broker operation per tick."""

    def __init__(self, stream, admin=None):
        """Reuse the configured TLS/SASL credentials; never extract new authority."""
        if stream.topic != TOPIC:
            raise ConfigurationError("Reset is confined to earthquake_events_dev")
        if admin is None:
            from kafka.admin import KafkaAdminClient
            options = security(stream)
            options.pop("allow_auto_create_topics", None)
            admin = KafkaAdminClient(**options)
        self.admin = admin

    def advance(self, state, save):
        """Never repeat a completed deletion after topic recreation or a later crash."""
        topics = [TOPIC, SMOKE_TOPIC] if state["reset_smoke"] else [TOPIC]
        progress = state.setdefault("kafka_reset", {})
        for topic in topics:
            entry = progress.setdefault(topic, {"stage": "DELETE"})
            present = topic in self.admin.list_topics()
            if entry["stage"] == "DELETE":
                if topic == TOPIC and "replication_factor" not in entry:
                    metadata = self.admin.describe_topics([topic]) if present else []
                    parts = metadata[0].get("partitions", []) if metadata else []
                    entry["replication_factor"] = len(parts[0]["replicas"]) if parts else 1
                    entry["partitions"] = len(parts) or 2
                    if entry["partitions"] not in (1, 2) or not 1 <= entry["replication_factor"] <= 3:
                        raise ContractError("Unexpected topic topology; reset stopped")
                save(state)  # Intention and topology survive an uncertain deletion reply.
                if present:
                    result = self.admin.delete_topics([topic], timeout_ms=10000)
                    errors = getattr(result, "topic_error_codes", [])
                    if any(row[1] not in (0, 3) for row in errors):
                        raise ContractError("Kafka topic deletion was rejected")
                    return False
                entry["stage"] = "CREATE" if topic == TOPIC else "DONE"
                save(state)
            if entry["stage"] == "CREATE":
                if topic not in self.admin.list_topics():
                    from kafka.admin import NewTopic
                    result = self.admin.create_topics([NewTopic(topic,
                        num_partitions=entry["partitions"], replication_factor=entry["replication_factor"],
                        topic_configs={"cleanup.policy": "delete", "retention.ms": "259200000",
                                       "retention.bytes": "268435456"})], timeout_ms=10000)
                    errors = getattr(result, "topic_errors", [])
                    if any(row[1] not in (0, 36) for row in errors):
                        raise ContractError("Kafka topic creation was rejected")
                    return False
                entry["stage"] = "DONE"
                save(state)
        groups = [name for name, _ in self.admin.list_consumer_groups()
                  if name.startswith("earthquake-dev-landing-") or name.startswith("earthquake-compat-")]
        if groups:
            results = self.admin.delete_consumer_groups(groups)
            if any(error not in (None, 0) and getattr(error, "errno", -1) not in (0, 69)
                   for _, error in results):
                raise ContractError("Project consumer group is active or deletion was denied")
        return True

    def close(self):
        """Close the administration connection after a short reset tick."""
        self.admin.close()

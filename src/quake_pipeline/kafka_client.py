"""Connect to Aiven with in-memory TLS and explicit, bounded Kafka semantics.

Manual partition assignment requires the shared GitHub concurrency lock. There
is no group rebalance while a cloud upload is in progress. Commits are synchronous.
"""

import logging
import ssl
from typing import Any

from .errors import ConfigurationError, PipelineError
from .kafka_config import KafkaSettings


class KafkaFailure(PipelineError):
    """Expose only a safe Kafka failure category, never client exception details."""

    code = "KAFKA_FAILURE"


def security(settings: KafkaSettings) -> dict[str, Any]:
    """Load CA bytes directly into a verifying SSL context without a local file."""
    try:
        context = ssl.create_default_context(cadata=settings.ca_pem)
    except (ssl.SSLError, ValueError):
        raise ConfigurationError("AIVEN_KAFKA_CA_PEM must contain the complete PEM CA certificate") from None
    return {"bootstrap_servers": settings.bootstrap.split(","), "security_protocol": "SASL_SSL",
            "sasl_mechanism": "SCRAM-SHA-256", "sasl_plain_username": settings.username,
            "sasl_plain_password": settings.password, "ssl_context": context,
            "client_id": "earthquake-dev-handoff-v3", "request_timeout_ms": 30000,
            "api_version_auto_timeout_ms": 15000, "allow_auto_create_topics": False}


class KafkaClient:
    """Adapt pinned kafka-python APIs for dispatch, offset inspection and landing."""

    def __init__(self, settings: KafkaSettings) -> None:
        """Create the read client now and defer the producer until it is needed."""
        from kafka import KafkaConsumer
        logging.getLogger("kafka").setLevel(logging.CRITICAL)
        self.settings, self.producer = settings, None
        self.options = security(settings)
        self.consumer = KafkaConsumer(group_id=settings.group, enable_auto_commit=False,
                                      auto_offset_reset="none", max_poll_records=100,
                                      max_poll_interval_ms=1800000, session_timeout_ms=10000,
                                      fetch_max_bytes=4194304, max_partition_fetch_bytes=1048576,
                                      **self.options)

    def topic_config(self) -> dict[str, str]:
        """Read effective retention and cleanup settings; ACL denial fails visibly."""
        from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
        admin = KafkaAdminClient(**{key: value for key, value in self.options.items()
                                    if key != "allow_auto_create_topics"})
        try:
            responses = admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC, self.settings.topic)])
            output = {}
            for response in responses:
                for resource in response.resources:
                    if resource[0] != 0:
                        raise KafkaFailure("Kafka topic configuration inspection was denied")
                    for entry in resource[4]:
                        if entry[0] in {"retention.ms", "retention.bytes", "cleanup.policy"}:
                            output[entry[0]] = entry[1]
            return output
        finally:
            admin.close()

    def partitions(self) -> list[int]:
        """Require an existing topic with one or two contiguous partition IDs."""
        values = sorted(self.consumer.partitions_for_topic(self.settings.topic) or [])
        if values not in ([0], [0, 1]):
            raise KafkaFailure("Create the development topic with one or two partitions before running")
        return values

    def bounds(self, partition: int) -> tuple[int, int, int | None]:
        """Return retained start, current exclusive end and committed next offset."""
        from kafka import TopicPartition
        tp = TopicPartition(self.settings.topic, partition)
        return (self.consumer.beginning_offsets([tp])[tp],
                self.consumer.end_offsets([tp])[tp], self.consumer.committed(tp, timeout_ms=15000))

    def produce(self, messages: list[tuple[bytes, bytes]]) -> list[dict[str, int]]:
        """Wait for broker acknowledgements for one bounded source chunk."""
        if self.producer is None:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(enable_idempotence=True, acks="all",
                max_in_flight_requests_per_connection=1, delivery_timeout_ms=60000,
                max_block_ms=15000, compression_type="gzip",
                **self.options)
        futures = [self.producer.send(self.settings.topic, key=key, value=value) for key, value in messages]
        result = []
        for future in futures:
            metadata = future.get(timeout=65)
            result.append({"partition": metadata.partition, "offset": metadata.offset})
        return result

    def seek(self, partition: int, offset: int) -> None:
        """Assign exactly one partition and seek without an automatic offset reset."""
        from kafka import TopicPartition
        tp = TopicPartition(self.settings.topic, partition)
        self.consumer.assign([tp])
        self.consumer.seek(tp, offset)

    def poll(self) -> list[Any]:
        """Return at most one hundred raw records from the assigned partition."""
        records = self.consumer.poll(timeout_ms=2000, max_records=100)
        return [record for values in records.values() for record in values]

    def commit(self, partition: int, next_offset: int, batch_id: str) -> None:
        """Commit the next offset with the exact pinned leader_epoch signature."""
        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata
        self.consumer.commit({TopicPartition(self.settings.topic, partition):
                              OffsetAndMetadata(next_offset, "eq3:" + batch_id[:48], -1)})

    def close(self) -> None:
        """Release network clients without enabling a final automatic commit."""
        try:
            if self.producer is not None:
                self.producer.close(timeout=10)
        finally:
            self.consumer.close(autocommit=False)

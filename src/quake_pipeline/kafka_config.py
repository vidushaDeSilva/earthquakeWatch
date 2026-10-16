"""Validate Kafka stream identity, secret inputs, and fixed resource limits.

Credentials and the CA stay in process memory. A stream generation must change
when a topic is recreated; Kafka offset numbers alone are not durable identities.
"""

import re
from dataclasses import dataclass, field
from typing import Mapping

from .errors import ConfigurationError


@dataclass(frozen=True)
class KafkaSettings:
    """Describe one isolated development stream and its bounded client settings."""

    bootstrap: str
    topic: str
    group: str
    generation: str
    dispatch_generation: str
    username: str = field(repr=False)
    password: str = field(repr=False)
    ca_pem: str = field(repr=False)
    chunk_size: int = 100
    max_source_items: int = 10000
    max_message_bytes: int = 262144
    max_batch_bytes: int = 4194304
    max_batches: int = 10000
    max_land_records: int = 10000

    @classmethod
    def from_env(cls, values: Mapping[str, str]) -> "KafkaSettings":
        """Require explicit stream values and keep smoke-test resources isolated."""
        required = ("AIVEN_BOOTSTRAP_SERVERS", "AIVEN_KAFKA_USERNAME",
                    "AIVEN_KAFKA_PASSWORD", "AIVEN_KAFKA_CA_PEM", "EQ_KAFKA_TOPIC",
                    "EQ_KAFKA_CONSUMER_GROUP", "EQ_STREAM_GENERATION")
        if any(not values.get(key, "").strip() for key in required):
            raise ConfigurationError("Set all Aiven secrets and EQ Kafka stream variables")
        result = cls(bootstrap=values[required[0]].strip(),
                     username=values[required[1]].strip(), password=values[required[2]].strip(),
                     ca_pem=values[required[3]].strip(), topic=values[required[4]].strip(),
                     group=values[required[5]].strip(), generation=values[required[6]].strip(),
                     dispatch_generation=values.get("EQ_DISPATCH_GENERATION", "v1").strip() or "v1")
        if result.topic != "earthquake_events_dev" or not result.group.startswith("earthquake-dev-landing-"):
            raise ConfigurationError("Sprint 3 requires earthquake_events_dev and earthquake-dev-landing-* group")
        if any(not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", item) for item in (
                result.generation, result.dispatch_generation, result.group)):
            raise ConfigurationError("Kafka generations/group contain unsupported characters")
        if any(not re.fullmatch(r"[A-Za-z0-9.-]+:[0-9]{1,5}", item)
               for item in result.bootstrap.split(",")):
            raise ConfigurationError("Use Aiven SASL bootstrap hostname:port, without a URL scheme")
        return result

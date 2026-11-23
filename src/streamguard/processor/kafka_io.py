"""Kafka side of the processor: the alert/DLQ publisher (lag is the monitor's job, ADR-02)."""

from __future__ import annotations

from collections.abc import Mapping

from confluent_kafka import KafkaException, Producer

from streamguard.common.settings import kafka_producer_conf


class KafkaPublisher:
    """Publishes alerts (key = alert_id) and dead letters; flush() raises on any failure so
    the batch fails and Spark runs it again (at-least-once, ADR-01)."""

    def __init__(self, env: Mapping[str, str], client_id: str = "streamguard-processor"):
        self._producer = Producer(kafka_producer_conf(env, client_id))
        self._errors: list[str] = []

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self._errors.append(str(err))

    def publish(self, topic: str, key: str | None, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    topic,
                    key=key.encode() if key is not None else None,
                    value=value,
                    on_delivery=self._on_delivery,
                )
                return
            except BufferError:
                self._producer.poll(0.2)

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        errors, self._errors = self._errors, []
        if remaining or errors:
            raise KafkaException(f"{remaining} undelivered, errors: {errors[:3]}")

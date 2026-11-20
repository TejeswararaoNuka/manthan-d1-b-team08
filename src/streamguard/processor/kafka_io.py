"""Kafka side of the processor: alert/DLQ publisher and consumer-lag lookup."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

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


class LagReader:
    """Lag after a batch = sum over its partitions of (topic end offset - next offset to read).
    Partitions absent from the batch count as caught up (maxOffsetsPerTrigger spreads reads
    over all lagging partitions, so they appear in the batch)."""

    def __init__(self, env: Mapping[str, str]):
        self._consumer = Consumer(
            {
                "bootstrap.servers": env["KAFKA_BOOTSTRAP"],
                "group.id": "streamguard-processor-lag",  # metadata only, never commits
                "enable.auto.commit": False,
            }
        )

    def __call__(self, records: Sequence) -> int | None:
        next_offsets: dict[tuple[str, int], int] = {}
        for r in records:
            key = (r.topic, r.partition)
            next_offsets[key] = max(next_offsets.get(key, 0), r.offset + 1)
        try:
            total = 0
            for (topic, partition), nxt in next_offsets.items():
                _, high = self._consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=5
                )
                total += max(high - nxt, 0)
            return total
        except KafkaException:
            return None

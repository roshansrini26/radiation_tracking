"""
Flink Job 1 — Read radiation readings from Kafka, drop empty values, print.

This is the simplest real pipeline:
    Kafka source  ->  parse JSON  ->  FILTER (drop empty value)  ->  print

It uses Flink's DataStream API, where each processing step is an explicit
"operator" — which matches how the project spec talks about operators.

Run it INSIDE the jobmanager container:
    docker exec -it jobmanager flink run -py /opt/flink/jobs/job1_filter.py
"""

import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema


def main():
    # 1) The execution environment — the entry point for any Flink job.
    env = StreamExecutionEnvironment.get_execution_environment()

    # 2) Define the Kafka SOURCE.
    #    NOTE: broker address is 'kafka:19092' — the INTERNAL listener.
    #    Flink runs in its own container, so it reaches Kafka by container
    #    name across the Docker network, NOT via localhost.
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:19092")
        .set_topics("radiation-readings")
        .set_group_id("flink-radiation-group")
        # Start reading from the earliest message so we see existing data.
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        # Each Kafka message value is a UTF-8 JSON string.
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # 3) Plug the source into the stream.
    #    WatermarkStrategy.no_watermarks() for now — event-time/watermarks
    #    come later when we enforce capture-time ordering.
    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka-source",
    )

    # 4) PARSE operator: turn each JSON string into a Python dict.
    #    A 'map' applies a function to every element, one-to-one.
    def parse_json(raw: str):
        try:
            return json.loads(raw)
        except Exception:
            return None  # malformed -> mark for dropping in the next step

    parsed = stream.map(parse_json)

    # 5) FILTER operator (the spec's first required operator):
    #    drop rows that are None (parse failed) OR have an empty radiation value.
    #    A 'filter' keeps only elements for which the function returns True.
    def has_value(record) -> bool:
        if record is None:
            return False
        value = record.get("value")
        # Empty string, None, or whitespace -> discard.
        return value is not None and str(value).strip() != ""

    filtered = parsed.filter(has_value)

    # 6) SINK: for now, print to the TaskManager logs so we can see it works.
    #    Later this becomes a sink to the frontend.
    filtered.print()

    # 7) Execute. Nothing actually runs until this line — Flink builds the
    #    operator graph first, then this kicks it off.
    env.execute("radiation-job1-filter")


if __name__ == "__main__":
    main()
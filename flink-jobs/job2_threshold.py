"""
Flink Job 2 — Threshold alert operator.

Pipeline:
    Kafka source -> parse JSON -> filter empty -> parse value to float
                 -> tag severity (safe/danger) -> print

New vs job1:
  - parse_value : converts the string "81.8" to a float; drops non-numeric rows.
  - tag_severity: compares value against a CONFIGURABLE threshold and adds a
                  "severity" field ("safe" or "danger"). This satisfies the
                  spec's "user-configurable critical level" requirement and
                  gives the map something to colour by later.

Run it INSIDE the jobmanager container. The threshold is passed as a job arg:
    docker exec -it jobmanager flink run -py /opt/flink/jobs/job2_threshold.py --threshold 100
"""

import json
import argparse

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema


def main():
    # --- Job-level config (the configurable threshold) ---
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=float, default=100.0,
                        help="CPM value at/above which a reading is 'danger'")
    args = parser.parse_args()
    threshold = args.threshold

    env = StreamExecutionEnvironment.get_execution_environment()

    # --- Kafka source (same as job1; kafka:19092 internal listener) ---
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:19092")
        .set_topics("radiation-readings")
        .set_group_id("flink-radiation-threshold")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "kafka-source"
    )

    # --- Operator 1: parse JSON string -> dict ---
    def parse_json(raw: str):
        try:
            return json.loads(raw)
        except Exception:
            return None

    parsed = stream.map(parse_json)

    # --- Operator 2: FILTER out empty / unparseable rows ---
    def has_value(record) -> bool:
        if record is None:
            return False
        v = record.get("value")
        return v is not None and str(v).strip() != ""

    filtered = parsed.filter(has_value)

    # --- Operator 3: parse the value string to a float ---
    #     CSV gives "81.8" as text. We need a number to compare it.
    #     If it won't parse (junk/noise), mark None to drop next.
    def parse_value(record):
        try:
            record["value"] = float(record["value"])
            return record
        except (ValueError, TypeError):
            return None

    valued = filtered.map(parse_value)

    # Drop the rows that failed numeric parsing.
    numeric = valued.filter(lambda r: r is not None)

    # --- Operator 4: TAG SEVERITY against the configurable threshold ---
    #     Adds a "severity" field the frontend can colour by.
    def tag_severity(record):
        record["severity"] = "danger" if record["value"] >= threshold else "safe"
        return record

    tagged = numeric.map(tag_severity)

    # --- Sink: print (TaskManager logs) for now ---
    tagged.print()

    env.execute(f"radiation-job2-threshold-{int(threshold)}")


if __name__ == "__main__":
    main()

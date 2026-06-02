import csv
import json
import time
import argparse
from confluent_kafka import Producer


def parse_args():
    """Command-line options so we can change behaviour without editing code."""
    p = argparse.ArgumentParser(description="Safecast Kafka data provider")
    p.add_argument("--file", default="../data/safecast_sample.csv",
                   help="Path to the Safecast CSV file")
    p.add_argument("--broker", default="localhost:9092",
                   help="Kafka bootstrap server")
    p.add_argument("--topic", default="radiation-readings",
                   help="Kafka topic to produce to")
    p.add_argument("--delay", type=float, default=0.1,
                   help="Seconds to wait between messages (replay speed). "
                        "0 = as fast as possible.")
    p.add_argument("--limit", type=int, default=0,
                   help="Max number of rows to send (0 = no limit). "
                        "Useful for quick tests.")
    return p.parse_args()


def delivery_report(err, msg):
    """
    Called by Kafka after each message is sent (or fails).
    We only print on error to keep the output clean.
    """
    if err is not None:
        print(f"  ! delivery failed: {err}")


def main():
    args = parse_args()

    # Create the Kafka producer. 'bootstrap.servers' tells it where the broker is.
    producer = Producer({"bootstrap.servers": args.broker})

    sent = 0
    skipped = 0

    print(f"Reading {args.file}")
    print(f"Producing to topic '{args.topic}' on {args.broker}")
    print(f"Delay between messages: {args.delay}s\n")

    # Open the CSV. newline="" is the correct way to open files for the csv module.
    with open(args.file, newline="", encoding="utf-8") as f:
        # DictReader reads the header row automatically and gives each row as a
        # dictionary keyed by column name (e.g. row["Captured Time"]).
        reader = csv.DictReader(f)

        for row in reader:
            # --- Minimal structural validation only (NOT business logic) ---
            # The truncated last line, or any broken row, won't have the key
            # columns. Skip only if the row is structurally unusable.
            captured = row.get("Captured Time")
            lat = row.get("Latitude")
            lon = row.get("Longitude")

            if not captured or not lat or not lon:
                skipped += 1
                continue

            # Build the message. We keep the original column meanings but rename
            # to clean lowercase keys that are easier to work with downstream.
            # NOTE: we deliberately KEEP empty radiation values — discarding
            # those is a business rule, and the spec says that belongs in Flink.
            message = {
                "captured_at": captured,
                "uploaded_at": row.get("Uploaded Time"),
                "latitude": lat,
                "longitude": lon,
                "value": row.get("Value"),      # radiation reading (may be empty)
                "unit": row.get("Unit"),
                "device_id": row.get("Device ID"),
                "location_name": row.get("Location Name"),
            }

            # Serialise to JSON and send. Kafka deals in bytes, so we encode.
            producer.produce(
                topic=args.topic,
                value=json.dumps(message).encode("utf-8"),
                callback=delivery_report,
            )

            # poll(0) lets the producer handle delivery callbacks without blocking.
            producer.poll(0)

            sent += 1

            # Progress ping every 1000 messages.
            if sent % 1000 == 0:
                print(f"  sent {sent} messages...")

            # Stop early if a limit was set.
            if args.limit and sent >= args.limit:
                print(f"\nReached limit of {args.limit} messages.")
                break

            # The configurable replay delay.
            if args.delay > 0:
                time.sleep(args.delay)

    # flush() blocks until all queued messages are actually delivered.
    print("\nFlushing remaining messages...")
    producer.flush()

    print(f"\nDone. Sent: {sent}, Skipped (broken rows): {skipped}")


if __name__ == "__main__":
    main()
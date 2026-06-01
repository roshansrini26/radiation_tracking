# Radiation Tracking — Project Progress Notes

A running log of what's been built, the concepts behind it, and every command
needed to reproduce it. Pick up from the **"Where we stopped"** section at the end.

---

## 1. What this project is

A real-time radiation monitoring pipeline using the Safecast dataset.

Data flow (the whole architecture in one line):

```
[Safecast CSV] -> [Producer] -> [Kafka topic] -> [Flink operators] -> [Map GUI]
```

Core rules from the project spec:
- **Kafka** simulates an infinite live sensor stream (not a database).
- **Flink** does ALL the processing (filtering, alerts, dedup). The producer and
  the frontend stay "dumb" — no logic in them.
- Data is ingested in upload order, but must be **replayed/processed in capture-time
  order** inside the pipeline. No pre-sorting the file allowed. (This is a Flink
  event-time problem we tackle later.)

Tech stack: Python producer, Kafka + Flink in Docker, PyFlink for jobs,
Leaflet map + WebSocket for the frontend (Stage 2).

---

## 2. Key vocabulary (so commands make sense)

**Kafka** = a system that moves streams of messages between programs.
- **Broker**: the Kafka server itself (runs in Docker).
- **Topic**: a named channel where messages live (e.g. `radiation-readings`).
- **Producer**: a program that WRITES messages to a topic (our Python script).
- **Consumer**: a program that READS messages from a topic (Flink, later).
- **Partition**: a topic is split into partitions for parallelism. Order is
  guaranteed only WITHIN one partition.

**Flink** = a system that PROCESSES streams (the "thinking" Kafka avoids).
- **Job / Topology**: the whole processing program (a graph of steps).
- **Operator**: one processing step (filter, transform, alert...). Spec wants
  one operator per functionality.
- **Source**: where Flink reads in (our Kafka topic).
- **Sink**: where Flink sends results out (the map, later).
- **Event time**: when a reading was captured (`captured_at`).
- **Processing time**: when Flink happens to handle it.
- **Watermark**: Flink's marker for "I've probably seen everything up to time T"
  — used to handle out-of-order event-time data. (Comes up later.)

Mental model: producer = the hand dropping items on a conveyor belt;
Kafka = the belt (just holds/moves, no thinking);
Flink = the worker at the belt doing the actual work;
map = the screen on the wall showing results.

---

## 3. Project folder structure

```
radiation_tracking/
├── docker-compose.yml      # Kafka (and later Flink) setup
├── data-provider/          # Python producer
│   ├── venv/               # Python virtual environment (not committed)
│   └── producer.py         # the Kafka producer
├── data/                   # the Safecast CSV sample (not committed)
│   └── safecast_sample.csv
├── flink-jobs/             # PyFlink jobs (next session)
└── frontend/               # map GUI (Stage 2)
```

---

## 4. The data

Source: `https://api.safecast.org/system/measurements.csv` (full file ~29GB,
public domain / CC0). We work against a small sample, NOT the full file.

CSV columns (real headers):
`Captured Time, Latitude, Longitude, Value, Unit, Location Name, Device ID,
MD5Sum, Height, Surface, Radiation, Uploaded Time, Loader ID`

We use: Captured Time (event time / ordering key), Latitude, Longitude,
Value (the CPM radiation reading), Unit, Device ID (for dedup later),
Uploaded Time (ingestion time).

Watch out for:
- `Captured Time` sometimes has milliseconds, sometimes not — parser must handle both.
- `Value` arrives as a STRING from CSV — Flink must parse it to a number before
  doing `value > threshold`.

---

## 5. Setup commands (from scratch)

### 5.1 Create folder structure
```bash
mkdir -p data-provider flink-jobs frontend docker data
```

### 5.2 Start Docker
Open Docker Desktop, wait for the whale icon to go solid. Confirm:
```bash
docker info
```

### 5.3 Kafka via docker-compose
The working `docker-compose.yml` (the key fix was using `://:9092` with an
empty host instead of `0.0.0.0`, which the Kafka image rejects in advertised
listeners):

```yaml
services:
  kafka:
    image: apache/kafka:3.9.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: CONTROLLER://:9093,PLAINTEXT://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

Start it:
```bash
docker compose up -d
docker compose ps          # kafka should show "Up"
docker compose logs kafka  # look for "Kafka Server started"
```

Stop it (when done for the day):
```bash
docker compose down
```
Note: `down` clears the topic data, but that's fine — the producer regenerates
it from the CSV.

### 5.4 Download a data sample (first 19MB, not 29GB)
```bash
cd data
curl -r 0-20000000 -o safecast_sample.csv https://api.safecast.org/system/measurements.csv
head -5 safecast_sample.csv   # inspect columns
wc -l safecast_sample.csv     # row count (~160k)
cd ..
```
(The last row will be cut off mid-line — that's expected, the producer skips it.)

### 5.5 Python environment + Kafka client
```bash
cd data-provider
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install confluent-kafka      # NOTE: hyphen, single package. NOT "confluent kafka"
```

---

## 6. The producer (`producer.py`)

What it does: reads the CSV row by row, skips structurally broken rows only
(keeps empty-value rows — discarding those is Flink's job), sends each row to
Kafka as JSON, waits a configurable delay, repeats.

Options:
- `--file`   path to CSV (default `../data/safecast_sample.csv`)
- `--broker` Kafka address (default `localhost:9092`)
- `--topic`  topic name (default `radiation-readings`)
- `--delay`  seconds between messages (replay speed; 0 = max speed)
- `--limit`  max rows to send (0 = no limit)

### Create the topic (once per fresh Kafka)
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic radiation-readings \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### Run the producer
```bash
cd data-provider
source venv/bin/activate
python producer.py --delay 1            # one reading per second, non-stop
python producer.py --limit 5 --delay 0.5 # quick 5-message test
python producer.py --delay 0            # firehose (max speed)
```

---

## 7. Watching the stream (two terminals)

Both must run at the same time, in separate terminal windows.

**Terminal 1 — consumer (listens, hangs until messages arrive):**
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic radiation-readings \
  --bootstrap-server localhost:9092
```
Add `--from-beginning` to see all messages from the start, or
`--max-messages 5` to auto-exit after 5.

**Terminal 2 — producer:**
```bash
cd data-provider && source venv/bin/activate
python producer.py --delay 1
```

Watch readings appear one at a time in Terminal 1.

**To stop a running command:** press `Ctrl + C` (the Control key, NOT Cmd).
Or close the terminal window. Stopping terminals does NOT stop Kafka (it runs
in Docker independently).

---

## 8. Useful Kafka admin commands

```bash
# list topics
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092

# delete a topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --delete --topic <name> --bootstrap-server localhost:9092
```

---

## 9. Git (commit progress)

`.gitignore` (so we don't commit venv / big data files):
```
data-provider/venv/
data/*.csv
__pycache__/
```

Commit:
```bash
cd ~/radiation_tracking
git add .
git commit -m "Stage 1: Kafka setup + working Safecast producer"
git push
```

---

## 10. Status checklist

Stage 1:
- [x] Task 1: Kafka set up + configured in Docker
- [x] Task 2: Data provider (producer) reading Safecast, configurable speed
- [ ] Task 3: Flink set up + first operator   <-- NEXT SESSION

Stage 2 (later):
- [ ] Web GUI with map
- [ ] Display processed data on map, configurable display speed
- [ ] More operators (alerts, dedup, area/time filters)
- [ ] Cloud deployment + Docker images + README + presentation

---

## 11. Where we stopped / next steps

Kafka + producer fully working and verified (live stream confirmed).

**Next session = Flink (Stage 1, Task 3):**
1. Add Flink (JobManager + TaskManager) to `docker-compose.yml` next to Kafka.
2. Re-add an INTERNAL Kafka listener (`kafka:19092`) so Flink — in its own
   container — can reach Kafka by name across the Docker network.
3. Write the first PyFlink job: read from `radiation-readings`, parse JSON,
   drop empty-value rows (the spec's first required operator), print survivors.

Decision already made: run Flink jobs INSIDE the Flink container (not from the
Mac venv), because PyFlink support on Python 3.14 is unreliable and the Flink
image ships its own Python.

Open watch-item: the capture-time-ordering requirement (event time + watermarks)
is the trickiest part and comes after the first simple operator works.
```
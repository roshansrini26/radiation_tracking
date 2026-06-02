# Radiation Tracking — Progress Notes

Command-focused log of what's done and how to reproduce it.
Jump to **"Where we stopped"** at the end to resume.

---

## 1. Project in one line

```
[Safecast CSV] -> [Producer] -> [Kafka topic] -> [Flink operators] -> [Map GUI]
```

Rules: Kafka = dumb stream, Flink = all processing, frontend = display only.
Ingest in upload order, process in capture-time order, no pre-sorting.

---

## 2. Folder structure

```
radiation_tracking/
├── docker-compose.yml      # kafka + flink (jobmanager + taskmanager)
├── data-provider/
│   ├── venv/               # not committed
│   └── producer.py
├── data/
│   └── safecast_sample.csv # not committed
├── flink-jobs/
│   ├── Dockerfile          # custom Flink image (python + pyflink + kafka connector)
│   └── job1_filter.py
└── frontend/               # Stage 2
```

---

## 3. The data

Source: `https://api.safecast.org/system/measurements.csv` (full ~29GB, CC0).
We use a small sample, not the full file.

CSV headers:
`Captured Time, Latitude, Longitude, Value, Unit, Location Name, Device ID,
MD5Sum, Height, Surface, Radiation, Uploaded Time, Loader ID`

Used: Captured Time (ordering key), Latitude, Longitude, Value (CPM),
Unit, Device ID (dedup later), Uploaded Time.

Gotchas:
- `Captured Time` sometimes has milliseconds, sometimes not.
- `Value` is a STRING from CSV — parse to number in Flink before `value > threshold`.

---

## 4. One-time setup (already done)

### 4.1 Folders
```bash
mkdir -p data-provider flink-jobs frontend docker data
```

### 4.2 Download data sample (first ~19MB)
```bash
cd data
curl -r 0-20000000 -o safecast_sample.csv https://api.safecast.org/system/measurements.csv
head -5 safecast_sample.csv
wc -l safecast_sample.csv
cd ..
```
(Last row is cut off mid-line — expected, producer skips it.)

### 4.3 Python env + Kafka client (for producer)
```bash
cd data-provider
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install confluent-kafka     # hyphen, single package. NOT "confluent kafka"
cd ..
```

---

## 5. docker-compose.yml (current, with Flink)

Key fixes baked in:
- Kafka advertised listeners use `://:9092` (empty host), NOT `0.0.0.0` (image rejects it).
- INTERNAL listener `kafka:19092` added so Flink (own container) reaches Kafka by name.
- jobmanager/taskmanager build from `flink-jobs/Dockerfile`, tagged `radiation-flink:1.20.0`.
- `flink-jobs/` mounted into containers at `/opt/flink/jobs`.

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
      KAFKA_LISTENERS: CONTROLLER://:9093,PLAINTEXT://:9092,INTERNAL://:19092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,INTERNAL://kafka:19092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

  jobmanager:
    build: ./flink-jobs
    image: radiation-flink:1.20.0
    container_name: jobmanager
    ports:
      - "8081:8081"
    command: jobmanager
    volumes:
      - ./flink-jobs:/opt/flink/jobs
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    depends_on:
      - kafka

  taskmanager:
    build: ./flink-jobs
    image: radiation-flink:1.20.0
    container_name: taskmanager
    command: taskmanager
    volumes:
      - ./flink-jobs:/opt/flink/jobs
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
    depends_on:
      - jobmanager
```

---

## 6. flink-jobs/Dockerfile (current)

Notes:
- JRE base has no Python — install python3 + pip.
- PyFlink's `pemja` needs a full JDK to compile -> install `openjdk-11-jdk` + set JAVA_HOME.
- JAVA_HOME uses `arm64` (Apple Silicon). On Intel it'd be `amd64`.
- No `--break-system-packages` (older pip in image doesn't support it).

```dockerfile
FROM flink:1.20.0-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

RUN pip3 install --upgrade pip && \
    pip3 install apache-flink==1.20.0

RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
```

---

## 7. Build + start the cluster

First time / after Dockerfile change — build once explicitly to avoid the
double-tag race, then start:
```bash
cd ~/radiation_tracking
docker compose build jobmanager
docker compose up -d
```

Verify:
```bash
docker compose ps                             # kafka, jobmanager, taskmanager all "Up"
docker exec -it jobmanager python --version   # Python 3.10.x
```
Dashboard: http://localhost:8081  -> "Available Task Slots" should be 2.

If macOS asks Docker for Documents folder access -> click Allow (needed for volume mount).

---

## 8. Daily restart routine

`docker compose down` wipes the Kafka topic, so recreate it and resubmit the job:

```bash
cd ~/radiation_tracking
docker compose up -d           # no --build needed (image cached)

# recreate topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic radiation-readings \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# resubmit Flink job
docker exec -it jobmanager flink run -py /opt/flink/jobs/job1_filter.py
```

Check dashboard (http://localhost:8081) -> job shows green/RUNNING.

---

## 9. Run the pipeline (watch data flow)

Two terminals.

Terminal 1 — producer:
```bash
cd ~/radiation_tracking/data-provider
source venv/bin/activate
python producer.py --delay 0.5
```

Terminal 2 — watch Flink output (runs from anywhere):
```bash
docker logs -f taskmanager
```
Filtered radiation dicts appear in Terminal 2 = full pipeline working.

Producer options: `--delay` (speed, 0 = max), `--limit N` (stop after N),
`--topic`, `--broker`, `--file`.

---

## 10. Flink job management

```bash
docker exec -it jobmanager flink list              # list running jobs + IDs
docker exec -it jobmanager flink cancel <jobID>    # cancel a job
```
Or cancel via dashboard: click job -> Cancel Job (top right).
Note: Flink jobs run on the cluster, NOT in a terminal. Closing terminals does
not stop a job. Only `flink cancel`, dashboard cancel, or `docker compose down` stops it.

---

## 11. Kafka admin commands

```bash
# list topics
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092

# delete a topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --delete --topic <name> --bootstrap-server localhost:9092

# console consumer (manual check)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic radiation-readings --bootstrap-server localhost:9092 \
  --from-beginning --max-messages 5
```

Stop on a running command: `Ctrl + C` (Control key, NOT Cmd). Or close the window.

---

## 12. Git

`.gitignore`:
```
data-provider/venv/
data/*.csv
__pycache__/
```

```bash
cd ~/radiation_tracking
git add .
git commit -m "Stage 1: Flink cluster + first operator (filter empty readings)"
git push
```

---

## 13. Status checklist

Stage 1:
- [x] Task 1: Kafka in Docker
- [x] Task 2: Configurable producer replaying Safecast
- [x] Task 3: Flink set up + first operator (filter empty values), verified end-to-end

Stage 2 (later):
- [ ] Web GUI with map
- [ ] Display processed data on map, configurable display speed
- [ ] More operators (threshold alerts, fixed-sensor dedup, area/time filters)
- [ ] Cloud deployment + Docker images (DockerHub) + README + presentation

---

## 14. Where we stopped / next steps

Done: full Kafka -> Flink pipeline live. First operator (drop empty readings)
working, verified dicts flowing through to TaskManager logs.

Next session — add operators (in order of difficulty):
1. Parse `value` to number + threshold alert operator (flag CPM > configurable limit). Easy, demo-friendly. DO THIS NEXT.
2. Fixed-sensor dedup (key by device_id, Flink keyed state). Medium.
3. Capture-time ordering (event time + watermarks). Hardest, most interesting.
   Currently job uses no_watermarks().

Then Stage 2: web map + WebSocket middleware + area/time filters + cloud deploy.

Job file currently uses DataStream API (explicit operators) and reads Kafka at
`kafka:19092`, starting offset = earliest.
1. Introduction
Urban traffic congestion leads to increased travel time, fuel consumption, and pollution. Traditional traffic monitoring systems rely on fixed sensors and periodic reports, which are often delayed and insufficient for dynamic decision‑making. A real‑time analytics system that continuously ingests, processes, and visualizes traffic data can enable smarter traffic management and better commuter experiences. 
This project implements a scalable, event‑driven traffic analytics pipeline using Apache Kafka for high‑throughput data ingestion and Apache Spark Structured Streaming for low‑latency processing. The system computes live metrics such as average speed, congestion levels, and hotspot detection, and exposes them via a dashboard for real‑time monitoring. 
2. Problem Statement
Traffic data from multiple sources (IoT sensors, GPS devices, cameras) arrives continuously and at high velocity.
Conventional batch processing cannot provide timely insights for immediate interventions.
There is a need for a fault‑tolerant, scalable architecture that supports real‑time analytics and alerting.
Goal: Design and implement a real‑time traffic analytics system that ingests streaming traffic data, processes it on the fly, and visualizes actionable insights with minimal latency. 
3. Objectives
Build a real‑time data ingestion pipeline using Apache Kafka. 
Process streaming traffic data with Apache Spark Structured Streaming. 
Compute key metrics: average speed, vehicle count, congestion index, and hotspot zones.
Detect anomalies (e.g., sudden speed drops, accidents) and generate alerts.
Store processed data in a time‑series database for historical analysis.
Visualize live insights on a web dashboard for traffic authorities and commuters. 
4. System Architecture
4.1 High‑Level Architecture
Data Sources
Simulated vehicle GPS logs (latitude, longitude, speed, timestamp, vehicle ID)
Optionally, real data from public transport APIs or IoT sensors 
Message Broker – Apache Kafka
Topics:
raw-traffic: raw sensor/GPS data
enriched-traffic: cleaned and enriched records
alerts: congestion and anomaly alerts 
Kafka producers stream data into raw-traffic at high frequency.
Stream Processing – Apache Spark
Spark Structured Streaming subscribes to raw-traffic.
Performs transformations:
Data cleaning and validation
Geofencing (assigning road segments/zones)
Rolling aggregations (e.g., 5‑minute windows)
Congestion scoring and anomaly detection 
Outputs:
Aggregated metrics to database
Alerts to alerts topic
Storage Layer
PostgreSQL / TimescaleDB: time‑series storage for metrics and historical analysis 
Elasticsearch (optional): for fast search and alert indexing
API & Visualization
REST API (FastAPI/Flask) to query processed data
Web dashboard showing:
Live traffic heatmaps
Congestion trends over time
Real‑time alerts and notifications 
4.2 Data Flow
[Data Sources] 
      ↓ (JSON records)
[Kafka Producer] 
      ↓ (topic: raw-traffic)
[Apache Kafka]
      ↓ (Spark reads from Kafka)
[Apache Spark Streaming]
      ↙                  ↘
[Aggregated Metrics]   [Alerts]
      ↓                    ↓
[PostgreSQL/Timescale]  [Kafka topic: alerts → Dashboard]
      ↓
[REST API] → [Web Dashboard]
5. Technologies and Tools
Apache Kafka – distributed event streaming platform for high‑throughput ingestion 
Apache Spark (Structured Streaming) – unified engine for real‑time data processing 
Python / Scala – for Kafka clients and Spark jobs
PostgreSQL / TimescaleDB – relational/time‑series database for metrics storage 
Docker & Docker Compose – for containerized deployment of Kafka, Spark, and database
FastAPI / Flask – backend API for dashboard data
Plotly / Chart.js / Grafana – visualization components 
6. Data Model
6.1 Raw Traffic Record (JSON)
{
  "vehicle_id": "V12345",
  "timestamp": "2026-07-31T14:23:45Z",
  "latitude": 15.8281,
  "longitude": 78.0373,
  "speed_kmph": 42.5,
  "sensor_id": "S101"
}
6.2 Aggregated Metrics (per road segment, per window)
segment_id
window_start, window_end
avg_speed
vehicle_count
congestion_index (e.g., 0–100 scale)
is_congested (boolean)
6.3 Alert Record
alert_id
timestamp
segment_id
avg_speed
severity (LOW/MEDIUM/HIGH)
message (e.g., “Heavy congestion detected on Segment S12”)
7. Implementation Steps
7.1 Environment Setup
Install Docker and Docker Compose.
Use a docker-compose.yml to spin up:
Kafka + Zookeeper
Spark (master + worker)
PostgreSQL/TimescaleDB
Optional: Grafana for dashboards 
7.2 Kafka Producer (Data Simulator)
Write a Python script that:
Generates synthetic vehicle data (random routes, speeds, timestamps)
Publishes JSON records to raw-traffic topic at 1–5 messages/second
Use confluent-kafka or kafka-python library.
7.3 Spark Streaming Job
Use PySpark Structured Streaming:
Read from Kafka:
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw-traffic") \
    .load()
Parse JSON, extract fields, and add derived columns (e.g., road segment).
Apply windowed aggregations:
from pyspark.sql.functions import window, avg, count

agg_df = df.groupBy(
    window(df.timestamp, "5 minutes"),
    df.segment_id
).agg(
    avg("speed_kmph").alias("avg_speed"),
    count("*").alias("vehicle_count")
)
Compute congestion index and flag congested segments.
Write results to PostgreSQL and alerts to Kafka. 
7.4 Database Schema (PostgreSQL)
CREATE TABLE traffic_metrics (
    id SERIAL PRIMARY KEY,
    segment_id TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_speed REAL,
    vehicle_count INTEGER,
    congestion_index REAL,
    is_congested BOOLEAN
);

CREATE TABLE traffic_alerts (
    id SERIAL PRIMARY KEY,
    alert_time TIMESTAMP NOT NULL,
    segment_id TEXT NOT NULL,
    avg_speed REAL,
    severity TEXT,
    message TEXT
);
7.5 API and Dashboard
Build a FastAPI app with endpoints like:
GET /metrics/latest?segment_id=S12
GET /alerts?severity=HIGH
Create a simple frontend (HTML + JS) that:
Polls the API every few seconds
Displays line charts for speed/congestion over time
Shows a map with colored segments based on congestion level 
8. Expected Results
End‑to‑end pipeline that ingests and processes traffic data in near real‑time (latency of a few seconds to a minute). 
Live dashboard showing:
Current average speeds per road segment
Congestion heatmap or color‑coded routes
Real‑time alerts for severe congestion
Historical charts for trend analysis (e.g., peak hour patterns). 
9. Evaluation Metrics
End‑to‑end latency: time from data generation to dashboard update
Throughput: messages processed per second
Accuracy: correctness of congestion detection (compared to ground truth or rules)
Scalability: ability to handle increased data volume by adding Kafka partitions or Spark workers 
10. Advantages
Real‑time visibility into traffic conditions for faster decision‑making.
Scalable architecture that can grow with city size and data volume.
Modular design allowing integration with existing traffic systems or new data sources. 
11. Challenges and Mitigations
Data quality: simulated or noisy sensor data → apply validation and outlier filtering in Spark.
Latency: large windows increase delay → tune window size and Spark parallelism.
Fault tolerance: component failures → use Kafka’s replication and Spark’s checkpointing. 

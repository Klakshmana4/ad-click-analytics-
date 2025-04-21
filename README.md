# Ad Click Analytics Pipeline

## Overview

This project implements a real-time analytics pipeline using Apache Flink to process ad impression and click events. The pipeline calculates click-through rate (CTR) by ad campaign and user engagement metrics by device type. It also includes anomaly detection to identify unusual CTR patterns.

## Requirements

- Docker 20.10+
- Docker Compose 2.20+

## Components

- **Kafka**: Message broker for ingesting ad impression and click events.
- **Flink**: Stream processing engine for real-time analytics.
- **ZooKeeper**: Coordination service for Kafka.
- **Data Generator**: Python script to generate mock ad events.
- **Grafana**: Dashboard for visualizing metrics 

## Setup Instructions

1.  **Clone the repository:**

    ```
    git clone <repository_url>
    cd ad-click-analytics
    ```

2.  **Start the services with Docker Compose:**

    ```
    docker-compose up -d --build
    ```

    This command builds and starts all the required services in detached mode.

3.  **Submit the Flink job:**

    ```
    docker exec flink-jobmanager ./bin/flink run -py /opt/flink_jobs/ads_analytics.py
    ```

    This command executes the Flink job that processes the data streams.

## Accessing the Services

-   **Flink Dashboard**: `http://localhost:8081`

    Use this dashboard to monitor the Flink job, check for errors, and view task execution details.

-   **Grafana**: `http://localhost:3000` (optional)

    The default login credentials are `admin/admin`.  Configure Grafana to visualize the metrics produced by the Flink job.

## Verifying the Setup

1.  **Check Kafka Topics:** Verify that the `ad-impressions` and `ad-clicks` topics are created and receiving messages.  You can use a Kafka command-line consumer to inspect the messages:

    ```
    docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic ad-impressions --from-beginning
    docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic ad-clicks --from-beginning
    ```

2.  **Monitor Flink Job:** Access the Flink dashboard to ensure the job is running without errors and processing data.

## Configuration

The `docker-compose.yml` file allows you to configure various aspects of the environment:

-   **Kafka**:  Adjust the number of partitions, replication factor, and other Kafka settings.

-   **Data Generator**:  Modify the event generation rate, click ratio, and other parameters in the `data-generator` service definition.  Important environment variables include:

    -   `EVENT_RATE`:  Events per second (default: 50).
    -   `CLICK_RATIO`:  Percentage of impressions that result in clicks (default: 0.1).
    -   `IMPRESSION_TOPIC`: Kafka topic for impression events.
    -   `CLICK_TOPIC`: Kafka topic for click events.

-   **Flink**: Configure the number of task slots and other Flink-specific settings.

## Data Format

The data generator produces events in the following JSON format:

{
"impression_id": "imp-123456789",
"user_id": "user-123",
"campaign_id": "camp-456",
"ad_id": "ad-789",
"device_type": "mobile",
"browser": "chrome",
"event_timestamp": 1647890123456,
"cost": 0.01
}

{
"click_id": "click-987654321",
"impression_id": "imp-123456789",
"user_id": "user-123",
"event_timestamp": 1647890128456
}


## Flink Job Details

The Flink job (`flink_jobs/ads_analytics.py`) performs the following steps:

1.  **Reads data from Kafka topics** (`ad-impressions` and `ad-clicks`).

2.  **Calculates the Click-Through Rate (CTR)**: Calculates CTR per campaign in 1-minute windows.
    *   Uses `TumblingEventTimeWindows` for time-based aggregation.
    *   The `CTRCalculator` function calculates the CTR by dividing the number of clicks by the number of impressions for each campaign.

3.  **Implements Anomaly Detection**:  Identifies campaigns with unusual CTR patterns using a simple statistical method (e.g., comparing against a moving average or standard deviation).
    *   The `CTRAnomalyDetector` function checks for significant deviations from the historical CTR.

4.  **Writes results to a Kafka topic** (`ctr-metrics`).

## Anomaly Detection

The anomaly detection logic identifies significant deviations in CTR from the historical average for each campaign. If the current CTR is outside a certain threshold (e.g., 3 standard deviations) from the average, it's flagged as an anomaly.

## Stopping the Environment

To stop and remove the containers, run:

docker-compose down



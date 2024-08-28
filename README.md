Realtime Election Voting System
===============================

This repository contains the code for a realtime election voting system. The system is built using Python, Kafka, Spark Streaming, Postgres and Streamlit. The system is built using Docker Compose to easily spin up the required services in Docker containers.

## System Architecture
![system_architecture.jpg](images%2Fsystem_architecture.jpg)

## System Flow
![system_flow.jpg](images%2Fsystem_flow.jpg)

## System Components
- **kafka-admin.py**: This is the Python code file used to initialize the admins and client_id, as well as the necessary topics for data streaming (`voters_topic`, `votes_topic`, `aggregated_votes_per_candidate`, `aggregated_turnout_by_location`).
- **main.py**: This is the main Python script that creates the required tables on postgres (`candidates`, `voters` and `votes`), it also creates the Kafka topic and creates a copy of the `votes` table in the Kafka topic. It also contains the logic to consume the votes from the Kafka topic and produce data to `voters_topic` on Kafka.
- **voting.py**: This is the Python script that contains the logic to consume the votes from the Kafka topic (`voters_topic`), generate voting data and produce data to `votes_topic` on Kafka.
- **spark-streaming.py**: This is the Python script that contains the logic to consume the votes from the Kafka topic (`votes_topic`), enrich the data from postgres and aggregate the votes and produce data to specific topics on Kafka.
- **streamlit-app.py**: This is the Python script that contains the logic to consume the aggregated voting data from the Kafka topic as well as postgres and display the voting data in realtime using Streamlit.

### Running the App
1. Install the required Python packages using the following command:

```bash
pip install -r requirements.txt
```

2. Initialize the KafkaAdminClient and create the necessary topics:

```bash
python kafka-admin.py
```

3. Creating the required tables on Postgres and generating voter information on Kafka topic:

```bash
python main.py
```

4. Consuming the voter information from Kafka topic, generating voting data and producing data to Kafka topic:

```bash
python voting.py
```

5. Consuming the voting data from Kafka topic, enriching the data from Postgres and producing data to specific topics on Kafka:

```bash
python spark-streaming.py
```

6. Running the Streamlit app:

```bash
streamlit run streamlit-app.py
```

## Screenshots
### Candidates and Parties information
![candidates_and_party.png](images/candidates_and_party.png)
### Voters
![voters.png](images%2Fvoters.png)

### Voting
![voting.png](images%2Fvoting.png)

### Dashboard
![dashboard_image.png](images%2Fdashboard_image.png)

## Video
[![Realtime Voting System Data Engineering](https://img.youtube.com/vi/X-JnC9daQxE/0.jpg)](https://youtu.be/X-JnC9daQxE)

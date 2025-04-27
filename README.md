# Real-Time Named Entity Tracker with Kafka, Spark, and ELK
---

## Dependencies

Install the following before running the project:

### Python (via pip)
- `pyspark`
- `finnhub-python`
- `pandas`
- `re`, `time` (standard library)

### Platforms & Tools
- **Kafka:** `kafka_2.13-4.0.0`
- **Apache Spark:** `spark-3.4.4-bin-hadoop3-scala2.13`
- **ELK Stack:** Elasticsearch + Logstash + Kibana (tested with 8.x+)

---

## How to Run

### 0. Create Finnhub account get API key & Create Kafka Topics
- Update API key in headline_producer.py and producer_alternate.py
- Create two Kafka Topics--> finnhub_headlines and ner_counts

### 1. Start the ELK Stack
- Run **Elasticsearch** (default: `localhost:9200`)
- Run **Kibana** (default: `localhost:5601`)

### 2. Start Kafka
Open a terminal and navigate to your Kafka directory:
```bash
cd path/to/kafka/
bin/kafka-server-start.sh config/server.properties
```
### 3. Run Logstash
- First, get Kabana credentials, open logstash.conf and update password to your assigned Kibana password
- Open a terminal and navigate to your Project directory:
```bash
cd path/to/project/folder
logstash -f logstash.conf
```

### 4. Start Consumer and Producer Respectively
- Open a terminal and navigate to your Project directory:
```bash
cd path/to/project/folder
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4 headline_[consumer/producer].py
```
- Note that there are two different options for producers--> if you want "live" headline streaming then use headline_producer.py but it updates VERY slowly on the weekends especially due to markets close
- If you want to simulate streaming data via batch processing then chose producer_alternate.py
- Consumer for both remains the same.
  
### 5. Login to Kibana
Current ElasticSearch index in Logstash is set up as test2
- Add that to index in Kibana and use the discover/dashboard feature to plot count vs entities plot


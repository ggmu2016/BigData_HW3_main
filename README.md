Install dependencies:
	- Python (pip): PySpark, re, finnhub, pandas, time
 	- Kafka_2.13-4.0.0
	- spark-3.4.4-bin-hadoop3-scala2.13
	- ELK Stack 


How to Run:
	1. Run Kibana (localhost port 5601 default) and ElasticSearch (port 9200)
	2. Open a Terminal Window-> cd into Kafka folder -> run: bin/kafka-server-start.sh config/server.properties  (after its already been set up once)
 	3. Open another Terminal Window -> cd into Project folder -> run logstash file: logstash -f logstash.conf
	4. Open another Terminal Window -> cd into Project folder -> run consumer: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4 headline_consumer.py
	5. Open another Terminal Window -> cd into Project folder -> run producer: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4 headline_producer.py
 	6. Open Kibana, login, create index: test2, and then create the dashboard as necessary to see the plots
	

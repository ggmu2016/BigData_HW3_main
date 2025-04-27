import finnhub
import time
from pyspark.sql import SparkSession

# Finnhub api--> ENTER YOUR API KEY here!!
api_key = "ADD YOUR API KEY HERE"

finnhub_client = finnhub.Client(api_key=api_key)

# get all historical news
news_arr = finnhub_client.company_news('TSLA', _from="2024-04-25", to="2025-04-25")

# Sort by datetime
news_arr = sorted(news_arr, key=lambda x: x['datetime'])

# Extract headlines into list
headlines = []
for news in news_arr:
    headline = news.get('headline', '').strip()
    if headline:
        headlines.append((headline,))

# Create Spark Session
spark = SparkSession.builder \
    .appName("FinnhubProducer") \
    .getOrCreate()

# Batch settings
batch_size = 40
sleep_time = 60

for i in range(0, len(headlines), batch_size):
    batch = headlines[i:i+batch_size]

    batch_df = spark.createDataFrame(batch, ["headline"])

    # Write to kafka
    query = (batch_df.selectExpr("CAST(headline AS STRING) as value")
             .write
             .format("kafka")
             .option("kafka.bootstrap.servers", "localhost:9092")
             .option("topic", "finnhub_headlines")
             .save())

    time.sleep(sleep_time)

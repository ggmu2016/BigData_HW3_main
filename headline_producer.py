import time

import finnhub
from pyspark.sql import SparkSession
import pandas as pd


# setup client (obtain API key for free from Finnhub.io)
finnhub_client = finnhub.Client(api_key="d01dbfhr01qile5uvjugd01dbfhr01qile5uvjv0")


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("FinnhubWordCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    last_seen_id = 0
    while True:
        # General market news
        news_arr = finnhub_client.general_news('crypto', min_id=last_seen_id)
        if not news_arr:
            print("No new headlines")
            time.sleep(60)
            continue

        headlines = []
        for news in news_arr:
            headline = news['headline']
            news_id = news['id']
            headlines.append((headline,))
            last_seen_id = max(last_seen_id, news_id)

        #converting to df to use in spark
        df = pd.DataFrame(headlines, columns=['value'])
        spark_df = spark.createDataFrame(df)

        # Write to Kafka topic (topic = finnhub_headlines)
        query = (spark_df.selectExpr("CAST(value AS STRING) AS value").write.format("kafka").
              option("kafka.bootstrap.servers", "localhost:9092").
              option("topic","finnhub_headlines").save())

        print(f"Pushed {len(headlines)} headlines to Kafka. Latest ID: {last_seen_id}")
        time.sleep(60)

import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct
from collections import Counter

# Load spacy
nlp = spacy.load("en_core_web_sm")

def get_named_entities(text):
    if text is None or text.strip() == "":
        return []
    doc = nlp(text)
    return [ent.text for ent in doc.ents]

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    rows = batch_df.select("headline").collect()

    all_entities = []

    for row in rows:
        headline = row["headline"]

        # Extract named entities
        ents = get_named_entities(headline)
        if ents:
            all_entities.extend(ents)

    # counting
    entity_counts = Counter(all_entities)
    top_10 = entity_counts.most_common(10)

    if not top_10:
        return

    # spark df
    spark = SparkSession.builder.getOrCreate()
    top10_rows = [{"entity": entity, "count": count} for entity, count in top_10]
    top10_df = spark.createDataFrame(top10_rows)

    # Write to Kafka
    output_df = top10_df.selectExpr(
        "CAST(entity AS STRING) as key",
        "to_json(struct(entity, count)) as value"
    )

    output_df.show(truncate=False)

    output_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "ner_counts") \
        .save()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FinnhubConsumer") \
        .getOrCreate()

    headlines = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "finnhub_headlines")
        .load()
        .selectExpr("CAST(value AS STRING) as headline")
    )

    # remove empty headlines
    headlines = headlines.filter("headline IS NOT NULL AND headline != ''")

    query = (
        headlines.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "tmp/ner_kafka_checkpoint")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()

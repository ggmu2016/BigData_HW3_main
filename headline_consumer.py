"""
Create a PySpark structured streaming application that continuously reads data from the Kafka topic (topic1)
and keeps a running count of the named entities being mentioned.
You should already know how to extract named entities from text.
In this part, you will keep a running count of the named entities being mentioned.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,udf,split, desc, asc, to_json, struct
from pyspark.sql.types import ArrayType, StringType

import nltk
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')
from nltk import word_tokenize, pos_tag, ne_chunk
from nltk.tree import Tree


# load spacy english words (medium sized)
#nlp = spacy.load("en_core_web_sm")

# spark user-defined function (UDF) to convert text into NER
"""
#SPACY option

def get_named_entities(text):
    try:
        if text is None or text.strip() == "":
            return []
        import spacy
        nlp = spacy.load("en_core_web_sm")
        doc = nlp(text)
        return [ent.text for ent in doc.ents]
    except Exception as e:
        return []
"""
""" 
#NLTK OPTION

def get_named_entities(text):
    try:
        if text is None or text.strip() == "":
            return []

        chunks = ne_chunk(pos_tag(word_tokenize(text)))
        entities = []

        for chunk in chunks:
            if isinstance(chunk, Tree):
                entity = " ".join(c[0] for c in chunk)
                entities.append(entity)

        return entities
    except:
        return []
"""


# simulating non NLP

def get_named_entities(text):
    try:
        if text is None or text.strip() == "":
            return []

        # Simulate NER: extract capitalized words only (like Apple, Tesla)
        tokens = text.strip().split()
        entities = [word for word in tokens if word.istitle() and word.isalpha()]

        # Optional: remove noise like "The", "In", etc.
        stopwords = {"The", "A", "An", "In", "On", "Of", "For", "At", "To", "From"}
        return [e for e in entities if e not in stopwords]

    except Exception:
        return []


ner_udf = udf(get_named_entities, ArrayType(StringType()))

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("FinnhubNERConsumer") \
        .getOrCreate()

    # READ from Kafka: Value that is read from kafka is a df, so it includes ~25 headlines
    headlines = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", "localhost:9092")
                 .option("subscribe", "finnhub_headlines")
                 .load()
                 .selectExpr("CAST(value AS STRING) as headline"))

    headlines = headlines.filter("headline IS NOT NULL AND headline != ''")

    # Do NER to each headline (using Spacy md) in parallel using spark df
    entities = headlines.withColumn("entities", ner_udf("headline"))
    entity_counts = entities.select(explode("entities").alias("entity")).groupBy("entity").count()
    entity_counts_top10 = entity_counts.orderBy(desc("count")).limit(10)

    # converting df to json for logstash
    json_df = entity_counts_top10.select(to_json(struct("entity","count")).alias("value"))


    # Send to another Kafka topic (ner_counts) at trigger time
    query = (json_df.writeStream
             .format("kafka")
             .outputMode("complete")
             .option("kafka.bootstrap.servers", "localhost:9092")
             .option("topic","ner_counts")
             .option("checkpointLocation", "BigData_HW3/tmp/ner_kafka_checkpoint")
             .trigger(processingTime='60 seconds')
             .start())

    query.awaitTermination()
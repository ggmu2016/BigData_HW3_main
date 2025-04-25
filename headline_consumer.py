"""
Create a PySpark structured streaming application that continuously reads data from the Kafka topic (topic1)
and keeps a running count of the named entities being mentioned.
You should already know how to extract named entities from text.
In this part, you will keep a running count of the named entities being mentioned.
"""
import sys
import spacy
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,udf,split, desc, asc, to_json, struct
from pyspark.sql.types import ArrayType, StringType

'''
#############################
SPACY CAUSING CONSUMER TO CRASH
##############################

# load spacy english words (small sized)
nlp = spacy.load("en_core_web_sm")

def get_named_entities(text):
    try:
        if text is None or text.strip() == "":
            return []
        doc = get_spacy_pipeline(text)
        return [ent.text for ent in doc.ents]
    except Exception as e:
        return []
'''


nlp = None

def get_spacy_pipeline(lang="en_core_web_sm"):
    global nlp
    if nlp is None:
        nlp = spacy.load(lang)
    return nlp

def get_named_entities(text):
    try:
        if text is None or text.strip() == "":
            return []
        pipeline = get_spacy_pipeline()
        doc = pipeline(text)
        return [ent.text for ent in doc.ents]
    except Exception as e:
        return []

'''
# Using Rule-based NER because both Spacy and NLTK fail
def get_named_entities(text):
    try:
        if not text or text.strip() == "":
            return []

        # Tokenize + punctuation removal
        tokens = re.findall(r'\b[A-Z][a-z]{2,}\b', text)

        # Combine sequences of capitalized words
        full_entities = []
        current = []

        for word in text.split():
            if word.istitle() and len(word) >= 3:
                current.append(word)
            else:
                if current:
                    full_entities.append(" ".join(current))
                    current = []
        if current:
            full_entities.append(" ".join(current))

        # Filter out travial stopwords
        stop_sim = {"The", "This", "That", "And", "But", "With", "For", "From", "Into", "Analysts", "It", "A",
                    "They"}
        return [ent for ent in full_entities if ent not in stop_sim and len(ent) > 4]

    except Exception as e:
        return []

'''

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
             .option("checkpointLocation", "tmp/ner_kafka_checkpoint")
             .trigger(processingTime='60 seconds')
             .start())

    query.awaitTermination()
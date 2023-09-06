from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import nltk
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')

from nltk import ne_chunk, pos_tag, word_tokenize


def extract_named_entities_nltk(line):
    # Tokenize the text into words
    words = word_tokenize(line)
    
    # Part-of-speech tagging
    tagged_words = pos_tag(words)
    
    # Perform named entity recognition using NLTK's ne_chunk
    named_entities = ne_chunk(tagged_words)
    
    # Extract the named entities as a list of strings
    named_entities_list = [' '.join([word for word, pos in leaf]) for leaf in named_entities if isinstance(leaf, nltk.Tree)]
    
    return named_entities_list

spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

    # Create DataSet representing the stream of input lines from kafka
lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "topic1")\
        .load()

extract_named_entities_udf = udf(extract_named_entities_nltk)

    # Apply the UDF to each row in the DataFrame
lines_with_named_entities = lines.withColumn("named_entities", extract_named_entities_udf(col("value").cast("string")))

    # Explode the named_entities column to separate rows for each named entity
words = lines_with_named_entities.select(
        explode(split(col("named_entities"),", ")).alias("word")
    )

    # Generate running word count
wordCounts = words.groupBy('word').count()

sorted_word_counts = wordCounts.orderBy(desc('count'))

    # Take the top 10 words
top_10_words = sorted_word_counts.limit(10)

result_df = top_10_words.withColumn("value", to_json(struct("*")).cast("string"))

 # query = top_10_words\
    #     .writeStream\
    #     .outputMode('complete')\
    #     .format('console')\
    #     .start()

    # query.awaitTermination()

write_query = result_df.writeStream \
    .outputMode("complete") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "topic2") \
    .option("checkpointLocation", "./checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

write_query.awaitTermination()
 

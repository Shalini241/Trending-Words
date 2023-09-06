#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Consumes messages from one or more topics in Kafka and does wordcount.
 Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
   comma-separated list of host:port.
   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
   'subscribePattern'.
   |- <assign> Specific TopicPartitions to consume. Json string
   |  {"topicA":[0,1],"topicB":[2,4]}.
   |- <subscribe> The topic list to subscribe. A comma-separated list of
   |  topics.
   |- <subscribePattern> The pattern used to subscribe to topic(s).
   |  Java regex string.
   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
   |  specified for Kafka source.
   <topics> Different value format depends on the value of 'subscribe-type'.

 Run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_kafka_wordcount.py \
    host1:port1,host2:port2 subscribe topic1,topic2`
"""
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from newsapi import NewsApiClient

def fetch_top_headlines():
    top_headlines = newsapi.get_top_headlines(country='us')
    return top_headlines["articles"]

    # Function to convert data to JSON string
def to_json_string(data):
        import json
        return json.dumps(data)

if __name__ == "__main__":
    
    kafka_topic = "topic1"
    kafka_bootstrap_servers = "localhost:9092"

    # Init
    newsapi = NewsApiClient(api_key='0a212728834e402a9d35bf119ce8e548')

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Read top headlines from NewsAPI as a streaming DataFrame
    news_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .option("startingOffsets", "latest") \
        .load() \
        .withColumn("value", lit(to_json_string(fetch_top_headlines())))

        # Write the streaming DataFrame to Kafka
    query = news_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_topic) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/cp") \
        .start()

        # Wait for the termination of the query (use query.awaitTermination() for a long-running job)
    query.awaitTermination()
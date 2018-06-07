# Analysing Twitter Data using spark-streaming
Spark Streaming is a component of Spark that provides highly scalable, fault-tolerant
streaming processing. It is a near real time streaming system .Data can be ingested from
many sources like Kafka , twitter and Flume. this spark
application  accomplish the following tasks:-
1. Get the stream of hashtags from twitter.
2. Count the hashtags over a 3 minute window.
3. Find the top 4 hashtags based on their count.
4. Find the tweets that contain specific hashtag ex: #bigdata
package com.mycompany.twitterstreaming;

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.*;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
public class TweetStream {
    public static void main(String[] args) {
        final String consumerKey = "j0nZhiPXIWoM2v3hZ5KOFvJq4";
        final String consumerSecret = "JJgA7Pzbd68j16X0AaDkdVahUpF2wZX4kBYb2IGofF2geLVisK";
        final String accessToken = "982412696481095680-aFZfHxRoEV7TEhGDrdJamMRCl6D7Fas";
        final String accessTokenSecret = "kB6kkZ9nc4JRyKNWSy3PQA44yK2CFcfXLU8yDPtjdN0Cf";

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTwitter");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

       
       //Get the stream of hashtags from twitter
      JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);
      
        // Without filter: Output text of all tweets
        JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {    
                    public String call(Status status) { return status.getText(); }
                }
        );
        
        JavaDStream<String> words = statuses.flatMap(
        new FlatMapFunction<String, String>() {
       public Iterable<String> call(String in) {
         return Arrays.asList(in.split(" "));
       }
     }
   );
   JavaDStream<String> hashTags = words.filter(
     new Function<String, Boolean>() {
       public Boolean call(String word) { return word.startsWith("#"); }
     }  
   );
   
   JavaDStream<String> tweets = statuses.filter(
     new Function<String, Boolean>() {
       public Boolean call(String word) { return word.contains("#bigdata"); }
     }  
   ); 
   System.out.println("\n\n-----Tweets that contain #bigdata------");
   tweets.print();
     //Count the hashtags over a 3 minute window
     
     JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String in) throws Exception{
          return new Tuple2<>(in, 1);
        }
      });
    JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer i1, Integer i2) { return i1 + i2; }
      },
      // Reduce last 3 minutes of data, every 1 second
      new Duration(60 * 3 * 1000), //specifies the window size 
      new Duration(10 * 1000)//specifies the movement of the window
    );
   
    /* JavaPairDStream<Integer,String> swappedPair = counts.mapToPair(x -> x.swap());
    JavaPairDStream<Integer,String> sortedStream = swappedPair.transformToPair(
     new Function<JavaPairRDD<Integer,String>, JavaPairRDD<Integer,String>>() {
         @Override
         public JavaPairRDD<Integer,String> call(JavaPairRDD<Integer,String> jPairRDD) throws Exception {
                    return jPairRDD.sortByKey(false);
                  }
              });*/
    counts.print();
      
      JavaPairDStream<Integer,String> swappedPair = counts.mapToPair(Tuple2::swap);  
      JavaPairDStream<Integer,String> sortedStream = swappedPair.transformToPair(s -> s.sortByKey(false));
      sortedStream.foreachRDD((JavaPairRDD<Integer, String> happinessTopicPairs) -> {
          
          List<Tuple2<Integer, String>> topList = happinessTopicPairs.take(4);
          topList.stream().forEach((pair) -> {
              System.out.println(
                      String.format("%s : (%s)", pair._2(), pair._1()));
          });
          
          
          return null;
        });
      //JavaPairDStream<Integer,String> topStream = sortedStream.foreachRDD(rdd -> {return rdd.take(4); });
    //System.out.println("\n\n------------------The Top 4 hashtags ------------------");
    //sortedStream.print();
    jssc.start();
    jssc.awaitTermination();
}
}
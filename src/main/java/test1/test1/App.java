package test1.test1;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;




public class App 
{
    private static final String KAFKA_BROKER_LIST = "localhost:9092";
    private static final int STREAM_WINDOW_MILLISECONDS = 10000; // 10 seconds
    private static final Collection<String> TOPICS = Arrays.asList("kr");
    public static final String APP_NAME = "Kafka Spark Streaming App";
    private static Map<String, Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new HashMap();
        //kafkaParams.put("bootstrap.servers", KAFKA_BROKER_LIST);
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "g6");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }
    
    public static void main( String[] args ) throws InterruptedException
    {
        System.out.println( "Hello World!" );
        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(STREAM_WINDOW_MILLISECONDS));
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(TOPICS, getKafkaParams())
                );
        
        stream.foreachRDD(rdd ->
        {
        	System.out.print(" Hello .....................................................");
        	if(rdd.isEmpty()) {
        		System.out.print("\n RDD IS EMPTY ...................\n");
        	}
        	else {
        		System.out.print("\n No Of Messages : ");
        		System.out.print(rdd.count());
        		System.out.print("\n");
        		rdd.foreach(line -> {
        			System.out.print("\n Message : ");
        			System.out.print(line.value());
        			System.out.print("\n");
        		});
        	}
            
          
        });
        
        /*
        stream.map(x-> x.toString()).foreachRDD(r -> {
        	System.out.print("\n No Of Partitions \n");
        	
        	System.out.print(r.getNumPartitions());
        	
        	int numRecords = (int) r.count();
        	System.out.print("\n No Of Messages : \n");
        	System.out.print(numRecords);
        });
        
        */

        ssc.start();
        ssc.awaitTermination();
        
        
        
    }
}

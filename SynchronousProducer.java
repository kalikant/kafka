package com.scb.edm.kafak.poc;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import org.apache.kafka.clients.producer.*;
public class SynchronousProducer {

   public static void main(String[] args) throws Exception{

      String topicName = "SynchronousProducerTopic";
          String key = "Key1";
          String value = "Value-1";
          
          Properties props = new Properties();
          try {
			   props.load(new FileInputStream("kafka.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
      Producer<String, String> producer = new KafkaProducer <>(props);
      ProducerRecord<String, String> record = new ProducerRecord<>(topicName,key,value);

      try{
           RecordMetadata metadata = producer.send(record).get();
           System.out.println("Message is sent to Partition no " + metadata.partition() + " and offset " + metadata.offset());
           System.out.println("SynchronousProducer Completed with success.");
      }catch (Exception e) {
           e.printStackTrace();
           System.out.println("SynchronousProducer failed with an exception");
      }finally{
           producer.close();
      }
   }
}

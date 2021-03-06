package com.kafka.consumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class Consumer {
	public static void main(String[] args) {

		String topic = "2Partitionedtopic";
		String brokers = "XXXXXXX:9093";

		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", "grp1");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("2Partitionedtopic"));
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("HI");
				System.out.printf("offset = %d, key = %s, value = %s",
						record.offset(), record.key(), record.value());
			}
		}
	}

}

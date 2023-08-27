package com.madlabs.kafka.avro;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.example.CustomerV1;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class KafkaAvroConsumer {

	static String topic = "customer-avro";

	public static void main(String[] args) {

		Properties prop = new Properties();
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "customer-avro-consumer");
		prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		prop.setProperty("schema.registry.url", "http://localhost:8081");
		prop.setProperty("specific.avro.reader", "true");

		KafkaConsumer<String, CustomerV1> consumer = new KafkaConsumer<String, CustomerV1>(prop);

		consumer.subscribe(Arrays.asList(topic));

		while (true) {
			ConsumerRecords<String, CustomerV1> records = consumer.poll(Duration.ofSeconds(1));
			for (ConsumerRecord<String, CustomerV1> record : records) {
				System.out.println(record.offset() + " Customer Value: " + record.value().toString());
			}
			consumer.commitSync();
		}

	}
}

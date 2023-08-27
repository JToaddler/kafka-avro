package com.madlabs.kafka.avro;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.example.CustomerV1;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class KafkaAvroProducer {

	static String topic = "customer-avro";

	public static void main(String[] args) {

		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.setProperty(ProducerConfig.ACKS_CONFIG, "1");
		prop.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		prop.setProperty("schema.registry.url", "http://localhost:8081");
		

		KafkaProducer<String, CustomerV1> producer = new KafkaProducer<String, CustomerV1>(prop);

		CustomerV1 customer = CustomerV1.newBuilder().setFirstName("FName").setLastName("Doe").setAge(26)
				.setWeight(81.2f).setHeight(178.5f).setAutomatedEmail(false).build();

		ProducerRecord<String, CustomerV1> producerRecord = new ProducerRecord<String, CustomerV1>(topic, customer);
		producer.send(producerRecord, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				System.out.println("!!");
				if (exception != null) {
					System.out.println("exception");
					exception.printStackTrace();
				} else {
					System.out.println("Success!!");
					System.out.println(metadata.toString());
				}
			}
		});
		producer.flush();
		producer.close();
	}

}

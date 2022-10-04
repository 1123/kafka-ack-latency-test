package io.confluent.samples.KafkaCommitLatencyTest;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * This application fetches many messages using a single poll() call.
 * For this to work, we must set the fetch.min.bytes, max.partition.fetch.bytes and max.poll.records
 * settings accordingly. Yet this may lead to longer rebalances, and is therefore not recommended.
 */

@Slf4j
public class KafkaCommitLatencyTest  {

	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
		Properties properties = new Properties();
		properties.load(new FileInputStream("src/main/resources/producer-ccloud.properties"));
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		for (int i = 0; i < 100; i++) {
			var beforeTime = System.currentTimeMillis();
			Future<RecordMetadata> result = producer.send(new ProducerRecord<>("test-topic", "foo", "bar"));
			result.get();
			var timeDiff = System.currentTimeMillis() - beforeTime;
			log.info("timeDiff: {}", timeDiff);
		}
		producer.close();
	}

}

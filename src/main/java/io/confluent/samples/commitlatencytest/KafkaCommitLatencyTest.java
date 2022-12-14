package io.confluent.samples.commitlatencytest;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

	public static int numThreads = 16;

	public static void main(String[] args) throws IOException, InterruptedException {
		Properties properties = new Properties();
		String e = System.getenv("ENVIRONMENT");
		String env = (e == null || e.isEmpty()) ? "local" : e;
		log.info("Using environment: {}", env);
		properties.load(new FileInputStream(String.format("src/main/resources/producer-%s.properties", env)));
		try (AdminClient kafkaAdminClient = KafkaAdminClient.create(properties)) {
			CreateTopicsResult createTopicsResult = kafkaAdminClient.createTopics(Collections.singleton(new NewTopic("test-topic", 6, (short) 3)));
			createTopicsResult.all().get();
		} catch(ExecutionException topicExistsException) {
			log.warn(topicExistsException.getMessage());
		}
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			List<ProducerThread> threads = new ArrayList<>();
			for (int t = 0; t < numThreads; t++) {
				ProducerThread producerThread = new ProducerThread(producer);
				threads.add(producerThread);
				producerThread.start();
			}
			threads.forEach(thread -> {
				try {
					thread.join();
				} catch (InterruptedException ex) {
					ex.printStackTrace();
				}
			});
		}
	}

}


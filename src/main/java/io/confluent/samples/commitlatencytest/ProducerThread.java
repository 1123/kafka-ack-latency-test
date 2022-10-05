package io.confluent.samples.commitlatencytest;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
public class ProducerThread extends Thread {

    int numMessagesPerBatch;
    int numBatches;
    int numKeyChars;
    int numValueChars;
    int batchSleepMs;

    private final KafkaProducer<String, String> producer;
    private final RandomStringGenerator randomStringGenerator;

    public ProducerThread(KafkaProducer<String, String> producer) {
        try {
            numMessagesPerBatch = Integer.parseInt(System.getenv("NUM_MESSAGES_PER_BATCH"));
            numBatches = Integer.parseInt(System.getenv("NUM_BATCHES"));
            numKeyChars = Integer.parseInt(System.getenv("NUM_KEY_CHARS"));
            numValueChars = Integer.parseInt(System.getenv("NUM_VALUE_CHARS"));
            batchSleepMs = Integer.parseInt(System.getenv("BATCH_SLEEP_MS"));
        } catch (NumberFormatException e) {
            log.error("Could not parse configuration options. Make sure you have the following environment variables set: \n " +
                    "NUM_MESSAGES_PER_BATCH, NUM_BATCHES, NUM_KEY_CHARS, NUM_VALUE_CHARS, BATCH_SLEEP_MS");
            log.error(e.getMessage());
            System.exit(1);
        }
        this.producer = producer;
        randomStringGenerator =
                new RandomStringGenerator.Builder()
                        .withinRange('0', 'z')
                        .build();
    }

    @SneakyThrows
    public void run() {
        var totalTimeDiff = 0;
        for (int i = 0; i < numBatches; i++) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            var key = randomStringGenerator.generate(numKeyChars);
            var value = randomStringGenerator.generate(numValueChars);
            var beforeTime = System.currentTimeMillis();
            for (int j = 0; j < numMessagesPerBatch; j++) {
                futures.add(producer.send(
                        new ProducerRecord<>("test-topic", key, value)
                ));
            }
            futures.forEach(f -> {
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException ex) {
                    ex.printStackTrace();
                }
			});
            var timeDiff = System.currentTimeMillis() - beforeTime;
            totalTimeDiff += timeDiff;
            log.info("timeDiff: {}", timeDiff);
            Thread.sleep(batchSleepMs);
        }
        log.info("average wait: {}", totalTimeDiff / numBatches);
    }

}

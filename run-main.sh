export ENVIRONMENT=docker
export NUM_MESSAGES_PER_BATCH=5
export NUM_BATCHES=1000
export NUM_KEY_CHARS=10
export NUM_VALUE_CHARS=5000
export BATCH_SLEEP_MS=10
mvn compile exec:java -Dexec.mainClass="io.confluent.samples.commitlatencytest.KafkaCommitLatencyTest"

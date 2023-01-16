package com.kafka.biginer.kafka.tutorial.consumer;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerThread implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerThread.class);

    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;

    public ConsumerThread(String bootstrapServer, String groupId,
            String topic, CountDownLatch latch) {
        this.latch = latch;
        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(singleton(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("Key: " + record.key());
                    LOGGER.info("Value: " + record.value());
                    LOGGER.info("Partition: " + record.partition());
                    LOGGER.info("Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            LOGGER.info("Receiving shut down msg");
        }
        finally {
            consumer.close();
            // tell our main code that we are done with the consumer
            latch.countDown();
        }
    }

    public void shutDown() {
        // to interrupt consumer.poll() and it will throw WakeUpException
        consumer.wakeup();
    }
}

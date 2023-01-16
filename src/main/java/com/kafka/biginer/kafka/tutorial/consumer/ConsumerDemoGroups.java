package com.kafka.biginer.kafka.tutorial.consumer;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoGroups {

    private static final String BOOTSTRAP_SERVERS = "c02f735jmd6r.dhcp.sofl.sap.corp:9092";
    private static final String GROUP_ID = "my-fifth-application";
    private static final String TOPIC = "first_topic";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoGroups.class);

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(singleton(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("Key: " + record.key() + "Value: " + record.value());
                LOGGER.info("Partition: " + record.partition());
                LOGGER.info("Offset: " + record.offset());
            }
        }
    }
}

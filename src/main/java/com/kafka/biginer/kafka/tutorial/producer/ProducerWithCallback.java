package com.kafka.biginer.kafka.tutorial.producer;

import static java.lang.String.format;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerWithCallback.class);

    private static final String BOOTSTRAP_SERVERS = "192.168.1.103:9092";
    private static final String TOPIC = "twitter_tweets";

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        for (int i = 0; i < 10; i++) {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC,
                        "key" + i, createMsg(i));

                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (e == null) {
                        System.out.println(
                                format("Received new metadata. %nTopic: %s%nPartition: %s%nOffset: %s%nTimestamp: %s%n",
                                        recordMetadata.topic(),
                                        recordMetadata.partition(),
                                        recordMetadata.offset(),
                                        recordMetadata.timestamp())
                        );
                    } else {
                        LOGGER.error("Error while processing: ", e);
                    }
                });
            }
        }
    }

    private static String createMsg(int i) {
        if (i % 2 == 0) {
            return format(
                    "{'@timestamp': '2022-04-08T13:55:32Z', 'level': 'warn', 'message': '{Some log message-%s}'}",
                    i);
        }

        return "{'@timestamp': '2022-04-08T13:55:32Z', 'level': 'warn', 'message': '{Some log message without number}'}";
    }
}

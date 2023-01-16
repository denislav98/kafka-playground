package com.kafka.biginer.kafka.tutorial.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);

    private static final String BOOTSTRAP_SERVERS = "c02f735jmd6r.dhcp.sofl.sap.corp:9092";
    private static final String GROUP_ID = "my-fourth-application";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        ConsumerThread consumerRunnable =
                new ConsumerThread(BOOTSTRAP_SERVERS, GROUP_ID, TOPIC, latch);

        // start the thread
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    LOGGER.info("Caught shut down hook");
                    consumerRunnable.shutDown();
                })
        );

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Application interrupted", e);
        }
        finally {
            LOGGER.info("Closing applicaition...");
        }
    }
}

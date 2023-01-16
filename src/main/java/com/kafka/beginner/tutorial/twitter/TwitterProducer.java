/*
package com.kafka.beginner.tutorial.twitter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);

    private static final String CONSUMER_KEY = "CjYyVPhttFLFSjgeaAp2RwtAJ";
    private static final String CONSUMER_SECRET = "gfDNjWRFlnD9nefDqWDrbYGmBpDvxGjwL4eNV2Gw04snkHzbHF";
    private static final String ACCESS_TOKEN = "1345411213283487751-LS6LVEajOT5YtIqNRmwj5es7v9r4H7";
    private static final String ACCESS_TOKEN_SECRET = "5pTTsUTMQfstAy4yPVPW02x20orFkTgfHWLnBjLQ9UC9d";

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        LOGGER.info("Setup");

        // create a twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(500);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                client.stop();
                e.printStackTrace();
            }

            if (msg != null) {
                LOGGER.info(msg);
            }
        }

        LOGGER.info("End of an application");

        // create a kafka producer
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        */
/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) *//*

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN,
                ACCESS_TOKEN_SECRET);

        return new ClientBuilder()
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue)).build();
    }
}
*/

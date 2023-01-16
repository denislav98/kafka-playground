package kafka.elasticsearch.consumer;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.Properties;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ElasticSearchConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    private static final String BOOTSTRAP_SERVERS = "192.168.1.104:9092";
    private static final String GROUP_ID = "kafka-demo-elasticsearch";
    private static final String TOPIC = "twitter_tweets";

    private static final String HOSTNAME = "sap-search-1550151132.us-east-1.bonsaisearch.net";
    private static final String USERNAME = "lnr9kngww";
    private static final String PASSWORD = "yykl65xez";

    public static void main(String[] args) throws IOException, InterruptedException {
        ElasticsearchClient client = ElasticSearchConsumer.createClient();
        try (KafkaConsumer<String, String> consumer = createKafkaConsumer(TOPIC)) {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                LOGGER.info("Received: " + records.count() + " records");
                for (ConsumerRecord<String, String> record : records) {
                    String indexId = record.topic() + "_" + record.partition() + "_" + record
                            .partition();
                    String jsonString = record.value();
                    Reader input = new StringReader(jsonString.replace('\'', '"'));
                    IndexRequest<Object> indexRequest = new IndexRequest.Builder<>()
                            .index("twitter")
                            .type("tweets")
                           // .id(indexId)
                            .withJson(input)
                            .build();
                    IndexResponse indexResponse = client.index(indexRequest);
                    LOGGER.info(indexResponse.id());
                    Thread.sleep(10);

                    LOGGER.info("Commiting offsets...");
                    consumer.commitSync();
                    LOGGER.info("Offsets have been committed.");
                    Thread.sleep(1000);
                }
            }
        }
    }

    public static ElasticsearchClient createClient() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(USERNAME, PASSWORD));

        // Create the low-level client
        RestClient restClient = RestClient.builder(new HttpHost(HOSTNAME, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider))
                .setDefaultHeaders(new Header[] {
                        new BasicHeader("Content-type", "application/json")
                })
                .build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(restClient,
                new JacksonJsonpMapper());

        return new ElasticsearchClient(transport);
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(MAX_POLL_RECORDS_CONFIG, "5");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(singleton(topic));

        return consumer;
    }
}

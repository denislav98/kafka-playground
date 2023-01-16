package kafka.streams;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

import java.util.Properties;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class StreamsFilterTweets {

    private static final String TOPIC = "twitter_tweets";
    private static final String FILTERED_TOPIC = "filter_tweets";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "192.168.1.103:9092");
        properties.setProperty(APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
        properties.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream(TOPIC);
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonMsg) -> extractUserFollowersFromTweets(jsonMsg).contains("message-")
        );
        filteredStream.to(FILTERED_TOPIC);

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();
    }

    private static String extractUserFollowersFromTweets(String jsonString) {
        try {
            return JsonParser.parseString(jsonString).getAsJsonObject().get("message")
                    .getAsString();
        } catch (NullPointerException e) {
            return "without msg";
        }
    }
}

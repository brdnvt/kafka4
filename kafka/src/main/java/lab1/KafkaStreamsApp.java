package lab1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;

import java.util.Properties;
import java.util.Map;

public class KafkaStreamsApp {
    private static final String INPUT_TOPIC = "coffee_products";
    private static final String NO_MILK_TOPIC = "no_milk_drinks";
    private static final String COCONUT_MILK_TOPIC = "coconut_milk_drinks";
    private static final String OTHER_MILK_TOPIC = "other_milk_drinks";
    private static final String HIGH_CALORIE_COUNT_TOPIC = "high_calorie_count";
    private static final String NO_MILK_CALORIES_SUM_TOPIC = "no_milk_calories_sum";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "starbucks-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        StreamsBuilder builder = new StreamsBuilder();
        Gson gson = new Gson();

        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);        
        Map<String, KStream<String, String>> branches = inputStream
            .filter((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("calories").getAsInt() > 200;
            })
            .split(Named.as("milk-type-"))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("milk").getAsInt() == 0;
            }, Branched.as("no-milk"))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("milk").getAsInt() == 5;
            }, Branched.as("coconut-milk"))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                int milk = json.get("milk").getAsInt();
                return milk != 0 && milk != 5;
            }, Branched.as("other-milk"))
            .noDefaultBranch();        
        branches.get("milk-type-no-milk").to(NO_MILK_TOPIC);
        branches.get("milk-type-coconut-milk").to(COCONUT_MILK_TOPIC);
        branches.get("milk-type-other-milk").to(OTHER_MILK_TOPIC);

        inputStream
            .filter((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("calories").getAsInt() > 200;
            })
            .groupBy((key, value) -> "total")
            .count(Materialized.as("high-calorie-count-store"))
            .toStream()
            .mapValues(Object::toString)
            .to(HIGH_CALORIE_COUNT_TOPIC);        
        branches.get("milk-type-no-milk")
            .groupBy((key, value) -> "total")
            .aggregate(
                () -> 0,
                (key, value, aggregate) -> {
                    JsonObject json = gson.fromJson(value, JsonObject.class);
                    return aggregate + json.get("calories").getAsInt();
                },
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("no-milk-calories-sum-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer())
            )
            .toStream()
            .mapValues(Object::toString)
            .to(NO_MILK_CALORIES_SUM_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

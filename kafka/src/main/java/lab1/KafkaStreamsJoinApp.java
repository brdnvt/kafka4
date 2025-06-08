package lab1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaStreamsJoinApp {
    private static final String DRINKS_TOPIC = "drinks-info";
    private static final String NUTRITION_TOPIC = "nutrition-info";
    private static final String HIGH_CALORIE_TOPIC = "high-calorie-drinks";
    private static final String LOW_CALORIE_TOPIC = "low-calorie-drinks";
    private static final String JOINED_TOPIC = "complete-drinks-info";    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "drinks-streams-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760);
        createTopicsIfNotExists("localhost:9092",
            DRINKS_TOPIC,
            NUTRITION_TOPIC,
            HIGH_CALORIE_TOPIC,
            LOW_CALORIE_TOPIC,
            JOINED_TOPIC
        );

        StreamsBuilder builder = new StreamsBuilder();
        Gson gson = new Gson();

        KStream<String, String> drinksStream = builder.stream(DRINKS_TOPIC);
        KStream<String, String> nutritionStream = builder.stream(NUTRITION_TOPIC);        
        KStream<String, String> highCalorieStream = drinksStream.filter((key, value) -> {
            JsonObject json = gson.fromJson(value, JsonObject.class);
            return json.get("calories").getAsInt() >= 200;
        });
        
        KStream<String, String> lowCalorieStream = drinksStream.filter((key, value) -> {
            JsonObject json = gson.fromJson(value, JsonObject.class);
            return json.get("calories").getAsInt() < 200;
        });
        
        highCalorieStream.to(HIGH_CALORIE_TOPIC);
        lowCalorieStream.to(LOW_CALORIE_TOPIC);        KStream<String, String> joinedStream = drinksStream.join(
            nutritionStream,
            (drinkInfo, nutritionInfo) -> {
                JsonObject drink = gson.fromJson(drinkInfo, JsonObject.class);
                JsonObject nutrition = gson.fromJson(nutritionInfo, JsonObject.class);
                
                String productName = drink.get("product_name").getAsString();
                drink.addProperty("join_id", productName + "_" + System.currentTimeMillis());
                
                for (String key : nutrition.keySet()) {
                    if (!key.equals("product_name")) {  
                        drink.add(key, nutrition.get(key));
                    }
                }
                return drink.toString();
            },
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),  
            StreamJoined.with(
                Serdes.String(),
                Serdes.String(), 
                Serdes.String()
            )        );        
        KStream<String, String> deduplicatedStream = joinedStream
            .selectKey((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("product_name").getAsString();
            })
            .groupByKey()
            .reduce((value1, value2) -> {
                try {
                    JsonObject json1 = gson.fromJson(value1, JsonObject.class);
                    JsonObject json2 = gson.fromJson(value2, JsonObject.class);
                    
                    if (json1.has("join_id") && json2.has("join_id")) {
                        String joinId1 = json1.get("join_id").getAsString();
                        String joinId2 = json2.get("join_id").getAsString();
                        
                        long timestamp1 = Long.parseLong(joinId1.split("_")[1]);
                        long timestamp2 = Long.parseLong(joinId2.split("_")[1]);
                        
                        return timestamp1 > timestamp2 ? value1 : value2;
                    }
                } catch (Exception e) {
                    System.err.println("Error during deduplication: " + e.getMessage());
                }
                return value1; 
            })
            .toStream();

        deduplicatedStream.to(JOINED_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void createTopicsIfNotExists(String bootstrapServers, String... topics) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        try (Admin admin = Admin.create(props)) {
            List<NewTopic> newTopics = new ArrayList<>();
            
            Set<String> existingTopics = admin.listTopics().names().get();
            
            for (String topic : topics) {
                if (!existingTopics.contains(topic)) {
                    NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                    newTopics.add(newTopic);
                }
            }
            
            if (!newTopics.isEmpty()) {
                CreateTopicsResult result = admin.createTopics(newTopics);
                result.all().get();
                System.out.println("Created topics: " + String.join(", ", topics));
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Error creating Kafka topics", e);
        }
    }
}

package lab1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;

import java.util.Properties;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class KafkaStreamsApp {
    private static final String INPUT_TOPIC = "coffee_products";
    private static final String NO_MILK_TOPIC = "no_milk_drinks";
    private static final String COCONUT_MILK_TOPIC = "coconut_milk_drinks";
    private static final String OTHER_MILK_TOPIC = "other_milk_drinks";
    private static final String HIGH_CALORIE_COUNT_TOPIC = "high_calorie_count";
    private static final String NO_MILK_CALORIES_SUM_TOPIC = "no_milk_calories_sum";    public static void main(String[] args) {
        Properties props = new Properties();        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "starbucks-streams-app-v4");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
          // Налаштування для правильної агрегації
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000); // Збільшуємо інтервал
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760); // 10MB кеш для агрегації

        // Створюємо топіки перед запуском стрімів
        createTopicsIfNotExist("localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        Gson gson = new Gson();        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);
        
        // Створюємо окремі потоки для різних типів молока (без фільтрації за калоріями)
        Map<String, KStream<String, String>> allMilkBranches = inputStream
            .split(Named.as("all-milk-type-"))
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

        // Фільтруємо висококалорійні напої з основного потоку
        KStream<String, String> highCalorieStream = inputStream
            .filter((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("calories").getAsInt() > 200;
            });

        // Розділяємо висококалорійні напої за типом молока
        Map<String, KStream<String, String>> highCalorieBranches = highCalorieStream
            .split(Named.as("high-calorie-milk-type-"))
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
        
        // Виводимо висококалорійні напої в відповідні топіки
        highCalorieBranches.get("high-calorie-milk-type-no-milk").to(NO_MILK_TOPIC);
        highCalorieBranches.get("high-calorie-milk-type-coconut-milk").to(COCONUT_MILK_TOPIC);
        highCalorieBranches.get("high-calorie-milk-type-other-milk").to(OTHER_MILK_TOPIC);        // Агрегатор 1: Підрахунок кількості висококалорійних напоїв
        highCalorieStream
            .groupBy((key, value) -> "total")
            .count(Materialized.as("high-calorie-count-store"))
            .toStream()
            .mapValues(Object::toString)
            .to(HIGH_CALORIE_COUNT_TOPIC);

        // Агрегатор 2: Сума калорій у ВСІХ напоях без молока (не тільки висококалорійних)
        allMilkBranches.get("all-milk-type-no-milk")
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
    
    private static void createTopicsIfNotExist(String bootstrapServers) {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrapServers);
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            CreateTopicsResult result = adminClient.createTopics(Arrays.asList(
                new NewTopic(INPUT_TOPIC, 1, (short) 1),
                new NewTopic(NO_MILK_TOPIC, 1, (short) 1),
                new NewTopic(COCONUT_MILK_TOPIC, 1, (short) 1),
                new NewTopic(OTHER_MILK_TOPIC, 1, (short) 1),
                new NewTopic(HIGH_CALORIE_COUNT_TOPIC, 1, (short) 1),
                new NewTopic(NO_MILK_CALORIES_SUM_TOPIC, 1, (short) 1)
            ));
            
            // Очікуємо завершення створення топіків
            result.all().get();
            System.out.println("Топіки створено або вже існують");
        } catch (ExecutionException e) {
            if (e.getCause().getMessage().contains("already exists")) {
                System.out.println("Топіки вже існують");
            } else {
                System.err.println("Помилка при створенні топіків: " + e.getMessage());
            }
        } catch (InterruptedException e) {
            System.err.println("Перервано при створенні топіків: " + e.getMessage());
        }
    }
}

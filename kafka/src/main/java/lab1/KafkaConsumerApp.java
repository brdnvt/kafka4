package lab1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.Collections;

public class KafkaConsumerApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "starbucks_group");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(java.util.Arrays.asList("drinks-info", "nutrition-info"));

        while (true) {
            consumer.poll(Duration.ofMillis(1000)).forEach(record -> {
                System.out.println("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value());
            });
        }
    }
}

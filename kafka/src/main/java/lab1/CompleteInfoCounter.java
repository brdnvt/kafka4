package lab1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CompleteInfoCounter {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "complete-info-counter");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList("complete-drinks-info"));
            
            int totalRecords = 0;
            int emptyPolls = 0;
            
            System.out.println("Підрахунок записів у топіку complete-drinks-info...");
            
            while (emptyPolls < 5) { 
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    emptyPolls++;
                    System.out.println("Пусте опитування " + emptyPolls + "/5");
                } else {
                    emptyPolls = 0;
                    totalRecords += records.count();
                    System.out.println("Знайдено " + records.count() + " записів. Загалом: " + totalRecords);
                    
                    if (totalRecords <= 5) {
                        for (ConsumerRecord<String, String> record : records) {
                            System.out.println("Запис " + totalRecords + ": " + record.value());
                            if (totalRecords >= 5) break;
                        }
                    }
                }
            }
            
            System.out.println("\n=== РЕЗУЛЬТАТ ===");
            System.out.println("Загальна кількість записів у complete-drinks-info: " + totalRecords);
        }
    }
}

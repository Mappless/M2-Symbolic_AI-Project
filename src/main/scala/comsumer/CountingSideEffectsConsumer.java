package comsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;


public class CountingSideEffectsConsumer {
    KafkaConsumer<String, String> consumer;
    String topic = "CountingSideEffects";
    HashMap<String, Integer> hashMap = new HashMap<>(); // Key : Side Effect Code, Value : count

    public CountingSideEffectsConsumer(Properties props) {
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    private void treatement(Person person) {
        if (hashMap.get(person.getSideEffectCode()) == null) {
            hashMap.put(person.getSideEffectCode(), 1);
        }
        hashMap.put(person.getSideEffectCode(), hashMap.get(person.getSideEffectCode() + 1));
    }

    private void displayCount() {
        hashMap.forEach((k, v) -> {
            System.out.println("For the Side effect code " + k + ", we have " + v + "case.");
        });
    }

    public void run() {
        while (true) {
            ObjectMapper mapper = new ObjectMapper();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    Person person = mapper.readValue(record.value(), Person.class);
                    treatement(person);
                    displayCount();
                } catch (JsonProcessingException e) {
                    System.err.println("Error read JSON file.");
                }
            }
        }
    }



    public static void main(String[] args) {
        System.out.println("-------------- Welcome to the Counting Side Effects Consumer --------------");

        Properties props = new Properties();
        props.put("group.id", "CountingSideEffects"); // Group consumer
        props.put("bootstrap.servers", "localhost:9092"); //Server
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        CountingSideEffectsConsumer consumer = new CountingSideEffectsConsumer(props);
        consumer.run();
    }
}

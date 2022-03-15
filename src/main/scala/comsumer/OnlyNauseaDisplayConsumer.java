package comsumer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OnlyNauseaDisplayConsumer {
    KafkaConsumer<String, String> consumer;
    String topic = "OnlyNauseaDisplay";

    public OnlyNauseaDisplayConsumer(Properties props) {
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    public void run() {
        ObjectMapper mapper = new ObjectMapper();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    Person person = mapper.readValue(record.value(), Person.class);
                    if (person.haveSideEffect("C0027497")) {
                        System.out.println("We have a Nausea Side Effect for the ID person : " + person.getId());
                    }
                } catch (JsonProcessingException e) {
                    System.err.println("Error read JSON file.");
                }
            }
        }
    }



    public static void main(String[] args) {
        System.out.println("-------------- Welcome to the Only Nausea Display Consumer --------------");

        Properties props = new Properties();
        props.put("group.id", "NauseaDisplay"); // Group consumer
        props.put("bootstrap.servers", "localhost:9092"); //Server
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        OnlyNauseaDisplayConsumer consumer = new OnlyNauseaDisplayConsumer(props);
        consumer.run();
    }
}

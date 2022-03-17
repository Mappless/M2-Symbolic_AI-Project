package comsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jena.base.Sys;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

//TODO : Faire un groupe de comsumer où un consumer va lire une partition

public class CountingSideEffectsWithPartitionConsumer {
    String topic = "CountingSideEffectsWithPartition";
    List<KafkaConsumer<String, String>> listKafkaConsumer = new ArrayList();
    HashMap<String, Integer> hashMap = new HashMap<>(); // Key : Side Effect Code, Value : count

    public CountingSideEffectsWithPartitionConsumer(Properties props) {
        for (int i = 0; i < 13; i++) {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            TopicPartition topicPartition = new TopicPartition(topic, i);
            consumer.assign(Arrays.asList(topicPartition));
            listKafkaConsumer.add(consumer);
        }
    }

    private void treatement(Person person) {
        synchronized (this) {
            if (hashMap.get(person.getSideEffectCode()) == null) {
                hashMap.put(person.getSideEffectCode(), 1);
            } else {
                hashMap.put(person.getSideEffectCode(), hashMap.get(person.getSideEffectCode()) + 1);
            }
        }
    }

    private void displayCount() {
        synchronized (this) {
            System.out.println("+--------------------------------------------------------+");
            hashMap.forEach((k, v) -> {
                System.out.println("For the Side effect code " + k + ", we have " + v + " case.");
        });
            System.out.println("+--------------------------------------------------------+");
        }
    }

    public void run() {
        for (KafkaConsumer<String, String> consumer : listKafkaConsumer) {
            new Thread(() -> {
                while (true) {
                    ObjectMapper mapper = new ObjectMapper();
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            synchronized (this) {
                                for (TopicPartition tp : consumer.assignment()) {
                                    System.out.println("Read in Partition : " + tp.partition() + ", from topic : " + tp.topic());
                                }
                                Person person = mapper.readValue(record.value(), Person.class);
                                treatement(person);
                                displayCount();
                            }
                        } catch (JsonProcessingException e) {
                            System.err.println("Error read JSON file.");
                        }
                    }
                }
            }).start();
        }
    }



    public static void main(String[] args) {
        System.out.println("-------------- Welcome to the Counting Side Effects With Partition Consumer --------------");

        Properties props = new Properties();
        props.put("group.id", "CountingSideEffectsWithPartition"); // Group consumer
        props.put("bootstrap.servers", "localhost:9092"); //Server
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        CountingSideEffectsWithPartitionConsumer consumer = new CountingSideEffectsWithPartitionConsumer(props);
        consumer.run();
    }
}

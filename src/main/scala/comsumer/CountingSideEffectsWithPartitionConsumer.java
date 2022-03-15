package comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

//TODO : Faire un groupe de comsumer où un consumer va lire une partition

public class CountingSideEffectsWithPartitionConsumer {
    KafkaConsumer<String, String> consumer;
    String topic = "CountingSideEffectsWithPartition";
    HashMap<Integer, Integer> hashMap = new HashMap<>(); // Key : Partition Number, Value : count

    private int partitionNumber;

    public CountingSideEffectsWithPartitionConsumer(Properties props) {
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    private void treatement(int partitionID) {
        if (hashMap.get(partitionID) == null) {
            hashMap.put(partitionID, 1);
        }
        hashMap.put(partitionID, hashMap.get(partitionID + 1));
    }

    private void displayCount() {
        hashMap.forEach((k, v) -> {
            System.out.println("For the partition number " + k + ", we have " + v + "case.");
        });
    }

    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    treatement(partition.partition());
                    displayCount();
                }
            }
        }
    }



    public static void main(String[] args) {
        System.out.println("-------------- Welcome to the Counting Side Effects With Partition Consumer --------------");

        Properties props = new Properties();
        props.put("group.id", "CountingSideEffectsWithPartition"); // Group consumer
        props.put("bootstrap.servers", "localhost:9092"); //Server
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        OnlyNauseaDisplayConsumer consumer = new OnlyNauseaDisplayConsumer(props);
        consumer.run();
    }
}

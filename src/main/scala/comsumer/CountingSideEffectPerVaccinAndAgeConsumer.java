package comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//TODO : Faire un groupe de comsumer où un consumer va lire une partition
public class CountingSideEffectPerVaccinAndAgeConsumer {
    KafkaConsumer<String, String> consumer;
    String topic = "CountingSideEffectPerVaccinAndAge";

    public CountingSideEffectPerVaccinAndAgeConsumer(Properties props) {
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                //treatement
            }
        }
    }



    public static void main(String[] args) {
        System.out.println("-------------- Welcome to the Counting Side Effect Per Vaccin And Age Consumer --------------");

        Properties props = new Properties();
        props.put("group.id", "CountingSideEffectPerVaccinAndAge"); // Group consumer
        props.put("bootstrap.servers", "localhost:9092"); //Server
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        CountingSideEffectPerVaccinAndAgeConsumer consumer = new CountingSideEffectPerVaccinAndAgeConsumer(props);
        consumer.run();
    }
}

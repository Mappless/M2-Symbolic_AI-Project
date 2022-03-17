package comsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import scala.Tuple2;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;

public class FakerPersonPartitionProducer {
    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        Faker faker = new Faker();
        SideEffect sideEffect = new SideEffect();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); //Server
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 1000; i++) {
            Thread.sleep(100);
            Person person = new Person();
            int partitionNumber;
            person.setId(i);
            Tuple2<String, String> tuple2 = sideEffect.getRandomSideEffect(0.5f);
            if (tuple2 == null) {
                continue;
            }
            person.setGetSideEffectCode(tuple2._2());
            person.setSideEffectName(tuple2._1());
            switch (tuple2._2()) {
                case "C0151828":
                    partitionNumber = 0;
                    break;
                case "C0015672":
                    partitionNumber = 1;
                    break;
                case "C0018681":
                    partitionNumber = 2;
                    break;
                case "C0231528":
                    partitionNumber = 3;
                    break;
                case "C0085593":
                    partitionNumber = 4;
                    break;
                case "C0003862":
                    partitionNumber = 5;
                    break;
                case "C0015967":
                    partitionNumber = 6;
                    break;
                case "C0151605":
                    partitionNumber = 7;
                    break;
                case "C0852625":
                    partitionNumber = 8;
                    break;
                case "C0027497":
                    partitionNumber = 9;
                    break;
                case "C0231218":
                    partitionNumber = 10;
                    break;
                case "C0497156":
                    partitionNumber = 11;
                    break;
                case "C0863083":
                    partitionNumber = 12;
                    break;
                default :
                    continue;
            }
            System.out.println(person.toJson() + " -> " + partitionNumber);
            Future<RecordMetadata> topicPerson = producer.send(
                    new ProducerRecord<String, String>("CountingSideEffectsWithPartition", partitionNumber, null, person.toJson()));
        }
        producer.close();
    }
}

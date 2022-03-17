package comsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import scala.Tuple2;

import java.util.Properties;
import java.util.concurrent.Future;

public class FakerPersonProducer {

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
            person.setId(i);
            Tuple2<String, String> tuple2 = sideEffect.getRandomSideEffect(0.5f);
            if (tuple2 == null) {
                continue;
            }
            person.setGetSideEffectCode(tuple2._2());
            person.setSideEffectName(tuple2._1());
            System.out.println(person.toJson());
            Future<RecordMetadata> topicPerson = producer.send(new ProducerRecord<String, String>("CountingSideEffects", person.toJson()));

        }
        producer.close();
    }
}


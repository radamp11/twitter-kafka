package bec.kafka.course.kafka01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {

        // steps to build producer

        // 1 create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        // 2 create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3 create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("first_topic", "hello world!");

        // 4 send data - send method is async!
        producer.send(producerRecord);

        // need to flush data because data wont be sent before the apps closes
        producer.flush();
        producer.close();
    }
}

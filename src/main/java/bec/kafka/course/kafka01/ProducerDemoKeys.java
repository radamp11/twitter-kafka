package bec.kafka.course.kafka01;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        // steps to build producer

        // 1 create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        // 2 create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i = 0; i < 10; i++){
            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // 3 create producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);

            logger.info("key: " + key);
            // 4 send data - send method is async!
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everytime a record is successfully sent or exceptions instead
                    if(e == null){
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else{
                        logger.info("Error while producing.", e);
                    }
                }
            }).get();
        }

        // need to flush data because data wont be sent before the apps closes
        producer.flush();
        producer.close();
    }
}

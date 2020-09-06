package kafka.beginner1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {

        // create Producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create the ProducerRecord
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "hello baby !");

        // send data
        producer.send(producerRecord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record successfully or an exception is thrown
                if (e == null) {
                    // the record was successfully send
                    logger.info("Received new metadate \n" + "topic: " + recordMetadata.topic() +
                            "\n Partition: " + recordMetadata.partition() +
                            "\n Offset: " + recordMetadata.offset() +
                            "\n Timestamp: " + recordMetadata.timestamp());
                } else {
                    e.printStackTrace();
                }
            }
        });

        // flush data
        producer.flush();

        // close
        producer.close();
    }
}

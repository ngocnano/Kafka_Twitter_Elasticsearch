package kafka.beginner1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();


    }

    private void run(){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "test5";
        String topic = "first_topic";

        // latch to dealing with mt
        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerThread = new ConsumerThread(latch, topic, bootstrapServers, groupId);

        // start thread
        Thread myThread = new Thread(consumerThread);
        myThread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("shutdown hook");
            ((ConsumerThread) consumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                logger.info("app is exit");
            }

        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("app inter", e);
        } finally {
            logger.info("app is closing");
        }
    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch countDownLatch;
        private KafkaConsumer<String,String> consumer;
        private String topic;
        private String bootstrapServers;
        private String groupId;
        Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

        public ConsumerThread(CountDownLatch countDownLatch,
                              String topic,
                              String bootstrapServers,
                              String groupId){
            this.countDownLatch = countDownLatch;
            this.topic = topic;
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
        }

        @Override
        public void run() {

            // create properties
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // subscriber consumer to our topic
            consumer.subscribe(Arrays.asList(topic));

            try {

                // poll data
                while (true) {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + " Value: " + record.value() +
                                "\n Partition + " + record.partition() +
                                "\n Offset + " + record.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("received shutdown signal");
            } finally {
                consumer.close();
                // tell your main code we're done with consumer
                countDownLatch.countDown();
            }


        }

        public void shutdown(){
            // throw wakeup exception
            consumer.wakeup();
        }
    }
}

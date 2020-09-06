package com.github.ngoctm.kafka.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    final static Logger logger = LoggerFactory.getLogger(RestHighLevelClient.class);

    public static RestHighLevelClient createClient(){

        // Access (https://did4vwib52:9zmtb22z2z@kafka-course-2342913569.ap-southeast-2.bonsaisearch.net:443)
        String hostname = "kafka-course-2342913569.ap-southeast-2.bonsaisearch.net";
        String username = "did4vwib52";
        String password = "9zmtb22z2z";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
        return client;
    }

    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "tw_app";

        // create properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = ElasticSearchConsumer.createClient();
        KafkaConsumer<String,String> consumer = createConsumer("twitter");
        BulkRequest bulkRequest = new BulkRequest();
        int recordCount = 0;
        // poll data
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            recordCount = records.count();
            logger.info("Received " + recordCount + "records");
            for(ConsumerRecord<String, String> record : records){
                String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                IndexRequest indexRequest = new IndexRequest("twitter").id(id);
                indexRequest.source(record.value(),XContentType.JSON);
                logger.info("Id " + id);
                bulkRequest.add(indexRequest);
                /*  IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info("ID   " + indexResponse.getId());
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    client.close();
                } */

            }
            if(recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing Offsets........");
                consumer.commitSync();
                logger.info("Offset has been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //client.close();
    }
}

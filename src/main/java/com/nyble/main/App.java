package com.nyble.main;

import com.nyble.facades.kafkaConsumer.KafkaConsumerFacade;
import com.nyble.managers.ProducerManager;
import com.nyble.topics.Names;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.*;

@SpringBootApplication(scanBasePackages = {"com.nyble.rest"})
public class App {

    final static String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "10.100.1.17:9093";
    final static Logger logger = LoggerFactory.getLogger(App.class);
    public final static String CONSUMER_UNIQUE_CRITERIA_TABLE = "consumers_unique_entity_criterias";

    final static Properties consumerProperties = new Properties();
    final static Properties producerProperties = new Properties();
    final static String groupId = "consumer-matcher";
    static ProducerManager producerManager;

    public static void main(String[] args) {
        try{
            SpringApplication.run(App.class, args);

            init();
            final int noOfConsumers = 4;
            KafkaConsumerFacade<String, String> attributesConsumerFacade = new KafkaConsumerFacade<>(consumerProperties,
                    noOfConsumers, KafkaConsumerFacade.PROCESSING_TYPE_SINGLE);
            attributesConsumerFacade.subscribe(Collections.singletonList(Names.CONSUMERS_TOPIC));
            attributesConsumerFacade.startPolling(Duration.ofSeconds(10), RecordProcessorImpl.class);

        }catch(Exception e){
            logger.error(e.getMessage(), e);
            logger.error("EXITING");
            System.exit(1);
        }
    }

    private static void init(){
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, 5);
        producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        producerManager = ProducerManager.getInstance(producerProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            producerManager.getProducer().flush();
            producerManager.getProducer().close();
        }));
    }
}

package com.nyble.main;

import com.google.gson.Gson;
import com.nyble.managers.ProducerManager;
import com.nyble.match.Matcher;
import com.nyble.match.SystemConsumerEntity;
import com.nyble.models.consumer.Consumer;
import com.nyble.models.consumer.ConsumerFlag;
import com.nyble.topics.Names;
import com.nyble.topics.consumer.ConsumerValue;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class App {

    final static String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "10.100.1.17:9093";
    final static Logger logger = LoggerFactory.getLogger(App.class);

    final static Properties consumerProperties = new Properties();
    final static Properties producerProperties = new Properties();
    final static String groupId = "consumer-matcher";
    static ProducerManager producerManager;
    static{
        consumerProperties.put("bootstrap.servers", KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("group.id", groupId);
        consumerProperties.put("max.poll.records", 500);
        consumerProperties.put("max.poll.interval.ms", 1000*500);
        consumerProperties.put("session.timeout.ms", 1000*250);
        consumerProperties.put("heartbeat.interval.ms", 1000*125);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        producerProperties.put("bootstrap.servers", KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 5);
        producerProperties.put("batch.size", 16384);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());
        producerManager = ProducerManager.getInstance(producerProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            producerManager.getProducer().flush();
            producerManager.getProducer().close();
        }));
    }

    public static void main(String[] args) {

        final int noOfConsumers = 4;
        for(int i=0;i<noOfConsumers; i++){
            new Thread(()->{
                KafkaConsumer<String, String> consumerMatcher = new KafkaConsumer<>(consumerProperties);
                consumerMatcher.subscribe(Collections.singleton(Names.CONSUMERS_TOPIC));
                while(true){
                    ConsumerRecords<String, String> records = consumerMatcher.poll(1000);
                    Set<TopicPartition> partitions = records.partitions();
                    String partitionsName = partitions.stream()
                            .map(part->part.topic()+"#"+part.partition())
                            .collect(Collectors.joining(";"));
                    if(records.count() > 0){
                        logger.debug("Found {} new records on partition {}", records.count(), partitionsName);
                        try{
                            records.forEach(App::processRecord);
                        }catch(Exception e){
                            logger.error(e.getMessage(), e);
                            break;
                        }
                        logger.debug("End processing poll");
                    }
                }
            }).start();
        }


    }

    public static void processRecord(ConsumerRecord<String, String> record) {
        Gson gson = new Gson();
        ConsumerValue value = gson.fromJson(record.value(), ConsumerValue.class);
        String changedProperty = value.getChangedProperty().getPropertyName();
        if("fullName".equals(changedProperty) || "phone".equals(changedProperty) ||
                "email".equals(changedProperty) || "location".equals(changedProperty) ||
                ("flags".equalsIgnoreCase(changedProperty) &&
                        validationFlagsChanged(
                                value.getChangedProperty().getNewValue(),
                                value.getChangedProperty().getOldValue(),
                                value.getConsumer())
                )
        ){
            int consumerId = Integer.parseInt(value.getConsumer().getValue("consumerId"));
            int systemId = value.getConsumer().hasProperty("systemId")?
                    Integer.parseInt(value.getConsumer().getValue("systemId")) :
                    Integer.parseInt(value.getConsumer().getValue("sourceSystem"));
            Map<String, Object> extraInfoMap = new HashMap<>();
            extraInfoMap.put("consumer", value);
            try {
                logger.debug("Get consumers entity");
                List<SystemConsumerEntity> consumersEntities = Matcher.getConsumersEntityId(consumerId, systemId, extraInfoMap);
                KafkaProducer<String, String> producerConsumerAttributeTopic = producerManager
                        .getProducer();
                String currentTimestamp = new Date().getTime()+"";
                for(SystemConsumerEntity sce : consumersEntities){
                    if(sce.needToUpdate){
                        ConsumerAttributesKey attributeKey = new ConsumerAttributesKey(sce.systemId, sce.consumerId);
                        ConsumerAttributesValue attributeValue = new ConsumerAttributesValue(sce.systemId+"",
                                sce.consumerId+"",
                                "entityId", sce.entityId+"", currentTimestamp, currentTimestamp);
                        ProducerRecord<String, String> consumerAttributeMessage = new ProducerRecord<>(Names.CONSUMER_ATTRIBUTES,
                                gson.toJson(attributeKey), gson.toJson(attributeValue));
                        producerConsumerAttributeTopic.send(consumerAttributeMessage);
                    }
                }

            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private static boolean validationFlagsChanged(String newValue, String oldValue, Consumer consumer) {

        String name = consumer.getValue("fullName");
        String phone = consumer.getValue("phone");

        if(name!=null  && !name.isEmpty() && phone!=null && !phone.isEmpty()){
//            if(consumer.isFlagSet(ConsumerFlag.IS_EMAIL_VALID) || consumer.isFlagSet(ConsumerFlag.IS_PHONE_VALID)){
                if(oldValue == null || oldValue.isEmpty()){
                    return true;
                }
                int oldMask = Integer.parseInt(oldValue);
                return !oldValue.equals(newValue) && (
                        (oldMask & (1 << ConsumerFlag.IS_EMAIL_VALID.getBitPosition()) ) == 0 ||
                                (oldMask & (1 << ConsumerFlag.IS_PHONE_VALID.getBitPosition()) )  == 0
                );
//            }else{
//                return false;
//            }
        }else{
            //if it doesn't have name and phone, don't take it into account
            return false;
        }


    }
}

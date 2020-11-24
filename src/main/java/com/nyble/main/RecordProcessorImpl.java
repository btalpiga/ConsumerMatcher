package com.nyble.main;

import com.nyble.facades.kafkaConsumer.RecordProcessor;
import com.nyble.match.Matcher;
import com.nyble.match.SystemConsumerEntity;
import com.nyble.models.consumer.Consumer;
import com.nyble.models.consumer.ConsumerFlag;
import com.nyble.topics.Names;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumer.ConsumerValue;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import com.nyble.utils.StringConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RecordProcessorImpl implements RecordProcessor<String, String> {

    private final static Logger logger = LoggerFactory.getLogger(RecordProcessorImpl.class);
    private final static Set<String> checkAttributes = new HashSet<>(Arrays.asList("fullName", "phone", "email", "location", "flags"));

    private Matcher matcher = new Matcher();
    public void setMatcher(Matcher m){
        matcher = m;
    }

    @Override
    public boolean process(ConsumerRecord<String, String> consumerRecord) {
        ConsumerValue consumerValue = (ConsumerValue) TopicObjectsFactory.fromJson(consumerRecord.value(), ConsumerValue.class);
        return processConsumerValue(consumerValue);
    }

    @Override
    public boolean processBatch(ConsumerRecords<String, String> consumerRecords) {
        throw new UnsupportedOperationException("This method is unimplemented and should remain so!");
    }

    public boolean processConsumerValue(ConsumerValue consumerValue){
        Consumer consumer = consumerValue.getConsumer();
        if(consumerValue.getChangedProperty() == null){
            logger.warn("ConsumerValue consumer_id = {} system_id = {} does not have 'changedProperty'",
                    consumer.getValue("consumerId"), consumer.getValue("systemId"));
            return true;
        }
        String changedProperty = new StringConverter(consumerValue.getChangedProperty().getPropertyName())
                .nullIf("").get();
        if(changedProperty!=null && (
                (!Objects.equals(changedProperty, "flags") && checkAttributes.contains(changedProperty)) ||
                ( Objects.equals(changedProperty, "flags") && validationFlagsChanged(
                        consumerValue.getChangedProperty().getNewValue(),
                        consumerValue.getChangedProperty().getOldValue(),
                        consumer)
                )
        )){
            int consumerId = Integer.parseInt(consumer.getValue("consumerId"));
            int systemId = Integer.parseInt(consumer.getValue("systemId"));
            Map<String, Object> extraInfoMap = new HashMap<>();
            extraInfoMap.put("consumer", consumerValue);
            try {
                logger.debug("Get consumers entity");
                updateConsumersEntityId(matcher.getConsumersEntityId(consumerId, systemId, extraInfoMap));
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return false;
            }
        }
        return true;
    }

    /**
     *
     * @param newValue represents the new value of the 'flags' property
     * @param oldValue represents the old value of the 'flags' property
     * @param consumer represents the entire consumer
     * @return true if EMAIL_VALID or PHONE_VALID flags changed from false to true
     */
    public boolean validationFlagsChanged(String newValue, String oldValue, Consumer consumer) {

        String name = consumer.getValue("fullName");
        String phone = consumer.getValue("phone");
        if(name!=null  && !name.isEmpty() && phone!=null && !phone.isEmpty()){
            if(oldValue == null || oldValue.isEmpty()){
                return consumer.isFlagSet(ConsumerFlag.IS_EMAIL_VALID) || consumer.isFlagSet(ConsumerFlag.IS_PHONE_VALID);
            }
            int oldMask = Integer.parseInt(oldValue);
            boolean wasEmailValid = (oldMask & (1 << ConsumerFlag.IS_EMAIL_VALID.getBitPosition())) > 0;
            boolean wasPhoneValid = (oldMask & (1 << ConsumerFlag.IS_PHONE_VALID.getBitPosition())) > 0;

            return !oldValue.equals(newValue) && (
                    (!wasEmailValid && consumer.isFlagSet(ConsumerFlag.IS_EMAIL_VALID)) ||
                    (!wasPhoneValid && consumer.isFlagSet(ConsumerFlag.IS_PHONE_VALID))
            );
        }else{
            return false;
        }
    }

    public void updateConsumersEntityId(List<SystemConsumerEntity> consumersEntities){
        final KafkaProducer<String, String> producerConsumerAttributeTopic = App.producerManager.getProducer();
        final String currentTimestamp = new Date().getTime()+"";
        final String entityIdProp = "entityId";
        for(SystemConsumerEntity sce : consumersEntities){
            if(sce.needToUpdate){
                logger.info("Consumer system {} id {} changed entity to {}", sce.systemId, sce.consumerId, sce.entityId);
                ConsumerAttributesKey attributeKey = new ConsumerAttributesKey(sce.systemId, sce.consumerId);
                ConsumerAttributesValue attributeValue = new ConsumerAttributesValue(sce.systemId+"",
                        sce.consumerId+"", entityIdProp, sce.entityId+"",
                        currentTimestamp, currentTimestamp);
                ProducerRecord<String, String> consumerAttributeMessage = new ProducerRecord<>(Names.CONSUMER_ATTRIBUTES,
                        attributeKey.toJson(), attributeValue.toJson());
                producerConsumerAttributeTopic.send(consumerAttributeMessage);
            }
        }
    }
}

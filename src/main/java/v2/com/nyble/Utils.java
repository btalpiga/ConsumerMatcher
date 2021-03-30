package v2.com.nyble;

import com.nyble.managers.ProducerManager;
import com.nyble.topics.Names;
import com.nyble.topics.consumer.ConsumerKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

public class Utils {

    final static String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "10.100.1.17:9093";
    static Properties producerProperties = new Properties();
    static ProducerManager producerManager;
    static ReentrantLock lock = new ReentrantLock();

    private static void initProps(){
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, 5);
        producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public static String toSHA1(String s){
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        }
        catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return Base64.getEncoder().encodeToString(md.digest(s.getBytes()));
    }

    public static void updateConsumerAttribute(int systemId, int consumerId, String attribute, String value, String dateUpdate){
        if(producerManager == null){
            lock.lock();
            if(producerManager == null){
                initProps();
                producerManager = ProducerManager.getInstance(producerProperties);
                Runtime.getRuntime().addShutdownHook(new Thread(()->{
                    producerManager.getProducer().flush();
                    producerManager.getProducer().close();
                }));
            }
            lock.unlock();
        }
        ConsumerAttributesKey cak = new ConsumerAttributesKey(systemId, consumerId);
        ConsumerAttributesValue cav = new ConsumerAttributesValue(systemId+"", consumerId+"", attribute, value,
                dateUpdate, dateUpdate);

        ProducerRecord<String, String> consumerMessage = new ProducerRecord<>(Names.CONSUMER_ATTRIBUTES_TOPIC,
                cak.toJson(), cav.toJson());
        producerManager.getProducer().send(consumerMessage);
    }
}

package v2.com.nyble;

import com.nyble.managers.ProducerManager;
import com.nyble.topics.Names;
import com.nyble.topics.consumer.ConsumerKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import com.nyble.util.DBUtil;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
    final static Logger logger = LogManager.getLogger(Utils.class);

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

    public static void runSqlFromTerminal(String dataSourceName, String command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
        pb.environment().put("PGPASSWORD", DBUtil.getInstance().getDataSourceProperties(dataSourceName).get("PASSWORD"));
        Process p = pb.start();
        String output = loadStream(p.getInputStream());
        String error = loadStream(p.getErrorStream());
        int rc = p.waitFor();
        if(!output.isEmpty()){
            logger.info(output);
        }
        if(!error.isEmpty()){
            if(rc == 0){
                logger.warn(error);
            }else{
                throw new RuntimeException("Error loading groups to DB, code = "+rc+" message = "+error);
            }
        }
        if( rc!=0 ){
            throw new RuntimeException("Error loading groups to DB, code = "+rc+" message = "+error);
        }
    }

    static String loadStream(InputStream s) throws IOException {
        try(BufferedReader br = new BufferedReader(new InputStreamReader(s))){
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null)
                sb.append(line).append("\n");
            return sb.toString();
        }
    }
}

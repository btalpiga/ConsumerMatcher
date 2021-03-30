package v2.com.nyble;

import com.nyble.util.DBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import v2.com.nyble.dataStructures.ConsumerFeatures;
import v2.com.nyble.dataStructures.ConsumerMapping;
import v2.com.nyble.manager.RuleManager;
import v2.com.nyble.manager.RulesManager;
import v2.com.nyble.rules.FullNameEmailRule;
import v2.com.nyble.rules.FullNamePhoneRule;
import v2.com.nyble.rules.PhoneEmailRule;

import java.sql.*;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

@Component
public class StartUpRunner  {
    //implements CommandLineRunner

    private static Logger logger = LoggerFactory.getLogger(StartUpRunner.class);

    @Scheduled(fixedDelay = 3600000)
    public void run() throws Exception{
        ConsumerMapping consumerMapping = createMapping();
        logger.info("end creating map");
        RulesManager rulesManager = new RulesManager(consumerMapping);
        rulesManager.registerRuleManager(new RuleManager(new FullNamePhoneRule()));
        rulesManager.registerRuleManager(new RuleManager(new FullNameEmailRule()));
        rulesManager.registerRuleManager(new RuleManager(new PhoneEmailRule()));

        processConsumers(rulesManager);
        logger.info("end creating buckets");

        Collection<Set<String>> groups = rulesManager.getBuckets();
        final String now = System.currentTimeMillis()+"";
        groups.forEach(group->{
            String uuid = UUID.randomUUID().toString();
            for(String consumerIdentification : group){
                String[] tokens = consumerIdentification.split("#");
                int systemId = Integer.parseInt(tokens[0]);
                int consumerId = Integer.parseInt(tokens[1]);
                Utils.updateConsumerAttribute(systemId, consumerId, "entityId", uuid, now);
            }
            if(group.size() > 1){
                logger.info(uuid+": "+group);
            }
        });

        logger.info("End consumer matcher");
    }

    private ConsumerMapping createMapping() throws SQLException {
        ConsumerMapping cm = new ConsumerMapping();
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("select system_id||'#'||consumer_id from consumers ")){
            while(rs.next()){
                cm.mapConsumer(rs.getString(1));
                if(cm.getSize()%10000 == 0){
                    logger.info("Mapped {} consumers", cm.getSize());
                }
            }
        }
        return cm;
    }

    private void processConsumers(RulesManager rulesManager) throws SQLException {
        final String query = "select cuec.system_id, cuec.consumer_id,\n" +
                "\tcuec.full_name as \"fullName\", cuec.phone, cuec.email, cf.is_phone_valid::text as \"phoneConfirmed\", " +
                "cf.is_email_valid::text as \"emailConfirmed\"\n" +
                "from consumers_unique_entity_criterias cuec join consumers_flags cf \n" +
                "on cuec.system_id = cf.system_id and cuec.consumer_id = cf.consumer_id ";
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)){
            while(rs.next()){
                int systemId = rs.getInt("system_id");
                int consumerId = rs.getInt("consumer_id");
                String fullName = rs.getString("fullName");
                String phone = rs.getString("phone");
                String email = rs.getString("email");
                String phoneConfirmed = rs.getString("phoneConfirmed");
                String emailConfirmed = rs.getString("emailConfirmed");
                ConsumerFeatures cf = new ConsumerFeatures(systemId, consumerId);
                cf.addFeature("fullName", fullName);
                cf.addFeature("phone", phone);
                cf.addFeature("email", email);
                cf.addFeature("phoneConfirmed", phoneConfirmed);
                cf.addFeature("emailConfirmed", emailConfirmed);
                rulesManager.updateGroups(cf);
            }
        }
    }
}

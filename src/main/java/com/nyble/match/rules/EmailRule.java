package com.nyble.match.rules;

import com.nyble.main.App;
import com.nyble.match.SystemConsumerEntity;
import com.nyble.models.consumer.Consumer;
import com.nyble.models.consumer.ConsumerFlag;
import com.nyble.topics.Names;
import com.nyble.topics.consumer.ConsumerValue;
import com.nyble.util.DBUtil;
import com.nyble.utils.StringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

public class EmailRule extends MatchingRule{

    final static Logger logger = LoggerFactory.getLogger(EmailRule.class);

    @Override
    public boolean match(String consumerId, String systemId, Set<SystemConsumerEntity> rez, Map<String, Object> extraInfoMap) {
        ConsumerValue consumerValue = (ConsumerValue) extraInfoMap.get("consumer");
        Consumer consumer = consumerValue.getConsumer();
        int system = Integer.parseInt(consumer.getValue("systemId"));
        if(system != Names.RRP_SYSTEM_ID && !consumer.isFlagSet(ConsumerFlag.EMAIL_CONFIRMED)){
            logger.info("Consumer is from {} system and email is not confirmed; continue to next rule", system);
            return true;
        }
        String emailValue = new StringConverter(consumer.getValue("email")).trim().nullIf("").get();
        if(emailValue == null){
            return true;
        }
        return getSameConsumers(rez, emailValue);
    }

    public boolean getSameConsumers(Set<SystemConsumerEntity> rez, String emailValue){
        final String query = String.format(
                "select cuec.system_id, cuec.consumer_id, entity_id \n" +
                "from %s cuec join consumers_flags cf using(system_id, consumer_id) \n" +
                "where email = '%s' and cf.email_confirmed", App.CONSUMER_UNIQUE_CRITERIA_TABLE, emailValue);

        try(Connection conn = DBUtil.getInstance().getConnection();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)
        ){
            while(rs.next()){
                SystemConsumerEntity sce = new SystemConsumerEntity(rs.getInt("system_id"), rs.getInt("consumer_id"),
                        rs.getString("entity_id"));
                rez.add(sce);
            }
            return true;
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            logger.error("Moving to next rule (error thrown)...");
            return true;
        }
    }
}

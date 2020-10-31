package com.nyble.match.rules;

import com.nyble.match.SystemConsumerEntity;
import com.nyble.models.consumer.Consumer;
import com.nyble.models.consumer.ConsumerFlag;
import com.nyble.topics.consumer.ConsumerValue;
import com.nyble.util.DBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

public class PhoneAndNameRule extends MatchingRule {

    final static Logger logger = LoggerFactory.getLogger(PhoneAndNameRule.class);

    @Override
    public boolean match(String consumerId, String systemId, Set<SystemConsumerEntity> rez, Map<String, Object> extraInfoMap) {

        ConsumerValue consumervalue = (ConsumerValue) extraInfoMap.get("consumer");
        Consumer consumer = consumervalue.getConsumer();
        String phoneValue = consumer.getValue("phone");
        String fullNameValue = consumer.getValue("fullName") ;
        if(phoneValue == null || phoneValue.isEmpty() || fullNameValue == null || fullNameValue.isEmpty() ||
                !consumer.isFlagSet(ConsumerFlag.IS_PHONE_VALID)){
            return true;
        }

        final String query = "select system_id, consumer_id, case when entity_id is null then -1 else entity_id end as entity_id \n" +
                "from consumers_unique_entity_criterias where phone = ':phone' and full_name = ':name'";
        try(Connection conn = DBUtil.getInstance().getConnection();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query.replace(":phone",phoneValue).replace(":name", fullNameValue));
        ){
            while(rs.next()){
                int entityId = rs.getInt("entity_id");
                SystemConsumerEntity sce = new SystemConsumerEntity(rs.getInt("system_id"), rs.getInt("consumer_id"),
                        entityId);
                rez.add(sce);
            }
            return true;
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            return true;
        }
    }
}

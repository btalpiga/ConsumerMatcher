package com.nyble.match;

import com.nyble.main.App;
import com.nyble.match.rules.*;
import com.nyble.util.DBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class Matcher {

    private static final Logger logger = LoggerFactory.getLogger(Matcher.class);
    private static final RulesChain chainOfRules;

    static  {
        synchronized(Matcher.class){
            chainOfRules = new RulesChain();
            chainOfRules.addRule(new RemoveUsersWithNoNameRule());
            chainOfRules.addRule(new RemoveSmsUsersRule());
            chainOfRules.addRule(new PhoneAndNameRule());
            chainOfRules.addRule(new PhoneAndLocationRule());
            chainOfRules.addRule(new EmailRule());
        }
    }

    public List<SystemConsumerEntity> getConsumersEntityId(int consumerId, int systemId, Map<String, Object> extraInfoMap)
            throws SQLException {
        Set<SystemConsumerEntity> matchedConsumers = chainOfRules.process(systemId, consumerId, extraInfoMap);
        if(matchedConsumers.isEmpty()) {
            return Collections.singletonList(new SystemConsumerEntity(systemId, consumerId, getNewEntityId(), true));
        }else{
            if(matchedConsumers.size() == 1){
                SystemConsumerEntity sce = new ArrayList<>(matchedConsumers).get(0);
                if(sce.entityId>=0 && isConsumerMovingToNewEntity(systemId+"", consumerId+"", sce.entityId+"")){
                    sce.entityId = getNewEntityId();
                    sce.needToUpdate = true;
                    return new ArrayList<>(matchedConsumers);
                }
            }
            unifyEntityId(matchedConsumers);
            return new ArrayList<>(matchedConsumers);
        }
    }

    public void unifyEntityId(Set<SystemConsumerEntity> matchedConsumers) throws SQLException {

        Map<Integer, Integer> freqMap = new HashMap<>();
        for(SystemConsumerEntity sce : matchedConsumers){
            if(sce.entityId == -1){ continue;}
            if(freqMap.containsKey(sce.entityId)){
                freqMap.put(sce.entityId, freqMap.get(sce.entityId)+1);
            }else{
                freqMap.put(sce.entityId, 1);
            }
        }

        List<Map.Entry<Integer, Integer>> orderedFreq = new ArrayList<>(freqMap.entrySet());
        orderedFreq.sort(Comparator.comparingInt(Map.Entry::getValue));
        int electedEntityId;
        if(orderedFreq.size() == 0){
            electedEntityId = getNewEntityId();
        }else{
            electedEntityId = orderedFreq.get(orderedFreq.size()-1).getKey();
        }
        for(SystemConsumerEntity sce : matchedConsumers){
            if(sce.entityId != electedEntityId){
                sce.entityId = electedEntityId;
                sce.needToUpdate = true;
            }
        }
    }

    public int getNewEntityId() throws SQLException {
        try(Connection conn = DBUtil.getInstance().getConnection();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT nextval('consumer_entity_seq')")){
            rs.next();
            return rs.getInt(1);
        }
    }

    public boolean isConsumerMovingToNewEntity(String systemId, String consumerId, String entityId)
            throws SQLException {

        final String query = String.format(
                "SELECT * from %s " +
                "where entity_id = %s and (system_id<>%s or consumer_id<>%s) limit 1",
                App.CONSUMER_UNIQUE_CRITERIA_TABLE, entityId, systemId, consumerId
        );

        logger.debug("call isConsumerMovingToNewEntity {}", systemId+"#"+consumerId);
        try(Connection conn = DBUtil.getInstance().getConnection();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)){
            return rs.next();
        }finally {
            logger.debug("end call isConsumerMovingToNewEntity {}", systemId+"#"+consumerId);
        }
    }
}

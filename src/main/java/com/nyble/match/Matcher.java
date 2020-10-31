package com.nyble.match;

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
    private static RulesChain chainOfRules;

    static  {
        synchronized(Matcher.class){
            chainOfRules = new RulesChain();
            chainOfRules.addRule(new RemoveUsersWithNoName());
            chainOfRules.addRule(new RemoveSmsUsersRule());
            chainOfRules.addRule(new PhoneAndNameRule());
            chainOfRules.addRule(new PhoneAndLocationRule());
            chainOfRules.addRule(new EmailRule());
        }
    }

    public static List<SystemConsumerEntity> getConsumersEntityId(int consumerId, int systemId, Map<String, Object> extraInfoMap)
            throws SQLException, IllegalAccessException {
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

    public static void unifyEntityId(Set<SystemConsumerEntity> matchedConsumers) throws IllegalAccessException, SQLException {
//        int uniqueEntityId = -1;
        //calculate frequencies
        Map<Integer, Integer> freqMap = new HashMap<>();
        for(SystemConsumerEntity sce : matchedConsumers){
            if(sce.entityId == -1){ continue;}
            if(freqMap.containsKey(sce.entityId)){
                freqMap.put(sce.entityId, freqMap.get(sce.entityId)+1);
            }else{
                freqMap.put(sce.entityId, 1);
            }
        }

        int maxEntityId = -1;
        int maxEntityIdFreq = 0;
        for(Map.Entry<Integer, Integer> e : freqMap.entrySet()){
            if(e.getValue() > maxEntityIdFreq){
                maxEntityIdFreq = e.getValue();
                maxEntityId = e.getKey();
            }
        }

        if(maxEntityId != -1){
            for(SystemConsumerEntity sce : matchedConsumers){
                if(sce.entityId != maxEntityId){
                    sce.entityId = maxEntityId;
                    sce.needToUpdate = true;
                }
            }
        }else{
            if(matchedConsumers.isEmpty()){
                throw new IllegalAccessException("Check point 1");
            }
            maxEntityId = getNewEntityId();
            for(SystemConsumerEntity sce : matchedConsumers){
                sce.entityId = maxEntityId;
                sce.needToUpdate = true;
            }
        }
    }

    public static int getNewEntityId() throws SQLException {
        try(Connection conn = DBUtil.getInstance().getConnection();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT nextval('consumer_entity_seq')")){
            rs.next();
            return rs.getInt(1);
        }
    }

    public static boolean isConsumerMovingToNewEntity(String systemId, String consumerId, String entityId)
            throws SQLException {

        final String query = "SELECT * from consumers_unique_entity_criterias " +
                "where entity_id = :entityId and (system_id<>:systemId or consumer_id<>:consumerId) limit 1";

        logger.debug("call isConsumerMovingToNewEntity {}", systemId+"#"+consumerId);
        try(Connection conn = DBUtil.getInstance().getConnection();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query.replace(":entityId", entityId)
                .replace(":systemId", systemId).replace(":consumerId", consumerId))){
            return rs.next();
        }finally {
            logger.debug("end call isConsumerMovingToNewEntity {}", systemId+"#"+consumerId);
        }
    }
}

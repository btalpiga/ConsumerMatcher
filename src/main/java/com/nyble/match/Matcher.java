package com.nyble.match;

import com.nyble.match.rules.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            chainOfRules.addRule(new RemoveBlackListedEmailsRule());
            chainOfRules.addRule(new EmailRule());
        }
    }

    public List<SystemConsumerEntity> getConsumersEntityId(int consumerId, int systemId, Map<String, Object> extraInfoMap){
        Set<SystemConsumerEntity> matchedConsumers = chainOfRules.process(systemId, consumerId, extraInfoMap);
        if(matchedConsumers.isEmpty()) {
            return Collections.singletonList(new SystemConsumerEntity(systemId, consumerId, getNewEntityId(), true));
        }else{
            unifyEntityId(matchedConsumers);
            return new ArrayList<>(matchedConsumers);
        }
    }

    public void unifyEntityId(Set<SystemConsumerEntity> matchedConsumers){
        String electedEntityId = getNewEntityId();
        for(SystemConsumerEntity sce : matchedConsumers){
                sce.entityId = electedEntityId;
                sce.needToUpdate = true;
        }
    }

    public String getNewEntityId(){
        UUID uniqueId = UUID.randomUUID();
        return uniqueId.toString();
    }
}

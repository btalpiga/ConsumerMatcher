package com.nyble.match.rules;

import com.nyble.match.SystemConsumerEntity;
import com.nyble.match.rules.MatchingRule;
import com.nyble.topics.consumer.ConsumerValue;

import java.util.Map;
import java.util.Set;

public class RemoveSmsUsersRule extends MatchingRule {

    @Override
    public boolean match(String consumerId, String systemId, Set<SystemConsumerEntity> consumers, Map<String, Object> extraInfoMap) {
        ConsumerValue cv = (ConsumerValue) extraInfoMap.get("consumer");
        if(cv != null){
            String fullName = cv.getConsumer().getValue("fullName");
            if(fullName == null) return false;
            return !fullName.toUpperCase().startsWith("SMSUSER");
        }
        return true;
    }
}

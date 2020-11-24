package com.nyble.match.rules;

import com.nyble.match.SystemConsumerEntity;
import com.nyble.topics.consumer.ConsumerValue;
import com.nyble.utils.StringConverter;

import java.util.Map;
import java.util.Set;

public class RemoveUsersWithNoNameRule extends MatchingRule {

    @Override
    public boolean match(String consumerId, String systemId, Set<SystemConsumerEntity> consumers, Map<String, Object> extraInfoMap) {
        boolean moveToNextRule;
        ConsumerValue cv = (ConsumerValue) extraInfoMap.get("consumer");
        if(cv == null || cv.getConsumer() == null){
            moveToNextRule = false;
        }else{
            String fullName = new StringConverter(cv.getConsumer().getValue("fullName"))
                    .trim().nullIf("").get();
            moveToNextRule = (fullName != null);
        }
        return moveToNextRule;
    }
}
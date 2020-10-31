package com.nyble.match.rules;

import com.nyble.match.SystemConsumerEntity;
import com.nyble.topics.consumer.ConsumerValue;

import java.util.Map;
import java.util.Set;

public class RemoveUsersWithNoName extends MatchingRule {

    @Override
    public boolean match(String consumerId, String systemId, Set<SystemConsumerEntity> consumers, Map<String, Object> extraInfoMap) {
        ConsumerValue cv = (ConsumerValue) extraInfoMap.get("consumer");
        return cv == null || cv.getConsumer().getValue("fullName") != null;
    }
}
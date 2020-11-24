package com.nyble.match.rules;

import com.nyble.match.SystemConsumerEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RulesChain {
    private MatchingRule head;
    private MatchingRule tail;
    private static final Logger logger = LoggerFactory.getLogger(RulesChain.class);

    public void addRule(MatchingRule r){
        if(head == null){
            head = r;
        }else{
            tail.setNextRule(r);
        }
        tail = r;
    }

    public Set<SystemConsumerEntity> process(int systemId, int consumerId, Map<String, Object> extraInfoMap){
        Set<SystemConsumerEntity> rez = new HashSet<>();
        if(head == null) {
            return rez;
        }

        MatchingRule currentRule = head;
        boolean continueToNextRule = true;
        while(continueToNextRule && currentRule != null){
            logger.debug("[START] Applying rule {} to consumer {}", currentRule.getClass(), systemId+"#"+consumerId);
            continueToNextRule = currentRule.match(consumerId+"", systemId+"", rez, extraInfoMap);
            logger.debug("[END] Applying rule {} to consumer {}", currentRule.getClass(), systemId+"#"+consumerId);
            currentRule = currentRule.getNextRule();
        }
        return rez;
    }
}

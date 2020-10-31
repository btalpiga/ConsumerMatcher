package com.nyble.match.rules;

import com.nyble.match.SystemConsumerEntity;

import java.util.Map;
import java.util.Set;

public abstract class MatchingRule {

    public abstract boolean match(String consumerId, String systemId, Set<SystemConsumerEntity> consumers, Map<String, Object> extraInfoMap);

    private MatchingRule nextRule;

    public void setNextRule(MatchingRule r){
        nextRule = r;
    }

    public MatchingRule getNextRule(){
        return nextRule;
    }

    protected boolean isNumerical(String numRep){
        for(char c: numRep.toCharArray()){
            if(!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }
}

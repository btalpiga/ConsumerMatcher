package com.nyble.match.rules;

import com.nyble.match.SystemConsumerEntity;
import com.nyble.topics.consumer.ConsumerValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RemoveBlackListedEmailsRule extends MatchingRule {

    final static Map<String, String> blackList = new HashMap<>();
    static{
        blackList.put("e338a9afc81062df988865af0d26f115167bce56cf3df2c7b74782fb872079f0", "_@YAHOO.COM");
        blackList.put("e4ec52fe5fa78a1742f899645093dae03396ed162db3b49934dcce8133a03cdc", "_@YAHOO.CO");
        blackList.put("7ee182648f9ff971b7eae0fe2aa32572a9b05b66e6d6a75e73e636bc09849d58", "_@YAHOO.CON");
        blackList.put("b6cb5167dc838b5c2361bd80a9c76cdc6ecd8c0a97236842ec869be81cb66db6", "_@YAHHO.COM");
        blackList.put("65eb5a493c2ed9b133c0e438ad80ae8b0ba4136ddf60251bf9d103884f0b8d0c", "NUARE@YAHOO.COM");
        blackList.put("017baef3a898bbbd4258183e53f76e1b008dbd5d7e1840b8ae7e8db788c6a277", "NU_ARE_GMAIL@YAHOO.COM");

    }

    @Override
    public boolean match(String consumerId, String systemId, Set<SystemConsumerEntity> consumers, Map<String, Object> extraInfoMap) {
        ConsumerValue cv = (ConsumerValue) extraInfoMap.get("consumer");
        boolean moveToNextRule;
        if(cv != null && cv.getConsumer() != null){
            String email = cv.getConsumer().getValue("email");
            moveToNextRule = !blackList.containsKey(email);
        }else{
            moveToNextRule = false;
        }
        return moveToNextRule;
    }
}

package v2.com.nyble.manager;

import v2.com.nyble.dataStructures.ConsumerFeatures;
import v2.com.nyble.rules.IRule;

import java.util.*;

public class RuleManager<T extends IRule> {

    private T rule;
    private Map<String, Set<String>> groups = new HashMap<>();

    public RuleManager(T rule) {
        this.rule = rule;
    }

    public Set<String> appendConsumer(ConsumerFeatures consumerFeatures){
        String hash = rule.getKey(consumerFeatures.getFeatures());
        Set<String> initSet = new HashSet<>();
        initSet.add(consumerFeatures.getSystemId()+"#"+consumerFeatures.getConsumerId());
        return groups.merge(hash, initSet, (existingSet, newSet)->{
            existingSet.addAll(newSet);
            return existingSet;
        });
    }

    public Map<String, Set<String>> getGroups(){
        return groups;
    }

    public T getRule(){
        return rule;
    }
}

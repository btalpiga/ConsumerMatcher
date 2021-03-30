package v2.com.nyble.manager;

import v2.com.nyble.dataStructures.ConsumerFeatures;
import v2.com.nyble.dataStructures.ConsumerMapping;
import v2.com.nyble.dataStructures.UnionFind;

import java.util.*;

public class RulesManager {

    private int idx = 0;
    private List<RuleManager> managers = new ArrayList<>();
    private Map<String, Integer> ruleIndexes = new HashMap<>();
    private UnionFind unionFind;
    private ConsumerMapping consumerMapping;

    public RulesManager(ConsumerMapping mapping){
        consumerMapping = mapping;
        unionFind = new UnionFind(mapping.getSize());
    }


    public void registerRuleManager(RuleManager rm){
        ruleIndexes.put(rm.getRule().getName(), idx);
        managers.add(rm);
        idx++;
    }

    public void updateGroups(ConsumerFeatures cf){
        managers.forEach(manager -> {
            Set<String> consumerGroup = manager.appendConsumer(cf);
            if(consumerGroup.size() > 1){
                Iterator<String> it = consumerGroup.iterator();
                String first = it.next();
                while(it.hasNext()){
                    String other = it.next();
                    unionFind.union(consumerMapping.getConsumerIndex(first), consumerMapping.getConsumerIndex(other));
                }
            }
        });
    }

    public Collection<Set<String>> getBuckets(){
        return unionFind.getBuckets(consumerMapping);
    }

    public List<RuleManager> getManagers(){
        return managers;
    }

}

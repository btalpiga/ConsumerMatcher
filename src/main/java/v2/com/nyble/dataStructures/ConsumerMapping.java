package v2.com.nyble.dataStructures;

import java.util.HashMap;
import java.util.Map;

public class ConsumerMapping {
    private Map<Integer, String> consumerMapping = new HashMap<>();
    private Map<String, Integer> reverse = new HashMap<>();
    private int index = 0;

    public void mapConsumer(String consumerIdentification){
        reverse.put(consumerIdentification, index);
        consumerMapping.put(index++, consumerIdentification);
    }

    public String getConsumerIdentification(Integer idx){
        return consumerMapping.get(idx);
    }

    public Integer getConsumerIndex(String id){
        return reverse.get(id);
    }

    public int getSize(){
        return index;
    }
}

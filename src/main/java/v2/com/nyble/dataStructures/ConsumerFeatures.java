package v2.com.nyble.dataStructures;

import java.util.HashMap;
import java.util.Map;

public class ConsumerFeatures {
    private int systemId;
    private int consumerId;
    private Map<String, String> features = new HashMap<>();

    public ConsumerFeatures(int systemId, int consumerId) {
        this.systemId = systemId;
        this.consumerId = consumerId;
    }

    public int getSystemId() {
        return systemId;
    }

    public int getConsumerId() {
        return consumerId;
    }

    public void addFeature(String name, String value){
        features.put(name, value);
    }

    public String getFeature(String name){
        return features.get(name);
    }

    public Map<String, String> getFeatures(){
        return features;
    }
}

package v2.com.nyble.dataStructures;

import java.util.*;

public class UnionFind {
    private int[] parent;
    public UnionFind(int size){
        parent = new int[size];
        for(int i=0;i<parent.length;i++){
            parent[i] = i;
        }
    }

    public int findRoot(int node){
        if(parent[node] != node){
            return findRoot(parent[node]);
        }else{
            return node;
        }
    }

    public void union(int node1, int node2){
        int root1 = findRoot(node1);
        int root2 = findRoot(node2);
        parent[root1] = root2;
    }

    public Collection<Set<String>> getBuckets(ConsumerMapping cm){
        Map<Integer, Set<String>> buckets = new HashMap<>();
        for(int i=0;i<parent.length;i++){
            int rootNode = findRoot(i);
            String consumerIdentification = cm.getConsumerIdentification(rootNode);
            Set<String> init = new HashSet<>();
            init.add(cm.getConsumerIdentification(i));
            buckets.merge(rootNode, init, (found, newSet)->{
                found.addAll(newSet);
                return found;
            });
        }

        return buckets.values();
    }
}

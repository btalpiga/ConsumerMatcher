import org.junit.Before;
import org.junit.Test;
import v2.com.nyble.*;
import v2.com.nyble.dataStructures.ConsumerFeatures;
import v2.com.nyble.dataStructures.ConsumerMapping;
import v2.com.nyble.manager.RuleManager;
import v2.com.nyble.manager.RulesManager;
import v2.com.nyble.rules.FullNameEmailRule;
import v2.com.nyble.rules.FullNamePhoneRule;
import v2.com.nyble.rules.PhoneEmailRule;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class V2_Test {

    RulesManager rulesManager;
    ConsumerMapping cm;
//
    @Before
    public void loadRules(){
        cm = new ConsumerMapping();
        cm.mapConsumer("2#6941");
        cm.mapConsumer("1#11777530");
        rulesManager = new RulesManager(cm);
        rulesManager.registerRuleManager(new RuleManager(new FullNamePhoneRule()));
        rulesManager.registerRuleManager(new RuleManager(new FullNameEmailRule()));
        rulesManager.registerRuleManager(new RuleManager(new PhoneEmailRule()));
    }
//
    @Test
    public void initTest(){
        ConsumerFeatures cf1 = new ConsumerFeatures(2, 6941);
        ConsumerFeatures cf2 = new ConsumerFeatures(1, 11777530);

        cf1.addFeature("fullName", "DRAGOSBRATU");
        cf1.addFeature("phone", "0cb86afd4100ebca8bc5f12c75ceaaab26d51ac3dd755bc7bd36af70e006b495");
        cf1.addFeature("phoneConfirmed", "true");
        cf1.addFeature("email", "0000dd1a5ebdadc52e1d6949b426f2881338bd1b9d09d9274db085ce6bfba1c8");
        cf1.addFeature("emailConfirmed", "true");

        cf2.addFeature("fullName", "DRAGOSBRATU");
        cf2.addFeature("phone", "0cb86afd4100ebca8bc5f12c75ceaaab26d51ac3dd755bc7bd36af70e006b495");
        cf2.addFeature("phoneConfirmed", "true");
        cf2.addFeature("email", "0000dd1a5ebdadc52e1d6949b426f2881338bd1b9d09d9274db085ce6bfba1c8");
        cf2.addFeature("emailConfirmed", "true");

        rulesManager.updateGroups(cf1);
        rulesManager.updateGroups(cf2);

        List<RuleManager> managers = rulesManager.getManagers();
        assertEquals(managers.get(0).getGroups().size(), 1);
        assertEquals(managers.get(1).getGroups().size(), 1);
        assertEquals(managers.get(2).getGroups().size(), 1);

        Collection<Set<String>> groups = rulesManager.getBuckets();
        assertEquals(1, groups.size());
        Set<String> group = groups.iterator().next();
        assertEquals(2, group.size());
        assertEquals(new HashSet<>(Arrays.asList("2#6941","1#11777530")), group);

        groups.forEach(g->{
            String uuid = UUID.randomUUID().toString();
            if(g.size() > 1){
                System.out.println(uuid+": "+g);
            }

        });
    }
//
//    @Test
//    public void twoInSameGroup(){
//        ConsumerFeatures cf1 = new ConsumerFeatures(1,1);
//        ConsumerFeatures cf2 = new ConsumerFeatures(1,2);
//        ConsumerFeatures cf3 = new ConsumerFeatures(1,3);
//
//        cf1.addFeature("fullName", "GHEORGHEPOPESCU");
//        cf1.addFeature("phone", "123");
//        cf1.addFeature("phoneConfirmed", "true");
//
//        cf2.addFeature("fullName", "GHEORGHEPOPESCU");
//        cf2.addFeature("phone", "123");
//        cf2.addFeature("phoneConfirmed", "true");
//
//        cf3.addFeature("fullName", "GHEORGHEPOPESCU");
//        cf3.addFeature("email", "222");
//        cf3.addFeature("emailConfirmed", "true");
//
//        List<ConsumerFeatures> consumerFeatures = Arrays.asList(cf1, cf2, cf3);
//        consumerFeatures.forEach(c -> rulesManager.updateGroups(c));
//
//        Map<String, String[]> actual = rulesManager.getConsumersMatrix();
//        Map<String, String[]> expected = new HashMap<>();
//        expected.put(cf1.getSystemId()+"#"+cf1.getConsumerId(),
//                new String[] {Utils.toSHA1(cf1.getFeature("fullName")+"#"+cf1.getFeature("phone")), "R2_0", "R3_0"});
//
//        expected.put(cf2.getSystemId()+"#"+cf2.getConsumerId(),
//                new String[] {Utils.toSHA1(cf1.getFeature("fullName")+"#"+cf1.getFeature("phone")), "R2_1", "R3_1"});
//
//        expected.put(cf3.getSystemId()+"#"+cf3.getConsumerId(),
//                new String[] {"R1_0", Utils.toSHA1(cf3.getFeature("fullName")+"#"+cf3.getFeature("email")), "R3_2"});
//
//        boolean equals = actual.size() == expected.size();
//        assertTrue(equals);
//
//        for(String expectedKey : expected.keySet()){
//            if(!Arrays.equals(expected.get(expectedKey), actual.get(expectedKey))){
//                equals = false;
//            }
//        }
//        if(!equals){
//            printMap(expected);
//            System.out.println("-------");
//            printMap(actual);
//        }
//        assertTrue(equals);
//    }
//
//
//    private void printMap(Map<String, String[]> m){
//        StringBuilder sb = new StringBuilder();
//        for(Map.Entry<String, String[]> e : m.entrySet()){
//            sb.append(e.getKey()).append("=").append(Arrays.toString(e.getValue())).append(", ");
//        }
//        sb.append("\n");
//        System.out.println(sb.toString());
//    }
}

import com.nyble.match.Matcher;
import com.nyble.match.SystemConsumerEntity;
import com.nyble.match.rules.*;
import com.nyble.models.consumer.CAttribute;
import com.nyble.models.consumer.Consumer;
import com.nyble.models.consumer.ConsumerFlag;
import com.nyble.topics.Names;
import com.nyble.topics.consumer.ChangedProperty;
import com.nyble.topics.consumer.ConsumerValue;
import junit.framework.TestCase;

import java.util.*;

public class MatchingTester extends TestCase {

    public void test_matchCase1(){
        String now = new Date().getTime()+"";
        Consumer c = new Consumer();
        c.setProperty("systemId", new CAttribute(Names.RMC_SYSTEM_ID+"", now));
        c.setProperty("consumerId", new CAttribute("11415230", now));
        c.setProperty("fullName", new CAttribute("CORNELIAPAUL", now));
        c.setProperty("phone", new CAttribute("f56837322ca60d7dc27c0210861212ac9613d4dd2fb246102f1f2136875be778", now));
        c.setProperty("email", new CAttribute("65eb5a493c2ed9b133c0e438ad80ae8b0ba4136ddf60251bf9d103884f0b8d0c", now));
        c.setProperty("location", new CAttribute("90eaf878c55bfcf5a528be638f8e0c2557495fa4e8e51a2b28200df4fa33c2d5", now));
        c.setFlag(ConsumerFlag.IS_PHONE_VALID);

        ChangedProperty cp = new ChangedProperty("birthDate", "", "fdsfsdgsgsfdg");
        ConsumerValue cv = new ConsumerValue(c, cp);

        Map<String, Object> map = new HashMap<>();
        map.put("consumer", cv);

        final RulesChain chainOfRules = new RulesChain();
        chainOfRules.addRule(new RemoveUsersWithNoNameRule());
        chainOfRules.addRule(new RemoveSmsUsersRule());
        chainOfRules.addRule(new PhoneAndNameRule());
        chainOfRules.addRule(new PhoneAndLocationRule());
        chainOfRules.addRule(new EmailRule());

        Set<SystemConsumerEntity> rez = chainOfRules.process(10647466, 1, map);
        assertFalse(rez.isEmpty());
        System.out.println(rez);

        assertEquals(1, rez.size());
    }

    public void test_matchCase2(){
        String now = new Date().getTime()+"";
        Consumer c = new Consumer();
        c.setProperty("systemId", new CAttribute(Names.RMC_SYSTEM_ID+"", now));
        c.setProperty("consumerId", new CAttribute("11969015", now));
        c.setProperty("fullName", new CAttribute("ADINAVIRJAN", now));
        c.setProperty("phone", new CAttribute("ce7c534a608848dcde376201c5532ec9e89efa016f9db29ee6ec7a767222b86a", now));
//        c.setProperty("email", new CAttribute("65eb5a493c2ed9b133c0e438ad80ae8b0ba4136ddf60251bf9d103884f0b8d0c", now));
        c.setProperty("location", new CAttribute("90eaf878c55bfcf5a528be638f8e0c2557495fa4e8e51a2b28200df4fa33c2d5", now));
        c.setFlag(ConsumerFlag.IS_PHONE_VALID);

        ChangedProperty cp = new ChangedProperty("birthDate", "", "fdsfsdgsgsfdg");
        ConsumerValue cv = new ConsumerValue(c, cp);

        Map<String, Object> map = new HashMap<>();
        map.put("consumer", cv);

        final RulesChain chainOfRules = new RulesChain();
        chainOfRules.addRule(new RemoveUsersWithNoNameRule());
        chainOfRules.addRule(new RemoveSmsUsersRule());
        chainOfRules.addRule(new PhoneAndNameRule());
        chainOfRules.addRule(new PhoneAndLocationRule());
        chainOfRules.addRule(new EmailRule());

        Set<SystemConsumerEntity> rez = chainOfRules.process(11969015, 1, map);
        assertFalse(rez.isEmpty());
        System.out.println(rez);

        assertEquals(2, rez.size());
    }

    public void test_matchCase3(){
        final int consumerId = 11372729;
        String now = new Date().getTime()+"";
        Consumer c = new Consumer();
        c.setProperty("systemId", new CAttribute(Names.RMC_SYSTEM_ID+"", now));
        c.setProperty("consumerId", new CAttribute(consumerId+"", now));
        c.setProperty("fullName", new CAttribute("COSMINDINU", now));
        c.setProperty("phone", new CAttribute("4e0e5ce2fb2e9f7439b4a89a5694c0185f652b6045f4a6ac0521dea4a3d36038", now));
        c.setProperty("email", new CAttribute("cb1ca7a04edbfdd46f1a7ae47662cb9029818c1e610f8cd507c38af5cd2c60dc", now));
        c.setProperty("location", new CAttribute("0c7ccf67c9a201ea2f106cc496e0eb078ade080ebda6c4bbe3ff49adf6c9c16f", now));
        c.setFlag(ConsumerFlag.IS_PHONE_VALID);
        c.setFlag(ConsumerFlag.IS_EMAIL_VALID);

        ChangedProperty cp = new ChangedProperty("birthDate", "", "fdsfsdgsgsfdg");
        ConsumerValue cv = new ConsumerValue(c, cp);

        Map<String, Object> map = new HashMap<>();
        map.put("consumer", cv);

        final RulesChain chainOfRules = new RulesChain();
        chainOfRules.addRule(new RemoveUsersWithNoNameRule());
        chainOfRules.addRule(new RemoveSmsUsersRule());
        chainOfRules.addRule(new PhoneAndNameRule());
        chainOfRules.addRule(new PhoneAndLocationRule());
        chainOfRules.addRule(new EmailRule());

        Set<SystemConsumerEntity> rez = chainOfRules.process(consumerId, 1, map);
        assertTrue(rez.isEmpty());
        System.out.println(rez);
    }
}

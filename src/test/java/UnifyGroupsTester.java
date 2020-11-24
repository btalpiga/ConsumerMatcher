import com.nyble.match.Matcher;
import com.nyble.match.SystemConsumerEntity;
import com.nyble.match.rules.EmailRule;
import com.nyble.models.consumer.CAttribute;
import com.nyble.models.consumer.Consumer;
import com.nyble.topics.consumer.ConsumerValue;
import junit.framework.TestCase;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UnifyGroupsTester extends TestCase {

//    public void test_unify() throws SQLException, IllegalAccessException {
//        EmailRule emailRule = new EmailRule();
//        String systemId = "1";
//        String consumerId = "12018608";
//        Set<SystemConsumerEntity> rez = new HashSet<>();
//        Map<String, Object> extraInfo = new HashMap<>();
//
//        Consumer consumer = new Consumer();
//        consumer.setProperty("email", new CAttribute("65eb5a493c2ed9b133c0e438ad80ae8b0ba4136ddf60251bf9d103884f0b8d0c", ""));
//        ConsumerValue cv = new ConsumerValue(consumer, null);
//        extraInfo.put("consumer", cv);
//        emailRule.match(consumerId, systemId, rez, extraInfo);
//
//        Matcher.unifyEntityId(rez);
//
//        assertEquals((int) rez.stream().filter(ce -> ce.needToUpdate).count(), 1);
//    }
}

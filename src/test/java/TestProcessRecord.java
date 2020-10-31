import com.nyble.main.App;
import junit.framework.TestCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TestProcessRecord extends TestCase {

    public void test_processRecord(){
        String key ="{\n" +
                "  \"systemId\": 2,\n" +
                "  \"consumerId\": 10438\n" +
                "}";
        String value = "{\n" +
                "  \"consumer\": {\n" +
                "    \"systemId\": {\n" +
                "      \"value\": \"2\",\n" +
                "      \"lut\": \"1601240400000\"\n" +
                "    },\n" +
                "    \"phone\": {\n" +
                "      \"value\": \"9c5a71a4199108d9fa4e519621e30b03e5399b3e68bfac0390185ec4246af6f7\",\n" +
                "      \"lut\": \"1601240400000\"\n" +
                "    },\n" +
                "    \"consumerId\": {\n" +
                "      \"value\": \"10438\",\n" +
                "      \"lut\": \"1601240400000\"\n" +
                "    },\n" +
                "    \"fullName\": {\n" +
                "      \"value\": \"MARIUSAFTEI\",\n" +
                "      \"lut\": \"1601240400000\"\n" +
                "    },\n" +
                "    \"location\": {\n" +
                "      \"value\": \"e2f79e5b60330bba4c289962231b6ba2957d0b14e7deb3110417003c79dea635\",\n" +
                "      \"lut\": \"1601240400000\"\n" +
                "    },\n" +
                "    \"declaredBrand\": {\n" +
                "      \"value\": \"13\",\n" +
                "      \"lut\": \"1601240400000\"\n" +
                "    },\n" +
                "    \"birthDate\": {\n" +
                "      \"value\": \"52c94443b54234e1aec68198433fab5310f300f4970c128169257652cd861a73\",\n" +
                "      \"lut\": \"1601240400000\"\n" +
                "    },\n" +
                "    \"email\": {\n" +
                "      \"value\": \"b460b79e4bfbaefde51fa7d10a935bc604cd77aef3aa6a81ba91fe0bcd57e04c\",\n" +
                "      \"lut\": \"1601240400000\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"changedProperty\": {\n" +
                "    \"propertyName\": \"fullName\",\n" +
                "    \"newValue\": \"MARIUSAFTEI\"\n" +
                "  }\n" +
                "}";
        App.processRecord(new ConsumerRecord<>("aaa", 0, 0, key, value));
    }
}

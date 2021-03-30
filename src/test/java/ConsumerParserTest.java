import com.google.gson.Gson;
import com.nyble.models.consumer.Consumer;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumer.ConsumerValue;
import junit.framework.TestCase;


public class ConsumerParserTest extends TestCase {

    public void test_checkJsonRep(){
        A a = new A();
        a.name="DANIE;ATOMA";
        Gson gson = new Gson();
        System.out.println(gson.toJson(a));
    }

    public void test_checkJsonRepDes(){
        String json = "{\"name\":\"DANIE;ATOMA\"}";
        Gson gson = new Gson();
        A a = gson.fromJson(json, A.class);
        System.out.println(a.name);
    }

    public void testConsumer1(){
        String json = "{\"consumer\": " +
                "{ " +
                "\"phone\": {\"lut\": \"1606337987651\", \"value\": \"78bc9926ff90505babb807731c0e69f9af671cdcf1fc169d6db8a328fd1b4ec1\"}," +
                "\"fullName\": {\"lut\": \"1606337987651\", \"value\": \"DANIE;ATOMA\"}, " +
                "\"consumerId\": {\"lut\": \"1601240400000\", \"value\": \"8463735\"}, " +
                "\"systemId\": {\"lut\": \"1601240400000\", \"value\": \"1\"}" +
                "}, " +
                "\"changedProperty\" : {\"propertyName\" : \"phone\", \"newValue\" : \"78bc9926ff90505babb807731c0e69f9af671cdcf1fc169d6db8a328fd1b4ec1\", \"oldValue\" : \"\"}" +
                "}";
        ConsumerValue c = (ConsumerValue) TopicObjectsFactory.fromJson(json, ConsumerValue.class);
    }
}

class A{
    public String name;
}

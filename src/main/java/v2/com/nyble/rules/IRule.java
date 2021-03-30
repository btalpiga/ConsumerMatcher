package v2.com.nyble.rules;

import java.util.Map;

public interface IRule {

    String getKey(Map<String, String> features);

    String getName();
}

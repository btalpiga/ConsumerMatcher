package v2.com.nyble.rules;

import v2.com.nyble.Utils;

import java.util.Map;
import java.util.Objects;

public class FullNamePhoneRule implements IRule {
    final static String ruleNo = "R1";
    long noQualifyCounter = 0;

    @Override
    public String getKey(Map<String, String> features) {


        if(!qualify(features)){
            return ruleNo+"_"+(noQualifyCounter++);
        }else{
            String fullName = features.get("fullName");
            String phone = features.get("phone");
            return Utils.toSHA1(fullName+"#"+phone);
        }
    }

    @Override
    public String getName() {
        return ruleNo;
    }

    private boolean qualify(Map<String, String> features){
        String fullName = features.get("fullName");
        String phone = features.get("phone");
        String phoneConfirmed = features.get("phoneConfirmed");
        return Objects.nonNull(fullName) && !fullName.isEmpty() && Objects.nonNull(phone) && !phone.isEmpty()
                && Objects.equals(phoneConfirmed, "true");
    }
}

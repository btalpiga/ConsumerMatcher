package v2.com.nyble.rules;

import v2.com.nyble.Utils;

import java.util.Map;
import java.util.Objects;

public class PhoneEmailRule implements IRule {
    final static String ruleNo = "R3";
    long noQualifyCounter = 0;

    @Override
    public String getKey(Map<String, String> features) {

        if(!qualify(features)){
            return ruleNo+"_"+(noQualifyCounter++);
        }else{
            String phone = features.get("phone");
            String email = features.get("email");
            return Utils.toSHA1(phone+"#"+email);
        }
    }

    @Override
    public String getName() {
        return ruleNo;
    }

    private boolean qualify(Map<String, String> features){
        String phone = features.get("phone");
        String phoneConfirmed = features.get("phoneConfirmed");
        String email = features.get("email");
        String emailConfirmed = features.get("emailConfirmed");
        return Objects.nonNull(phone) && !phone.isEmpty() && Objects.equals(phoneConfirmed, "true") &&
                Objects.nonNull(email) && !email.isEmpty() && Objects.equals(emailConfirmed, "true");
    }
}

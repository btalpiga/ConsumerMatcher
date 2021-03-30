package v2.com.nyble.rules;

import v2.com.nyble.Utils;

import java.util.Map;
import java.util.Objects;

public class FullNameEmailRule implements IRule {
    final static String ruleNo = "R2";
    long noQualifyCounter = 0;

    @Override
    public String getKey(Map<String, String> features) {


        if(!qualify(features)){
            return ruleNo+"_"+(noQualifyCounter++);
        }else{
            String fullName = features.get("fullName");
            String email = features.get("email");
            return Utils.toSHA1(fullName+"#"+email);
        }
    }

    @Override
    public String getName() {
        return ruleNo;
    }

    private boolean qualify(Map<String, String> features){
        String fullName = features.get("fullName");
        String email = features.get("email");
        String emailConfirmed = features.get("emailConfirmed");
        return Objects.nonNull(fullName) && !fullName.isEmpty() && Objects.nonNull(email) && !email.isEmpty()
                && Objects.equals(emailConfirmed, "true");
    }
}

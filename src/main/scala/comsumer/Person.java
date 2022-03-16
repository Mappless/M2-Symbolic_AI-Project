package comsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Person {
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    private int id;
    private String sideEffectName;
    private String sideEffectCode;

    public String toJson() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(this);
    }

    public boolean haveSideEffect(String code) {
        return code.equals(sideEffectCode);
    }

    public String getSideEffectName() {
        return sideEffectName;
    }

    public void setSideEffectName(String sideEffectName) {
        this.sideEffectName = sideEffectName;
    }

    public String getSideEffectCode() {
        return sideEffectCode;
    }

    public void setGetSideEffectCode(String sideEffectCode) {
        this.sideEffectCode = sideEffectCode;
    }
}

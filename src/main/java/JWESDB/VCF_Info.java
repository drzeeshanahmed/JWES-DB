package JWESDB;

import java.util.HashMap;

public class VCF_Info {
	
	HashMap<String,String> infoString = new HashMap<>();
	HashMap<String,Integer> infoInteger = new HashMap<>();
	HashMap<String,Float> infoFloat = new HashMap<>();
	HashMap<String,Boolean> infoFlag = new HashMap<>();
	
	public String getInfoString(String id) {
		if(infoString.containsKey(id))
			return infoString.get(id);
		else
			return "NA";
	}
	public void setInfoString(String id, String value) {
		infoString.put(id, value);
	}
	public Integer getInfoInteger(String id) {
		if(infoInteger.containsKey(id))
			return infoInteger.get(id);
		else
			return 0;
	}
	public void setInfoInteger(String id, Integer value) {
		infoInteger.put(id, value);
	}
	public Float getInfoFloat(String id) {
		if(infoFloat.containsKey(id))
			return infoFloat.get(id);
		else
			return 0f;
	}
	public void setInfoFloat(String id, Float value) {
		infoFloat.put(id, value);
	}
	public Boolean getInfoFlag(String id) {
		if(infoFlag.containsKey(id))
			return true;
		else
			return false;
	}
	public void setInfoFlag(String id, Boolean flag) {
		infoFlag.put(id, flag);
	}
	
}

package JWESDB;

import java.util.HashMap;

public class VCF_Sample {
	
	HashMap<String,String> sampleString = new HashMap<>();
	HashMap<String,Integer> sampleInteger = new HashMap<>();
	HashMap<String,Float> sampleFloat = new HashMap<>();
	HashMap<String,Boolean> sampleFlag = new HashMap<>();
	
	public String getSampleString(String id) {
		if(sampleString.containsKey(id))
			return sampleString.get(id);
		else
			return "NA";
	}
	public void setSampleString(String id, String value) {
		sampleString.put(id, value);
	}
	public Integer getSampleInteger(String id) {
		if(sampleInteger.containsKey(id))
			return sampleInteger.get(id);
		else
			return 0;
	}
	public void setSampleInteger(String id, Integer value) {
		sampleInteger.put(id, value);
	}
	public Float getSampleFloat(String id) {
		if(sampleFloat.containsKey(id))
			return sampleFloat.get(id);
		else
			return 0f;
	}
	public void setSampleFloat(String id, Float value) {
		sampleFloat.put(id, value);
	}
	public Boolean getSampleFlag(String id) {
		if(sampleFlag.containsKey(id))
			return true;
		else
			return false;
	}
	public void setSampleFlag(String id, Boolean flag) {
		sampleFlag.put(id, flag);
	}
}

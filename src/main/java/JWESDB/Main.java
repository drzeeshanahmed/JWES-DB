package JWESDB;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

public class Main {
	
	private static BufferedReader reader;
	private static final Pattern tabSplitter = Pattern.compile("\t");
	private static final Pattern semicolonSplitter = Pattern.compile(";");
	private static final Pattern colonSplitter = Pattern.compile(":");
	private static final Pattern equalSplitter = Pattern.compile("=");
	private static final Pattern commaSplitter = Pattern.compile(",");

	private static List<VCF_Variant> variants = new ArrayList<>();	
	static HashMap<String, String> filterMap = new HashMap<String, String>();
	static HashMap<String, String> infoMap = new HashMap<String, String>();
	static HashMap<String, String> infoMapType = new HashMap<String, String>();
	static HashMap<String, String> formatMap = new HashMap<String, String>();
	static HashMap<String, String> formatMapType = new HashMap<String, String>();
	
	public static List<String> toList(Pattern p, String text) {
		String[]array = p.split(text);
		List<String> list = new ArrayList<String>(array.length);
	    Collections.addAll(list, array);
	    return list;
	}
	
	public static void Samples(List<String> data, List<VCF_Sample> samples) {
		
		List<String> format = toList(colonSplitter, data.get(8));
		int nSamples = data.size() - 9;
		VCF_Sample tmpSample = null;
				
		for(int i =0; i < nSamples; i++) {
			
			tmpSample = new VCF_Sample();
			samples.add(tmpSample);
			
			for(int j =0; j < format.size(); j++) {
				List<String> sample = toList(colonSplitter, data.get(8 + i + 1));
				String value = sample.get(i + j);
				
				switch(formatMapType.get(format.get(j))) {
        		case "String":
        			samples.get(i).setSampleString(format.get(j), value);
        			break;
        		case "Float":
        			samples.get(i).setSampleFloat(format.get(j), Float.valueOf(value));
        			break;
        		case "Integer":
        			samples.get(i).setSampleInteger(format.get(j), Integer.valueOf(value));
        			break;
        		case "Flag":
        			samples.get(i).setSampleFlag(format.get(j), true);
        			break;
				}				
			}
		}
	}
	
	static void Information(List<String> data, VCF_Info info) {
		if (!data.get(7).equals("") && !data.get(7).equals(".")) {
	        List<String> props = toList(semicolonSplitter, data.get(7));
	        
	        for (String prop : props) {
	            int idx = prop.indexOf('=');
	            if (idx == -1) {
	            	info.setInfoFlag(prop, true);
	            }else {
		        	List<String> key = toList(equalSplitter, prop);
		        	switch(infoMapType.get(key.get(0))) {
		        		case "String":
		        			info.setInfoString(key.get(0), key.get(1));
		        			break;
		        		case "Float":
		        			info.setInfoFloat(key.get(0), Float.valueOf(key.get(1)));
		        			break;
		        		case "Integer":
		        			info.setInfoInteger(key.get(0), Integer.valueOf(key.get(1)));
		        			break;
		        		case "Flag":
		        			info.setInfoFlag(key.get(0), true);
		        			break;
		        	}
				}
	        }
		}
	}
	
	static int Variant(List<String> data, VCF_Variant v) {
		
		v.setChrom_number(data.get(0));
		v.setChrom_position(Long.parseLong(data.get(1)));
				
		if(!data.get(2).equals(".")) {
			v.setChrom_id(data.get(2));
		}
		
		v.setRef_base(data.get(3));
		v.setAlt_base(data.get(4));
				
	    if (!data.get(5).isEmpty() && !data.get(5).equals(".")) {
	    	v.setQuality(Float.parseFloat(data.get(5)));
	    }
		
		v.setFilter(data.get(6));
				
		v.setDescription(filterMap.get(data.get(6)));
				
		return 1;
	}
	
	public static void Metadata(String line) {
		
		int idx = line.indexOf("=");
	    String propName = line.substring(2, idx).trim();
	    String propValue = line.substring(idx + 1).trim();
	    propValue = propValue.replaceAll("<|>", "");
        List<String> values = toList(commaSplitter, propValue);
        
        String id, num, type, description;

	    switch (propName) {
	      case "FILTER":
	    	  id = values.get(0).split("=")[1];
	    	  description = values.get(1).split("=")[1].replaceAll("\"", "");
	    	  filterMap.put(id, description);
	    	  break;
	      case "INFO":
	    	  id = values.get(0).split("=")[1];
	    	  num = values.get(1).split("=")[1];
	    	  type = values.get(2).split("=")[1];
	    	  description = values.get(3).split("=")[1];
	    	  if(!num.equals("1") && type.equals("Integer") || !num.equals("1") && type.equals("Float"))
	    		  type = "String";
	    	  infoMap.put(id, description);
	    	  infoMapType.put(id, type);
	    	  break;
	      case "FORMAT":
	    	  id = values.get(0).split("=")[1];
	    	  num = values.get(1).split("=")[1];
	    	  type = values.get(2).split("=")[1];
	    	  description = values.get(3).split("=")[1];
	    	  if(!num.equals("1") && type.equals("Integer") || !num.equals("1") && type.equals("Float"))
	    		  type = "String";
	    	  formatMap.put(id, description);
	    	  formatMapType.put(id, type);
	    	  break;
	    }
	}

	public static void main (String[] args) {
		
		if(args.length < 4)
			System.exit(1);
		
		String line;

		try {
			reader = new BufferedReader(new FileReader(args[5]));
		
	        while ((line = reader.readLine()) != null) {
	        	if (line.startsWith("##")) {
	        		Metadata(line);
	        	}else if (line.startsWith("#")) { 
	        		filterMap.put("PASS", "Filter Passed Successfuly");
	        	}else {
	        		VCF_Variant vari = new VCF_Variant();
	        		VCF_Info info = new VCF_Info();
	        		List<VCF_Sample> samples = new ArrayList<>();


		        	List<String> data = toList(tabSplitter, line);
		        	Variant(data, vari);
		        	Information(data, info);
		        	Samples(data,samples);
		        	
		        	vari.setInfo(info);
		        	vari.setSamples(samples);
		        	variants.add(vari);
	        	}
	        }
	        
	        Sql sql = new Sql(args[0],Integer.valueOf(args[1]),args[2],args[3], args[4]);
	        sql.insertInDB(variants, infoMap, infoMapType, formatMap, formatMapType, false);
        
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

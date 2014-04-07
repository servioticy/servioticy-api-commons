package com.servioticy.api.commons.elasticsearch;

import java.util.ArrayList;
import java.util.List;

import com.servioticy.api.commons.data.CouchBase;
import com.servioticy.api.commons.datamodel.Data;

public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		
		long lastUpdate;
		lastUpdate = SearchEngine.getLastUpdateTimeStamp("1396461657731411aa73c28444ecf9a8c803e62312fd1","location");
		System.out.println(lastUpdate);
		lastUpdate = SearchEngine.getLastUpdateTimeStamp("1396461657731411aa73c28444ecf9a8c803e62312fa1","location");
		System.out.println(lastUpdate);
		lastUpdate = SearchEngine.getLastUpdateTimeStamp("1396461657731411aa73c28444ecf9a8c803e62312fd1","foo");
		System.out.println(lastUpdate);
		
		return;
		
		/*List<String> IDs = new ArrayList<String>(); 
		IDs.add("139594599486709ceb6bfdddb48cfabfcce0e6a9cf6c8");
		IDs.add("1396461657731411aa73c28444ecf9a8c803e62312fd1");
		
		String groupLastUpdate = SearchEngine.getGropLastUpdateDocId("location",IDs);
	    System.out.println(groupLastUpdate);*/
		
		/*String testJson = "{\"timerange\":true,\"rangefrom\":13,\"rangeto\":17}";
		SearchCriteria test = SearchCriteria.buildFromJson(testJson);
		if(test != null) {
			//System.out.println(test.toString());
			System.out.println(test.buildFilter());
			System.out.println("Json valid: " + test.valid());
		}
		else
			System.out.println("Error building SearchCriteria object from Json");

		testJson = "{\"timerange\":true,\"rangefrom\":13}";
		test = SearchCriteria.buildFromJson(testJson);
		if(test != null) {
			//System.out.println(test.toString());
			System.out.println(test.buildFilter());
			System.out.println("Json valid: " + test.valid());
		}
		else
			System.out.println("Error building SearchCriteria object from Json");

		testJson = "{\"timerange\":true,\"rangeto\":13}";
		test = SearchCriteria.buildFromJson(testJson);
		if(test != null) {
			//System.out.println(test.toString());
			System.out.println(test.buildFilter());
			System.out.println("Json valid: " + test.valid());
		}
		else
			System.out.println("Error building SearchCriteria object from Json");
		
		
		testJson = "{\"numericrange\":true,\"rangefrom\":13,\"rangeto\":17,\"numericrangefield\":\"foofield\"}";
		test = SearchCriteria.buildFromJson(testJson);
		if(test != null) {
			//System.out.println(test.toString());
			System.out.println(test.buildFilter());
			System.out.println("Json valid: " + test.valid());
		}
		else
			System.out.println("Error building SearchCriteria object from Json");
	 
		
		testJson = "{\"geodistance\":true,\"geodistancevalue\":300,\"pointlat\":43.15,\"pointlon\":15.43,\"geodistanceunit\":\"km\"}";
		test = SearchCriteria.buildFromJson(testJson);
		if(test != null) {
			//System.out.println(test.toString());
			System.out.println(test.buildFilter());
			System.out.println("Json valid: " + test.valid());
		}
		else
			System.out.println("Error building SearchCriteria object from Json");
		

		testJson = "{\"geoboundingbox\":true,\"geoboxupperleftlat\":43.15,\"geoboxupperleftlat\":15.43,\"geoboxbottomrightlat\":47.15,\"geoboxbottomrightlon\":15.47}";
		test = SearchCriteria.buildFromJson(testJson);
		if(test != null) {
			//System.out.println(test.toString());
			System.out.println(test.buildFilter());
			System.out.println("Json valid: " + test.valid());
		}
		else
			System.out.println("Error building SearchCriteria object from Json");

		testJson = "{\"match\":true,\"matchfield\":\"foofield\",\"matchstring\":\"value1 value2\"}";
		test = SearchCriteria.buildFromJson(testJson);
		if(test != null) {
			//System.out.println(test.toString());
			System.out.println(test.buildFilter());
			System.out.println("Json valid: " + test.valid());
		}
		else
			System.out.println("Error building SearchCriteria object from Json");

	
		testJson = "{\"numericrange\":true,\"rangefrom\":13,\"rangeto\":17,\"numericrangefield\":\"channels.age.current-value\",\"timerange\":true,\"rangefrom\":1396859660,\"match\":true,\"matchfield\":\"channels.name.current-value\",\"matchstring\":\"Peter John\",\"geoboundingbox\":true,\"geoboxupperleftlat\":43.15,\"geoboxupperleftlat\":15.43,\"geoboxbottomrightlat\":47.15,\"geoboxbottomrightlon\":15.47}";
		test = SearchCriteria.buildFromJson(testJson);
		if(test != null) {
			//System.out.println(test.toString());
			System.out.println(test.buildFilter());
			System.out.println("Json valid: " + test.valid());
		}
		else
			System.out.println("Error building SearchCriteria object from Json");

		
		*/
		
	}

}

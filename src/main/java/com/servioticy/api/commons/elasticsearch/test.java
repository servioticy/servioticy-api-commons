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
		
		
		/*long lastUpdate;
		lastUpdate = SearchEngine.getLastUpdate("139594599486709ceb6bfdddb48cfabfcce0e6a9cf6c8","location");
		System.out.println(lastUpdate);
		lastUpdate = SearchEngine.getLastUpdate("139594599486709ceb6bfdddb48cfabfcce0e6a9cf6c9","location");
		System.out.println(lastUpdate);
		lastUpdate = SearchEngine.getLastUpdate("139594599486709ceb6bfdddb48cfabfcce0e6a9cf6c8","foo");
		System.out.println(lastUpdate);*/
		
		List<String> IDs = new ArrayList<String>(); 
		IDs.add("139594599486709ceb6bfdddb48cfabfcce0e6a9cf6c8");
		IDs.add("1396461657731411aa73c28444ecf9a8c803e62312fd1");
		
		String groupLastUpdate = SearchEngine.getGropLastUpdateDocId("location",IDs);
	    System.out.println(groupLastUpdate);
	    
	}

}

package com.servioticy.api.commons.security;

import java.io.StringWriter;
import java.util.Map;


import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;




public class IDM {

//	POST /IDM/SO/
//	Headers:
//	Authorization: Bearer {auth_token}


	
	
	private static  HttpClient httpClient = new DefaultHttpClient();
	
	public static String PostSO(String auth_token, 
								String type, 
								String soId, 
								boolean data_provenance_collection,
								long cost,
								String url) {
		
		HttpRequestBase httpMethod;
		StringEntity input;
		
		String body;
		
//		Body: {
//		“type”: “simple”  /* or “composite”*/
//		"id": "139221276359507f4059f607a4a16b9583b4a169e4937",
//		 "data_provenance_collection": true /*or false*/,
//		 “cost”: 7890.2
//		}
		
		try {
			input = new StringEntity(body);
		} catch (Exception e) {
			return null;
		}
		input.setContentType("application/json");
		HttpPost httpPost = new HttpPost(url+"/IDM/SO");
		httpPost.setEntity(input);
		httpMethod = httpPost;	
		
//		if(headers != null){
//			for(Map.Entry<String, String> header : headers.entrySet()){
//				httpMethod.addHeader(header.getKey(), header.getValue());
//			}
//		}
		
		
		HttpResponse response;
		try {
			response = httpClient.execute(httpMethod);
		} catch (Exception e) {
			return null;
		}
		
		
		int statusCode = response.getStatusLine().getStatusCode();
		
//
//			“API_TOKEN”:”asdfasdfasdfasdfsadf”
//
//			“security_metadata:  {
//
//				"attributes" : {
//
//				"API_TOKEN": "ñaksjfñakjdsf",
//
//				"userid": "8898998aaaa7999aabbbbb1117",
//
//				.........
//
//			},
//
//			"policy": {
//
//				PLACEHOLDER FOR THE POLICY DESCRIPTOR
//
//			}
//
//			}
//
//		}
		
		return null;
	}
	
	
}

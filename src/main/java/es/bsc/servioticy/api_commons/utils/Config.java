/*******************************************************************************
 * Copyright 2014 Barcelona Supercomputing Center (BSC)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package es.bsc.servioticy.api_commons.utils;

import java.io.IOException;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.couchbase.client.CouchbaseClient;

//http://stackoverflow.com/questions/3153739/config-files-for-a-webapplication-load-once-and-store-where !!!
//http://stackoverflow.com/questions/11948321/reading-properties-file-only-once-while-web-app-deployment
public class Config implements ServletContextListener {
	private static final String ATTRIBUTE_NAME = "config";

	public static Properties config = new Properties();

	public static CouchbaseClient client;
	public static List<URI> uris = new LinkedList<URI>();
	
    @Override
    public void contextInitialized(ServletContextEvent event) {
      try {
        System.out.println("[SERVIOTICY-API] Loading properties");
        // Load properties
        config.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties"));
            
        // Connect to Couchbase
        System.out.println("[SERVIOTICY-API] Connecting to Coubhbase");

    	  String[] uri_array = config.getProperty("uris").split(";");
    	  for (int i = 0; i < uri_array.length; i++) {
    	    uris.add(URI.create(uri_array[i]));
    		} 
    	  try {
    	    client = new CouchbaseClient(uris, config.getProperty("bucket_name"), "");
    	  } catch (Exception e) {
    	    throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
    	  }
      } catch (IOException e) {
        throw new WebApplicationException(Response.status(Response.Status.UNAUTHORIZED)
            .entity("Loading config failed = " + e.getMessage())
            .build());
      }
        event.getServletContext().setAttribute(ATTRIBUTE_NAME, this);
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
    	System.out.println("[SERVIOTICY-API] Closing Couchbase connection");
		
    	// Disconnect to Couchbase
    	client.shutdown();
    }

    public String getProperty(String key) {
        return config.getProperty(key);
    }

}

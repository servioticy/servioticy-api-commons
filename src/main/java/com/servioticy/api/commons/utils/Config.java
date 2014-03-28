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
package com.servioticy.api.commons.utils;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.ws.rs.core.Response;

import com.couchbase.client.CouchbaseClient;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

//http://stackoverflow.com/questions/3153739/config-files-for-a-webapplication-load-once-and-store-where !!!
//http://stackoverflow.com/questions/11948321/reading-properties-file-only-once-while-web-app-deployment
public class Config implements ServletContextListener {
  private static final String ATTRIBUTE_NAME = "config";

  public static Properties config = new Properties();

  public static CouchbaseClient cli_so;
  public static CouchbaseClient cli_data;
  public static List<URI> public_uris = new LinkedList<URI>();

  public static CouchbaseClient cli_private;
  public static List<URI> private_uris = new LinkedList<URI>();

    @Override
    public void contextInitialized(ServletContextEvent event) {
      try {
        System.out.println("[SERVIOTICY-API] Loading properties");
        // Load properties
        config.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties"));

        // Connect to Couchbase
        System.out.println("[SERVIOTICY-API] Connecting to Coubhbase");

        String[] public_uri_array = config.getProperty("public_uris").split(";");
        for (int i = 0; i < public_uri_array.length; i++) {
          public_uris.add(URI.create(public_uri_array[i]));
        }
        String[] private_uri_array = config.getProperty("private_uris").split(";");
        for (int i = 0; i < private_uri_array.length; i++) {
          private_uris.add(URI.create(private_uri_array[i]));
        }
        try {
          cli_so = new CouchbaseClient(public_uris, config.getProperty("so_bucket"), "");
          cli_data = new CouchbaseClient(public_uris, config.getProperty("updates_bucket"), "");
          cli_private = new CouchbaseClient(private_uris, config.getProperty("private_bucket"), "");
        } catch (Exception e) {
          throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
        }
      } catch (IOException e) {
        throw new ServIoTWebApplicationException(Response.Status.UNAUTHORIZED, "Loading config failed = " + e.getMessage());
      }
        event.getServletContext().setAttribute(ATTRIBUTE_NAME, this);
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
      System.out.println("[SERVIOTICY-API] Closing Couchbase connection");

      // Disconnect to Couchbase
      cli_so.shutdown();
      cli_data.shutdown();
      cli_private.shutdown();
    }

    public String getProperty(String key) {
        return config.getProperty(key);
    }

    public static int getOpIdExpiration() {
      return 5*60;
    }

}

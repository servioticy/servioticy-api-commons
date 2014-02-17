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
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.api.commons.data.SQLite;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

/*
 * Class to manage REST API Authorization
 */
public class Authorization {
	private ResultSet rs;
	private String user_uuid;
	
	public Authorization() {}
	
	public void checkAuthorization(MultivaluedMap<String, String> headerParams) {
    
    // Check if exists request header Authorization
    String autorizationToken = headerParams.getFirst("Authorization");
    if (autorizationToken == null)
      throw new ServIoTWebApplicationException(Response.Status.UNAUTHORIZED, "Error in Authorization Header");
		
    // Check if exists user with token api
		try {
			SQLite db = new SQLite();
			rs = db.queryDB("select * from user where api_token = '" + autorizationToken + "'");
			if (!rs.next())
			  throw new ServIoTWebApplicationException(Response.Status.FORBIDDEN, "");

			// Obtain the uuid from the autorizationToken
			rs = db.queryDB("select uuid from user where api_token = '" + autorizationToken + "'");
			if (!rs.next()) {
			  throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
			}
			user_uuid = rs.getString("uuid");
			
			db.close();
		} catch (ClassNotFoundException e) {
		  throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
		} catch (SQLException e) {
		  throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
		}
	}
	
	public String getUserId() {
		return user_uuid;
	}

}

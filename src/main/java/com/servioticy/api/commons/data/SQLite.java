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
package com.servioticy.api.commons.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.ws.rs.core.Response;

import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.utils.Config;


/** 
 * User management to DEPRECATE only for initial proposes
 */
public class SQLite{
	
	private Connection connection = null;
	private Statement statement = null;
	
	public SQLite() throws ClassNotFoundException {
		// Create SQLite connection in every SQLite creation
		Class.forName("org.sqlite.JDBC");
		
		try {
			connection = DriverManager.getConnection(Config.config.getProperty("sqlitedb"));
			statement = connection.createStatement();
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
		}
	}
	
	public ResultSet queryDB(String query) {
		try {
			return statement.executeQuery(query);
		} catch (SQLException e) {
			throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
		}
	}
	
	public void close() {
		// TODO Disable closes to translate to ServletContextListener contextDestroyed
		try {
			statement.close();
			connection.close();
		} catch (SQLException e) {
			throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
		}
	}

}
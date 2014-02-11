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
package es.bsc.servioticy.api_commons.data;

import java.io.IOException;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import es.bsc.servioticy.api_commons.exceptions.ServIoTWebApplicationException;


public class SO {
	protected static ObjectMapper mapper = new ObjectMapper();
	
	protected String so_key, user_uuid, so_id;
	protected JsonNode so_root = mapper.createObjectNode();
	
//	public SO() { }
	
	/** Create a Service Object only generating its key
	 * 
	 * @param user_uuid
	 * 
	 */
	public SO(String user_uuid) {

		UUID uuid = UUID.randomUUID();

		// servioticy key = user_uuid + "-" + so_uuid
		// TODO improve key and so_id generation
		this.user_uuid = user_uuid;
		so_id = String.valueOf(System.currentTimeMillis()) + uuid.toString().replaceAll("-", "");
		so_key = user_uuid + "-" + so_id;
	}
	
	/** Create a Service Object
	 * 
	 * @param user_uuid
	 * @param body
	 */
	public SO(String user_uuid, String body) {
		this(user_uuid);
		
		JsonNode root;

		try {
			root = mapper.readTree(body);
		} catch (JsonProcessingException e) {
		  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
		} catch (IOException e) {
		  throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
		}
		
		((ObjectNode)so_root).put("id", so_id);
		((ObjectNode)so_root).putAll((ObjectNode)root);
	}
	
	/** Create a SO with a database stored Service Object
	 * 
	 * @param user_uuid
	 * @param so_id
	 * @param stored_so
	 */
	public SO(String user_uuid, String so_id, String stored_so) {
		try {
			so_root = mapper.readTree(stored_so);
			this.so_id = so_id;
			this.user_uuid = user_uuid;
			this.so_key = user_uuid + "-" + so_id;
		} catch (Exception e) {
			throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
		}
	}
	
	public String getString() {
		return so_root.toString();
	}
	
	/** 
	 * @return Service Object id
	 */
	public String getId() {
		return so_id;
	}
	
	/**
	 * @return the couchbase key
	 */
	public String getSOKey() {
		return so_key;
	}
	
	/**
	 * @return the user uuid
	 */
	public String getUser_uuid() {
		return user_uuid;
	}
}

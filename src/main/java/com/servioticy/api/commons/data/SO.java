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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class SO {
	protected static ObjectMapper mapper = new ObjectMapper();
	
	protected String so_key, user_id, so_id;
	protected JsonNode so_root = mapper.createObjectNode();
	
//	public SO() { }
	
	/** Create a Service Object only generating its key
	 * 
	 * @param user_uuid
	 * 
	 */
	private SO(String user_uuid) {

		UUID uuid = UUID.randomUUID();

		// servioticy key = user_uuid + "-" + so_uuid
		// TODO improve key and so_id generation
		this.user_id = user_uuid;
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
			this.user_id = user_uuid;
			this.so_key = user_uuid + "-" + so_id;
		} catch (Exception e) {
			throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
		}
	}
	
	/** Put the subscription id in the Service Object stream subscription array
	 * 
	 * @param stream
	 * @param subsId
	 */
	public void setSubscription(JsonNode stream, String subsId) {
	  ArrayList<String> subscriptions = new ArrayList<String>();

    try {
      if (!stream.path("subscriptions").isMissingNode()) {
         subscriptions = mapper.readValue(mapper.writeValueAsString(stream.get("subscriptions")),
           new TypeReference<ArrayList<String>>() {});
      }
      subscriptions.add(subsId);
      
      ((ObjectNode)stream).put("subscriptions", mapper.readTree(mapper.writeValueAsString(subscriptions)));

    } catch (JsonParseException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error parsing subscriptions array");
    } catch (JsonMappingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error deserializing subscriptions array");
    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error Json processing exception");
    } catch (IOException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error in subscriptions array");
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }
	  
	}
	
	public void setData(JsonNode stream, String dataId) {
	  ((ObjectNode)stream).put("data", dataId);
	}
	
//	public void appendData(String streamId, String body) {
//		JsonNode stream = getStream(streamId);
//		String dataId;
//
//		// Check if exists this streamId in the Service Object
//		if (stream == null)
//			throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "This Service Object does not have this stream.");
//		
//		CouchBase cb = new CouchBase();
//		Data data;
//		// Obtain or create new Data
//		if (!stream.path("data").isMissingNode()) {
//		  dataId = stream.get("data").asText();
//		  data = cb.getData(user_id, dataId);
//		} else {
//		  data = new Data(user_id);
//		  ((ObjectNode)stream).put("data", data.getId());
//		}
//
//		data.appendData(body);
//		
//	}
	 
	/** Return the subscriptions in output format
	 * 
	 * @param streamId
	 * @return subscriptions in output format
	 */
	public String responseSubscriptions(String streamId) {
	  
	  JsonNode stream = getStream(streamId);

		// Check if exists this streamId in the Service Object
		if (stream == null)
			throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No exists Stream: " + streamId + " in the Service Object");
		
		JsonNode subscriptions = stream.get("subscriptions");
		// Check if there are subscriptions
		if (subscriptions == null) 
		    throw new ServIoTWebApplicationException(Response.Status.NO_CONTENT, "");

		// iterate over the subscriptions id array, obtain the subscriptions and add to array with response fields
    CouchBase cb = new CouchBase();
    SO subscription;

    Iterator<JsonNode> subs = subscriptions.iterator();

    JsonNode root = mapper.createObjectNode();
    try {
      ArrayList<JsonNode> subsArray = new ArrayList<JsonNode>();
      while (subs.hasNext()) {
        subscription = cb.getSO(user_id, subs.next().asText());
        subsArray.add(mapper.readTree(subscription.getString()));
      }

      ((ObjectNode)root).put("subscriptions", mapper.readTree(mapper.writeValueAsString(subsArray)));
    } catch (JsonProcessingException e) {
			throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error parsing subscriptions array");
    } catch (IOException e) {
			throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error in subscriptions array");
    }catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }

    return root.toString();
    
    // [TODO] No well formated - To check //
    // use mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data)
    //  Map<String, Object> data = new LinkedHashMap<String, Object>(); 
//    Map<String, Object> map = new HashMap<String, Object>();
//    List<Object> list = new ArrayList<Object>();
//    while (subs.hasNext()) {
//      subscription = cb.getSO(user_id, subs.next().asText());
//      list.add(subscription.getString());
//    }
//    map.put("subscriptions", list);
//    
//    try {
//      return mapper.writeValueAsString(map.toString());
//    } catch (JsonProcessingException e) {
//      e.printStackTrace();
//    }
//    
//    return null;
////    return map.toString();
	}
	
	/**
	 * @return The streams in output format
	 */
	public String responseStreams() {
	  
	  JsonNode streams = so_root.path("streams");
	  if (streams == null)
	    throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No stremas in the Service Object");
	  
    JsonNode root = mapper.createObjectNode();
    try {
      Map<String, JsonNode> mstreams = mapper.readValue(streams.traverse(), new TypeReference<Map<String, JsonNode>>() {});
      ArrayList<JsonNode> astreams = new ArrayList<JsonNode>();
      JsonNode s = mapper.createObjectNode();
      
      for (Map.Entry<String, JsonNode> stream : mstreams.entrySet()) {
        ((ObjectNode)s).put("name", stream.getKey());
        ((ObjectNode)s).putAll((ObjectNode)stream.getValue());
        ((ObjectNode)s).remove("data");
        astreams.add(s);
      }
      ((ObjectNode)root).put("streams", mapper.readTree(mapper.writeValueAsString(astreams)));
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }
	  
	  return root.toString();
	}
  
	/**
	 * @return Service Object as String
	 */
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
		return user_id;
	}
	
	/**
	 * @param streamId
	 * @return the streamId JsonNode
	 */
	public JsonNode getStream(String streamId) {
	  return so_root.path("streams").get(streamId);
	}
	
//  // return ArrayList<String> with the subscriptions
//  public ArrayList<String> getSubscriptions(String streamId) {
//    ArrayList<String> subscriptions = new ArrayList<String>();
//    
//    if (so_root.path("streams").path(streamId).path("subscriptions").isMissingNode())
//      return null;
//    
//    try {
//      subscriptions = mapper.readValue(so_root.path("streams").path(streamId)
//          .get("subscriptions").traverse(), new TypeReference<ArrayList<String>>() {});
////    } catch (JsonParseException e) {
////      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error parsing subscription array");
////    } catch (JsonMappingException e) {
////      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error deserializing subscription array");
////    } catch (IOException e) {
////      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error in subscription array");
//    } catch (Exception e) {
//      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
//    }
//    
//    return subscriptions;
//  }
	 
  /** Update the Service Object updatedAt field
   * 
   */
  public void update() {
    // Update updatedAt time
    ((ObjectNode)so_root).put("updatedAt", System.currentTimeMillis());
  }
}

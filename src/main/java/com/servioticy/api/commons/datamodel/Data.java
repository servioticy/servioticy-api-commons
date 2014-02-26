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
package com.servioticy.api.commons.datamodel;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.data.CouchBase;
import com.servioticy.api.commons.data.SO;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class Data {
  protected static ObjectMapper mapper = new ObjectMapper();
  
  private String dataKey, dataId;
  private SO soParent;
  private JsonNode dataRoot = mapper.createObjectNode();
  
  /** Create a Data with a database stored Data
   * 
   * @param userId
   * @param dataId
   * @param stored_data
   */
  public Data(String dataId, String stored_data) {
    try {
      dataRoot = mapper.readTree(stored_data);
      this.dataId = dataId;
      this.dataKey = dataId;
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
  }
  

  /** Create a Data class
   * 
   * @param user_id
   * @param soId
   * @param streamId
   * @param body
   */
  public Data(SO so, String streamId, String body) {
    CouchBase cb = new CouchBase();
    
		soParent = so;
    JsonNode stream = soParent.getStream(streamId);

    // Check if exists this streamId in the Service Object
    if (stream == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "This Service Object does not have this stream.");
    
    JsonNode root;
    try {
      root = mapper.readTree(body);
      // Check if exists lastUpdate
      if (root.path("lastUpdate").isMissingNode())
        throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The lastUpdate field was not found");
      ((ObjectNode)dataRoot).put(root.get("lastUpdate").asText(), root);
    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "IOException");
    }
    
    // If Not exists generate the dataId and put in the SO stream data field
    // else loaded
    if (stream.path("data").isMissingNode()) {
      // servioticy key = dataId
      // TODO improve key and dataId generation
      UUID uuid = UUID.randomUUID(); //UUID java library
      dataId= String.valueOf(System.currentTimeMillis()) + uuid.toString().replaceAll("-", "");
      dataKey= dataId;
      soParent.setData(stream, dataId);
    }
    else {
      dataId = stream.get("data").asText();
      dataKey= dataId;

      JsonNode data = cb.getJsonNode(dataKey);
      if (data == null)
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);

      ((ObjectNode)data).putAll((ObjectNode)dataRoot);
      dataRoot = data;
    }
  }
  
//  public void appendData(String data) {
//    JsonNode root;
//    
//    try {
//      root = mapper.readTree(data);
//    } catch (JsonProcessingException e) {
//      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
//    } catch (IOException e) {
//      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "IOException");
//    }
//    
//    ((ObjectNode)data_root).put(root.get("lastUpdate").asText(), root.asText());
//    
//  }
  
  /**
   * @return the last JsonNode of Data
   */
  public JsonNode lastUpdate() {
    JsonNode lastUpdate = null;
    
    try {
//      Map<String, JsonNode> values = mapper.readValue(data_root.traverse(), new TypeReference<Map<String, JsonNode>>() {});
//      List<JsonNode> list = new ArrayList<JsonNode>(values.values());
//      lastUpdate = list.get(values.size() - 1).toString();
      
      NavigableMap<String, JsonNode> updates = mapper.readValue(dataRoot.traverse(), new TypeReference<TreeMap<String, JsonNode>>() {});
      Entry<String, JsonNode> lastEntry = updates.lastEntry();
      lastUpdate = lastEntry.getValue();
      
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
    
    return lastUpdate;
  }

	/** Generate response to last update of all data
	 * 
   * @return String
   */
  public String responseLastUpdate() {
    String response = "{ \"data\": [ " + lastUpdate().toString() + " ] }";
    return response;
  } 

  /** Generate response to getting all the Stream SO data
   * 
   * @return String
   */
  public String responseAllData() {
    String values = null;
    String response = null;
    try {
      // TODO evaluate performance
//      NavigableMap<String, JsonNode> updates = mapper.readValue(dataRoot.traverse(), new TypeReference<TreeMap<String, JsonNode>>() {});
      TreeMap<String, JsonNode> updates = mapper.readValue(dataRoot.traverse(), new TypeReference<TreeMap<String, JsonNode>>() {});
      values = updates.values().toString();
      response = "{ \"data\": " + values + " }";
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
    
    return response;
  }
  
  /**
   * @return Data key
   */
  public String getKey() {
    return dataKey;
  }

  /**
   * @return Data as String
   */
  public String getString() {
    return dataRoot.toString();
  }
  
  /**
   * @return The Service Object data owner
   */
  public SO getSO() {
    return soParent;
  }
  
  /** 
   * @return Data id
   */
  public String getId() {
    return dataId;
  }

}

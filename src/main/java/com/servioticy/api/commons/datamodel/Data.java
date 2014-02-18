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
  private SO so;
  private JsonNode data_root = mapper.createObjectNode();
  
  /** Create a Data with a database stored Data
   * 
   * @param user_id
   * @param dataId
   * @param stored_data
   */
  public Data(String user_id, String dataId, String stored_data) {
    try {
      data_root = mapper.readTree(stored_data);
      this.dataId = dataId;
      this.dataKey = user_id + "-" + dataId;
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }
  }
  

  /** Create a Data class
   * 
   * @param user_id
   * @param soId
   * @param streamId
   * @param body
   */
  public Data(String user_id, String soId, String streamId, String body) {
    CouchBase cb = new CouchBase();
    
    // Check if exists soId in the database
    so = cb.getSO(user_id, soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    JsonNode stream = so.getStream(streamId);

    // Check if exists this streamId in the Service Object
    if (stream == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "This Service Object does not have this stream.");
    
    JsonNode root;
    try {
      root = mapper.readTree(body);
      // Check if exists lastUpdate
      if (root.path("lastUpdate").isMissingNode())
        throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The lastUpdate filed was not found");
      ((ObjectNode)data_root).put(root.get("lastUpdate").asText(), root);
    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "IOException");
    }
    
    // If Not exists generate the dataId and put in the SO stream data field
    if (!stream.path("data").isMissingNode()) {
      dataId = stream.get("data").asText();
      dataKey= user_id + "-" + dataId;
      JsonNode data = cb.getJsonNode(dataKey);
      ((ObjectNode)data).putAll((ObjectNode)data_root);
      data_root = data;
      
    }
    else {
      // servioticy key = user_uuid + "-" + data_uuid
      // TODO improve key and subs_id generation
      UUID uuid = UUID.randomUUID(); //UUID java library
      dataId= String.valueOf(System.currentTimeMillis()) + uuid.toString().replaceAll("-", "");
      dataKey= user_id + "-" + dataId;

      so.setData(stream, dataId);
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
      
      NavigableMap<String, JsonNode> updates = mapper.readValue(data_root.traverse(), new TypeReference<TreeMap<String, JsonNode>>() {});
      Entry<String, JsonNode> lastEntry = updates.lastEntry();
      lastUpdate = lastEntry.getValue();
      
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }
    
    return lastUpdate;
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
    return data_root.toString();
  }
  
  /**
   * @return The Service Object data owner
   */
  public SO getSO() {
    return so;
  }
  
  /** 
   * @return Data id
   */
  public String getId() {
    return dataId;
  }

}
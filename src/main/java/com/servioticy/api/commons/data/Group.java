package com.servioticy.api.commons.data;

import java.io.IOException;
import java.util.ArrayList;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.datamodel.Data;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class Group {
  private static ObjectMapper mapper = new ObjectMapper();
  
  private String streamId, userId;
  private ArrayList<String> soIds;
  
  public Group(String user_id, String body) {
    JsonNode root;
    try {
      root = mapper.readTree(body);

      // Check if exists stream
      if (root.path("stream").isMissingNode())
        throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The stream field was not found");
      // Check if exists soIds
      if (root.path("soIds").isMissingNode())
        throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The soIds field was not found");
      
      streamId = ((ObjectNode)root).get("stream").asText();
      try {
        soIds = mapper.readValue(mapper.writeValueAsString(root.get("soIds")), new TypeReference<ArrayList<String>>() {});
      } catch (JsonProcessingException e) {
        throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "soIds has to be an array of Strings");
      }
      userId = user_id;
      
    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "IOException");
    }
  }
  
  public JsonNode lastUpdate() {
    CouchBase cb = new CouchBase();
    Data data;
    ObjectNode lastUpdate = mapper.createObjectNode();
    ObjectNode nextLastUpdate = mapper.createObjectNode();

    for (String soId : soIds) {
      data = cb.getData(userId, soId, streamId);
      if (data == null) throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND,
          "No data in the stream of the Service Object with soId: " + soId);
      nextLastUpdate = (ObjectNode)data.lastUpdate();
      if (lastUpdate.path("lastUpdate").asLong() < nextLastUpdate.get("lastUpdate").asLong()) {
        lastUpdate = nextLastUpdate;
      }
    }
    return lastUpdate;
  }

}

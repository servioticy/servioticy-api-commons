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

  protected String soKey, userId, soId;
  protected JsonNode soRoot = mapper.createObjectNode();

//  public SO() { }

  /** Create a SO with a database stored Service Object
   *
   * @param storedSO
   */
  public SO(String storedSO) {
    try {
      soRoot = mapper.readTree(storedSO);
      this.soId = soRoot.get("id").asText();
      this.userId = soRoot.get("userId").asText();
      this.soKey = soId;
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
  }

  /** Create a new Service Object
   *
   * @param userId
   * @param body
   */
  public SO(String userId, String body) {
    UUID uuid = UUID.randomUUID();

    // servioticy key = soId
    // TODO improve key and soId generation
    this.userId = userId;
    soId = String.valueOf(System.currentTimeMillis()) + uuid.toString().replaceAll("-", "");
    soKey = soId;

    JsonNode root;

    try {
      root = mapper.readTree(body);
    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }

    ((ObjectNode)soRoot).put("id", soId);
    ((ObjectNode)soRoot).put("userId", userId);
    if (root.path("public").isMissingNode()) {
      ((ObjectNode)soRoot).put("public", "false");
    } else {
      ((ObjectNode)soRoot).put("public", root.get("public").asText());
    }
    long time = System.currentTimeMillis();
    ((ObjectNode)soRoot).put("createdAt", time);
    ((ObjectNode)soRoot).put("updatedAt", time);
    ((ObjectNode)soRoot).putAll((ObjectNode)root);

    // If is a CSO with groups field create the derivate subscriptions
    if (!root.path("groups").isMissingNode()) {
      createGroupsSubscriptions(root.get("groups"));
    }
  }

//  /** Create a SO with a database stored Service Object
//   *
//   * @param userId
//   * @param soId
//   * @param storedSO
//   */
//  public SO(String userId, String soId, String storedSO) {
//    try {
//      soRoot = mapper.readTree(storedSO);
//      this.soId = soId;
//      this.userId = userId;
//      this.soKey = soId;
//    } catch (Exception e) {
//      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
//    }
//  }

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
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }

  }

  /** Creating subscriptions for groups information
   *
   * @param JsonNode representing groups information
   */
  public void createGroupsSubscriptions(JsonNode root) {
    ArrayList<Group> agroups = new ArrayList<Group>();

    try {
      Map<String, JsonNode> groups = mapper.readValue(root.traverse(), new TypeReference<Map<String, JsonNode>>() {});

      // Fill the groups data
      for (Map.Entry<String, JsonNode> group : groups.entrySet()) {
        agroups.add(new Group(group.getValue(), group.getKey()));
      }
    } catch (JsonParseException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error parsing groups");
    } catch (JsonMappingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error deserializing groups");
    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error Json processing exception");
    } catch (IOException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error in groups field");
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }

    // Check that soIds has the stream
    for (Group group : agroups) {
      group.checkSoIdsStream();
    }

    // Now create the subscriptions
    for (Group group : agroups) {
      group.createSubscriptions(soId);
    }

  }

  /** Create the data field to refer stream data values
   *
   * @param stream
   * @param dataId
   */
  public void setData(JsonNode stream, String dataId) {
    ((ObjectNode)stream).put("data", dataId);
  }

//  public void appendData(String streamId, String body) {
//    JsonNode stream = getStream(streamId);
//    String dataId;
//
//    // Check if exist this streamId in the Service Object
//    if (stream == null)
//      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "This Service Object does not have this stream.");
//
//    CouchBase cb = new CouchBase();
//    Data data;
//    // Obtain or create new Data
//    if (!stream.path("data").isMissingNode()) {
//      dataId = stream.get("data").asText();
//      data = cb.getData(user_id, dataId);
//    } else {
//      data = new Data(user_id);
//      ((ObjectNode)stream).put("data", data.getId());
//    }
//
//    data.appendData(body);
//
//  }

  /** Generate response to a SO creation
   *
   * @return String
   */
  public String responseCreateSO() {
    JsonNode root = mapper.createObjectNode();

    try {
      ((ObjectNode)root).put("id", soId);
      ((ObjectNode)root).put("createdAt", soRoot.get("createdAt").asLong());
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
    return root.toString();
  }

  /** Generate response to getting a SO
   *
   * @return String
   */
  public String responseGetSO() {
    JsonNode root = soRoot;

    ((ObjectNode)root).remove("userId");
    ((ObjectNode)root).remove("data");

    return root.toString();
  }

  /** Return the subscriptions in output format
   *
   * @param streamId
   * @return subscriptions in output format
   */
  public String responseSubscriptions(String streamId) {

    JsonNode stream = getStream(streamId);

    // Check if exist this streamId in the Service Object
    if (stream == null)
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "There is no Stream: " + streamId + " in the Service Object");

    JsonNode subscriptions = stream.get("subscriptions");
    // Check if there are subscriptions
    if (subscriptions == null) return null;

    // iterate over the subscriptions id array, obtain the subscriptions and add to array with response fields
    CouchBase cb = new CouchBase();
    Subscription subscription;

    Iterator<JsonNode> subs = subscriptions.iterator();

    JsonNode root = mapper.createObjectNode();
    try {
      ArrayList<JsonNode> subsArray = new ArrayList<JsonNode>();
      while (subs.hasNext()) {
        subscription = cb.getSubscription(subs.next().asText());
        subsArray.add(mapper.readTree(subscription.getString()));
      }

      ((ObjectNode)root).put("subscriptions", mapper.readTree(mapper.writeValueAsString(subsArray)));
    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error parsing subscriptions array");
    } catch (IOException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error in subscriptions array");
    }catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
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

  /** Return the streams in output format
   *
   * @return The streams in output format
   */
  public String responseStreams() {

    JsonNode streams = soRoot.path("streams");
    if (streams == null) return null;

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
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }

    return root.toString();
  }

  /**
   * @return Service Object as String
   */
  public String getString() {
    return soRoot.toString();
  }

  /**
   * @return Service Object id
   */
  public String getId() {
    return soId;
  }

  /**
   * @return the couchbase key
   */
  public String getSOKey() {
    return soKey;
  }

  /**
   * @return the user Id
   */
  public String getUserId() {
    return userId;
  }

  public boolean isPublic() {
    if (soRoot.path("public").equals("true")) {
      return true;
    }
    return false;
  }

  /**
   * @param streamId
   * @return the streamId JsonNode
   */
  public JsonNode getStream(String streamId) {
    return soRoot.path("streams").get(streamId);
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
//      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
//    }
//
//    return subscriptions;
//  }

  /** Update the Service Object updatedAt field
   *
   */
  public void update() {
    // Update updatedAt time
    ((ObjectNode)soRoot).put("updatedAt", System.currentTimeMillis());
  }
}

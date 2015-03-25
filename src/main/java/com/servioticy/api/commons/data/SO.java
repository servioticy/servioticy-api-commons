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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.elasticsearch.SearchEngine;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.mapper.StreamsMapper;
import com.servioticy.api.commons.security.IDM;
import com.servioticy.api.commons.utils.Config;


public class SO {
  protected static ObjectMapper mapper = new ObjectMapper();
  protected String soKey, userId, soId;
  protected JsonNode soRoot = mapper.createObjectNode();
  private static Logger LOG = org.apache.log4j.Logger.getLogger(SO.class);

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
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /** Create a new Service Object
   *
   * @param userId
   * @param body
   */
  public SO(String accessToken, String userId, String body) {
    UUID uuid = UUID.randomUUID();

    // servioticy key = soId
    // TODO improve key and soId generation
    this.userId = userId;
    soId = String.valueOf(System.currentTimeMillis()) + uuid.toString().replaceAll("-", "");
    soKey = soId;

    JsonNode root;

    try {
      root = mapper.readTree(body);

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

      // Parsing streams
      if (root.path("streams").isMissingNode()) {
        throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Service Object without streams");
      } else {
        ((ObjectNode)soRoot).put("streams", StreamsMapper.parse(root.get("streams")));
      }

    } catch (JsonProcessingException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
          "JsonProcessingException - " + e.getMessage());
    } catch (IOException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    ((ObjectNode)soRoot).put("id", soId);
    ((ObjectNode)soRoot).put("userId", userId);

    long time = System.currentTimeMillis();
    ((ObjectNode)soRoot).put("createdAt", time);
    ((ObjectNode)soRoot).put("updatedAt", time);

    ((ObjectNode)soRoot).putAll((ObjectNode)root);

    // Security stuff
    JsonNode security = IDM.PostSO(accessToken, soId, true, false, false, Config.idm_host, Config.idm_port);
    if (security == null)
    	throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    ((ObjectNode)soRoot).put("security", security);

    // If is a CSO with groups field create the derivate subscriptions
    if (!root.path("groups").isMissingNode()) {
      createGroupsSubscriptions(accessToken, root.get("groups"));
    }
  }

//  public void appendSecurity(JsonNode root) {
//    ((ObjectNode)soRoot).put("security", root);
//  }

  public JsonNode getSecurity() {
    return soRoot.get("security");
  }

  public void updateSecurity(String body) {

  JsonNode root;
  try {
    root = mapper.readTree(body);
      if (!root.path("security").isMissingNode()) {
        ((ObjectNode)soRoot).put("security", root.get("security"));
      }
  } catch (JsonProcessingException e) {
    LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

//  Security metadata modification does not modify the update timesatamp
//    // Update updatedAt time
//    ((ObjectNode)soRoot).put("updatedAt", System.currentTimeMillis());
  }

  /** Update the Service Object
   *
   */
  public void update(String body) {

  JsonNode root;
  try {
    root = mapper.readTree(body);
      if (!root.path("customFields").isMissingNode()) {
        ((ObjectNode)soRoot).put("customFields", root.get("customFields"));
      }
  } catch (JsonProcessingException e) {
    LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    // Update updatedAt time
    ((ObjectNode)soRoot).put("updatedAt", System.currentTimeMillis());
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

  /** Creating subscriptions for groups information
   *
   * @param JsonNode representing groups information
   */
  public void createGroupsSubscriptions(String accessToken, JsonNode root) {
    ArrayList<Group> agroups = new ArrayList<Group>();

    try {
      Map<String, JsonNode> groups = mapper.readValue(root.traverse(), new TypeReference<Map<String, JsonNode>>() {});

      // Fill the groups data
      for (Map.Entry<String, JsonNode> group : groups.entrySet()) {
        agroups.add(new Group(group.getValue(), group.getKey()));
      }
    } catch (JsonParseException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error parsing groups");
    } catch (JsonMappingException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error deserializing groups");
    } catch (JsonProcessingException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error Json processing exception");
    } catch (IOException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error in groups field");
    } catch (Exception e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    // Check that soIds has the stream
    for (Group group : agroups) {
      group.checkSoIdsStream();
    }

    // Now create the subscriptions
    for (Group group : agroups) {
      group.createSubscriptions(accessToken, soId, userId);
    }

  }

  /** Generate response to a SO creation
   *
   * @return String
   */
  public String responseCreateSO() {
    JsonNode root = mapper.createObjectNode();

    try {
      ((ObjectNode)root).put("id", soId);
      ((ObjectNode)root).put("api_token", soRoot.path("security").get("api_token").asText());
      ((ObjectNode)root).put("createdAt", soRoot.get("createdAt").asLong());
    } catch (Exception e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
    return root.toString();
  }

  /** Generate response to a SO creation
   *
   * @return String
   */
  public String responseUpdateSO() {
    JsonNode root = mapper.createObjectNode();

    try {
      ((ObjectNode)root).put("id", soId);
      ((ObjectNode)root).put("updatedAt", soRoot.get("updatedAt").asLong());
    } catch (Exception e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
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
    ((ObjectNode)root).remove("public");
    ((ObjectNode)root).remove("data");
    ((ObjectNode)root).remove("security");

    return root.toString();
  }

  /** Generate private response to getting a SO
   *
   * @return String
   */
  public String responsePrivateGetSO() {
    JsonNode root = soRoot;

    ((ObjectNode)root).remove("userId");
    ((ObjectNode)root).remove("public");
    ((ObjectNode)root).remove("data");

    return root.toString();
  }

  /** Return the subscriptions in output format
   *
   * @param streamId
   * @return subscriptions in output format
   */
  public String responseSubscriptions(String streamId, boolean externalOnly) {

    JsonNode stream = getStream(streamId);

    // Check if exist this streamId in the Service Object
    if (stream == null)
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "There is no Stream: " + streamId + " in the Service Object");

    // Get the Subscriptions
    List<String> IDs = externalOnly ?
               SearchEngine.getExternalSubscriptionsByStream(soId, streamId) :
               SearchEngine.getAllSubscriptionsByStream(soId, streamId);

    ArrayList<JsonNode> subsArray = new ArrayList<JsonNode>();

    JsonNode root = mapper.createObjectNode();
    if(root == null) {
      LOG.error("Could not create JSON mapper...");
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Could not create JSON mapper...");
    }
    try {
      for(String id : IDs) {
        Subscription tmp = CouchBase.getSubscription(id);
        if(tmp != null)
          subsArray.add(mapper.readTree(tmp.responseGetSubs()));
        else
          LOG.error("Subscription id: "+id+", reported by search engine but not found in CouchBase. Skipping...");
      }
      ((ObjectNode)root).put("subscriptions", mapper.readTree(mapper.writeValueAsString(subsArray)));
    } catch (JsonProcessingException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error parsing subscriptions array");
    } catch (IOException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error in subscriptions array");
    }catch (Exception e) {
      LOG.error("Error:",e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
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
  
  /** Return the subscriptions in raw format
  *
  * @param streamId
  * @return subscriptions in raw format
  */
 public String responseInternalSubscriptions(String streamId, boolean externalOnly) {

   JsonNode stream = getStream(streamId);

   // Check if exist this streamId in the Service Object
   if (stream == null)
     throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "There is no Stream: " + streamId + " in the Service Object");

   // Get the Subscriptions
   List<String> IDs = externalOnly ?
              SearchEngine.getExternalSubscriptionsByStream(soId, streamId) :
              SearchEngine.getAllSubscriptionsByStream(soId, streamId);

   ArrayList<JsonNode> subsArray = new ArrayList<JsonNode>();

   JsonNode root = mapper.createObjectNode();
   if(root == null) {
     LOG.error("Could not create JSON mapper...");
     throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Could not create JSON mapper...");
   }
   try {
     for(String id : IDs) {
       Subscription tmp = CouchBase.getSubscription(id);
       if(tmp != null)
         subsArray.add(mapper.readTree(tmp.getString())); // In raw format
       else
         LOG.error("Subscription id: "+id+", reported by search engine but not found in CouchBase. Skipping...");
     }
     ((ObjectNode)root).put("subscriptions", mapper.readTree(mapper.writeValueAsString(subsArray)));
   } catch (JsonProcessingException e) {
     LOG.error(e);
     throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error parsing subscriptions array");
   } catch (IOException e) {
     LOG.error(e);
     throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Error in subscriptions array");
   }catch (Exception e) {
     LOG.error("Error:",e);
     throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
   }

   return root.toString();
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
      JsonNode s;

      for (Map.Entry<String, JsonNode> stream : mstreams.entrySet()) {
      s = mapper.createObjectNode();
        ((ObjectNode)s).put("name", stream.getKey());
        ((ObjectNode)s).putAll((ObjectNode)stream.getValue());
        ((ObjectNode)s).remove("data");
        astreams.add(s);
      }
      ((ObjectNode)root).put("streams", mapper.readTree(mapper.writeValueAsString(astreams)));
    } catch (Exception e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    return root.toString();
  }

  /** Return the actions
  *
  * @return The actions in the SO
  */
 public String responseActuations() {

   JsonNode actions = soRoot.path("actions");
   if (actions == null) return null;

   return actions.toString();
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

//  public boolean isPublic() {
//    if (soRoot.get("public").asText().equals("true")) {
//      return true;
//    }
//    return false;
//  }

  /**
   * @param streamId
   * @return the streamId JsonNode
   */
  public JsonNode getStream(String streamId) {
    return soRoot.path("streams").get(streamId);
  }

  /**
   * @return the streamId JsonNode
   */
  public String getActuationsString() {
    //TODO: could be null?
    return soRoot.path("actions").toString();
  }

  /**
   * @param actuationName
   * @return the actutation JsonNode
   */
  public JsonNode getActuation(String actuationName) {
    JsonNode actions = soRoot.path("actions");
    if(actions == null) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
          "Actuation '"+actuationName+"' does not exist for SO: " + soId);
    }
    for (int i = 0; i < actions.size(); i ++) {
    if(actions.get(i).path("name").asText().equalsIgnoreCase(actuationName))
      return actions.get(i);
    }

    throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
        "Actuation '"+actuationName+"' does not exist for SO: " + soId);

  }



  /**
   * @return true if the action exists in the SO
   */

  public boolean checkActuation(String actuationName) {

    JsonNode actions = soRoot.path("actions");
    if(actions == null) {
      //System.out.println("actions is null");
      return false;
    }
    for (int i = 0; i < actions.size(); i ++) {
    //System.out.println(i+": "+actions.get(i).path("name").asText());
    if(actions.get(i).path("name").asText().equalsIgnoreCase(actuationName))
      return true;
    }

    return false;
  }

//  /** Update the Service Object updatedAt field
//   *
//   */
//  private void updateTimestamp() {
//    // Update updatedAt time
//    ((ObjectNode)soRoot).put("updatedAt", System.currentTimeMillis());
//  }
}

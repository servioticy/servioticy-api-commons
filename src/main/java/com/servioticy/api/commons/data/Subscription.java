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
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.elasticsearch.SearchEngine;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.security.IDM;

public class Subscription {
  private static ObjectMapper mapper = new ObjectMapper();
  private static Logger LOG = org.apache.log4j.Logger.getLogger(Subscription.class);

//  private String subsKey, subsId, userId;
  private String subsKey, subsId;
  private SO soParent;
  private JsonNode subsRoot = mapper.createObjectNode();

  /** Create a Subscription with a database stored Subscription
   *
   * @param storedSubs
   */
  public Subscription(String storedSubs, String subsKey) {
    try {
      subsRoot = mapper.readTree(storedSubs);
      this.subsId = subsRoot.get("id").asText();
      this.subsKey = subsKey;
//      this.userId = userId;
      this.soParent = CouchBase.getSO(subsRoot.get("source").asText());
    } catch (Exception e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /** Create a Subscription
   *
   * @param soId
   * @param streamId
   * @param body
   */
  public Subscription(String authToken, String userId, SO so, String streamId, String body) {
    JsonNode root;

    soParent = so;
    JsonNode stream = soParent.getStream(streamId);

    // Check if exists this streamId in the Service Object
    if (stream == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "This Service Object does not have this stream.");

    try {
      root = mapper.readTree(body);
    } catch (JsonProcessingException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    // Check if exists callback field in body request
    if (root.path("callback").isMissingNode())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No callback in the request");

    // Check if exists destination field in body request
    if (root.path("destination").isMissingNode() && !root.get("callback").asText().equals("pubsub"))
        throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No destination in the request");

    // OK, create the subscription

    // servioticy key = subsId
    // TODO improve key and subsId generation
    UUID uuid = UUID.randomUUID(); //UUID java library

    subsId= uuid.toString().replaceAll("-", "");
    subsKey = subsId;

    ((ObjectNode)subsRoot).put("id", subsId);
    ((ObjectNode)subsRoot).put("userId", userId);
    long time = System.currentTimeMillis();
    ((ObjectNode)subsRoot).put("createdAt", time);
    ((ObjectNode)subsRoot).put("updatedAt", time);
    ((ObjectNode)subsRoot).put("callback", root.get("callback").asText());
    ((ObjectNode)subsRoot).put("source", soParent.getId());

    // destination of pubsub subscription is the userId
    if (root.get("callback").asText().equals("pubsub")) {
        String destination = IDM.random_auth_token(authToken);
        if (SearchEngine.getRepeatedSubscriptions(destination, soParent.getId(), "pubsub", streamId) > 0)
            throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "Duplicated pubsub subscription");

      ((ObjectNode)subsRoot).put("destination", destination);
    }
    else{
      ((ObjectNode)subsRoot).put("destination", root.get("destination").asText());
    }

    ((ObjectNode)subsRoot).put("stream", streamId);
    if (!root.path("customFields").isMissingNode())
      ((ObjectNode)subsRoot).put("customFields", root.get("customFields"));
    if (!root.path("delay").isMissingNode())
      ((ObjectNode)subsRoot).put("delay", root.get("delay").asInt());
    if (!root.path("expire").isMissingNode()) {
      ((ObjectNode)subsRoot).put("expire", root.get("expire").asInt());
    }

//    // Put the subscription id in the so stream subscription array
//    soParent.setSubscription(stream, subsId);
  }

  /**
   * @return the couchbase Key
   */
  public String getKey() {
    return subsKey;
  }

  /** Generate response to a Subscription creation
   *
   * @return
   */
  public String responseCreateSubs() {
    JsonNode root = mapper.createObjectNode();

    try {
      ((ObjectNode)root).put("id", subsId);
      ((ObjectNode)root).put("createdAt", subsRoot.get("createdAt").asLong());
    } catch (Exception e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return root.toString();
  }

  /**
   * @return String
   */
  public String responseGetSubs() {
      JsonNode root = subsRoot;
      
      ((ObjectNode)root).remove("userId");
      ((ObjectNode)root).remove("destination");
      ((ObjectNode)root).remove("stream");
      ((ObjectNode)root).remove("source");
      
      return root.toString();
  }

  /**
   * @return Subscription as String
   */
  public String getString() {
    return subsRoot.toString();
  }

  /**
   * @return the Service Object
   */
  public SO getSO() {
    return soParent;
  }

  /**
   * @return the Subscription Id
   */
  public String getId() {
    return subsId;
  }

}

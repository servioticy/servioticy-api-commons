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

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.datamodel.Data;
import com.servioticy.api.commons.elasticsearch.SearchEngine;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class Group {
  private static ObjectMapper mapper = new ObjectMapper();

  private String streamId, groupId;
  private ArrayList<String> soIds;

  /** Create a Group from a JsonNode document.
   *
   * @param root The JsonNode Root
   */
  public Group(final JsonNode root) {
    try {
      // Check if exist stream
      if (root.path("stream").isMissingNode())
        throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The stream field was not found");
      // Check if exist soIds
      if (root.path("soIds").isMissingNode())
        throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The soIds field was not found");

      streamId = ((ObjectNode) root).get("stream").asText();
      try {
        soIds = mapper.readValue(mapper.writeValueAsString(root.get("soIds")), new TypeReference<ArrayList<String>>() {});
      } catch (JsonProcessingException e) {
        throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "soIds has to be an array of Strings");
      }

    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "IOException");
    }

  }

  /** Create a Group form a String.
   *
   * @param body
   * @throws JsonProcessingException
   * @throws IOException
   */
  public Group(String body) throws JsonProcessingException, IOException {
      this(mapper.readTree(body));
  }

  /** Create a group with a groupId - to support CSOs
   * @param root
   * @param groupId
   */
  public Group(JsonNode root, String groupId) {
    this(root);
    this.groupId = groupId;
  }

  /**
   * @return The lastUpdate value of the streamId for all the soIds
   */
  public String lastUpdate() {
    CouchBase cb = new CouchBase();
    Data data = null;
    //ObjectNode lastUpdate = mapper.createObjectNode();
    //ObjectNode nextLastUpdate = mapper.createObjectNode();
    //SO so;

    /*for (String soId : soIds) {
      so = cb.getSO(soId);
      if (so == null)
        continue;
      try {
        data = cb.getData(so, streamId);
      } catch (Exception e) {
        continue;
      }
      if (data == null)
        continue;
      nextLastUpdate = (ObjectNode)data.lastUpdate();
      if (lastUpdate.path("lastUpdate").asLong() < nextLastUpdate.get("lastUpdate").asLong()) {
        lastUpdate = nextLastUpdate;
      }
    }*/

    String groupLastUpdateDataID = SearchEngine.getGropLastUpdateDocId(streamId,soIds);
    data = cb.getData(groupLastUpdateDataID);

    return data.getString();
  }

  /** Create subscriptions to destination for all the soIds
   *
   * @param destination
   */
  public void createSubscriptions(String destination) {
    CouchBase cb = new CouchBase();
    SO so;
    String body;

    if (this.groupId == null)
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);

    for (String soId : soIds) {
      so = cb.getSO(soId);

      body = "{ " + "\"callback\" : " + "\"internal\", \"destination\":  \"" + destination + "\", \"customFields\": { \"groupId\": \"" + groupId + "\" }" + " }";

      // Create Subscription
      Subscription subs = new Subscription(so, streamId, body);

      // Store in Couchbase
      cb.setSubscription(subs);
    }

  }

  /** Check Group - All soIds has to exist and to have the stream
   *
   */
  public void checkSoIdsStream() {
    CouchBase cb = new CouchBase();
    SO so;
    JsonNode stream;

    for (String soId : soIds) {
      so = cb.getSO(soId);
      if (so == null)
        throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "The Service Object: " + soId + " does not exist.");

      stream = so.getStream(streamId);
      if (stream == null) throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object: " + soId + " does not have this stream" );
    }
  }
  /**
   * @return groupId
   */
  public String getGroupId() {
    return groupId;
  }

}

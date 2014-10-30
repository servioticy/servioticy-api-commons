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
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.data.SO;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.mapper.ChannelsMapper;

public class Data {
  protected static ObjectMapper mapper = new ObjectMapper();
  private static Logger LOG = org.apache.log4j.Logger.getLogger(Data.class);

  private String dataKey;
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
      this.dataKey = dataId;
    } catch (Exception e) {
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
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
	  soParent = so;
	  JsonNode stream = soParent.getStream(streamId);

	  // Check if exists this streamId in the Service Object
	  if (stream == null)
		  throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "This Service Object does not have this stream.");

	  JsonNode root;
	  try {
		  root = mapper.readTree(body);

		  // Check if exists lastUpdate
		  if (root.path("lastUpdate").isMissingNode()) {
			  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "The lastUpdate field was not found");
		  } else {
			  if (!root.path("lastUpdate").isLong() && !root.path("lastUpdate").isInt()) {
				  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "The lastUpdate has to be a long type");
			  }
		  }

		  // Check channels
		  if (root.path("channels").isMissingNode()) {
			  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No channels");
		  } else {
			  ChannelsMapper.parsePutData(stream, root.get("channels"));
		  }

	  } catch (JsonProcessingException e) {
		  LOG.error(e);
		  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
	  } catch (IOException e) {
		  LOG.error(e);
		  throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
	  }

	  ((ObjectNode)dataRoot).putAll((ObjectNode)root);

	  dataKey= soParent.getId() + "-" + streamId + "-" + root.get("lastUpdate").asLong();

  }

  public void appendSecurity(JsonNode root) {
    ((ObjectNode)dataRoot).put("security", root);
  }

  public JsonNode getSecurity() {
    return dataRoot.get("security");
  }



  /** Generate response to last update of all data
   *
   * @return String
   */
  public String responseLastUpdate() {
    String response = "{ \"data\": [ " + dataRoot.toString() + " ] }";
    return response;
  }

  /** Generate response to getting all the Stream SO data
   *
   * @return String
   */
  public static String responseAllData(List<Data> updates) {
    StringBuilder res = new StringBuilder();
    res.append("{ \"data\": [");

    boolean first = true;
    for(Data update : updates) {
        if(first)
            first=false;
        else
            res.append(",");

        res.append(update.responseData());
    }

    res.append("]}");

    return res.toString();
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
	  if(dataRoot!=null)
		  return dataRoot.toString();
	  else
		  return "";
  }

  public String getLastUpdate() {
	  if(dataRoot!=null)
	      return dataRoot.path("lastUpdate").toString();
	  else
		  return "";
  }

  public String responseData() {
    JsonNode root = dataRoot;

    ((ObjectNode)root).remove("security");

    return root.toString();
  }

  /**
   * @return The Service Object data owner
   */
 /* public SO getSO() {
    return soParent;
  }*/



}

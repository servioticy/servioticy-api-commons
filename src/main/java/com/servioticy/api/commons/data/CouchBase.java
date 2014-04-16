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

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.core.Response;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.api.commons.datamodel.Data;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.utils.Config;

public class CouchBase {
  private static CouchbaseClient cli_so;
  private static CouchbaseClient cli_data;
  private static CouchbaseClient cli_subscriptions;
  private static CouchbaseClient cli_private;

  public CouchBase() {
    cli_so = Config.cli_so;
    cli_data = Config.cli_data;
    cli_subscriptions = Config.cli_subscriptions;
    cli_private = Config.cli_private;
  }

  /**
   * @param soId
   * @return
   */
  public SO getSO(String soId) {
    String storedSO = (String)cli_so.get(soId);
    if (storedSO != null) {
      return new SO(storedSO);
    }
    return null;
  }





  /**
   * @param so
   */
  public void setSO(SO so) {
    // TODO check to insert unique so_id
    try {
      // Asynchronous set
      OperationFuture<Boolean> setOp = cli_so.set(so.getSOKey(), 0, so.getString());
      if (!setOp.get().booleanValue()) {
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
      }
    } catch (InterruptedException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    } catch (ExecutionException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
  }

  /**
   * @param userId
   * @return all the Service Objects as String
   */
  public String getAllSOs(String userId) {
    ArrayList<String> sos = new ArrayList<String>();

    View view = cli_so.getView("user", "byUser");
    Query query = new Query();
    query.setStale(Stale.FALSE);
    ViewResponse result = cli_so.query(view, query);

    for(ViewRow row : result) {
      if (row.getKey() != null && row.getKey().equals(userId))
        sos.add(row.getValue());
    }

    ObjectMapper mapper = new ObjectMapper();
    String str_sos = null;
    try {
      str_sos = mapper.writeValueAsString(sos);
    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }

    return str_sos;
  }

  /**
   * @param subsId
   * @return
   */
  public Subscription getSubscription(String subsId) {
    String storedSubs = (String)cli_so.get(subsId);
    if (storedSubs != null) {
      return new Subscription(storedSubs);
    }
    return null;
  }

  /** Store the new subscriptions
   *
   * @param subs
   */
  public void setSubscription(Subscription subs) {
    try {
      OperationFuture<Boolean> setOp;
      setOp = cli_subscriptions.set(subs.getKey(), 0, subs.getString());
      if (!setOp.get().booleanValue()) {
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
      }

//      // Update the SO stream with the subscription
//      subs.getSO().update();
//      setOp = cli_so.set(subs.getSO().getSOKey(), 0, subs.getSO().getString());
//      if (!setOp.get().booleanValue()) {
//        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
//      }
    } catch (InterruptedException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    } catch (ExecutionException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
}

  /** Store new data
   *
   * @param data
   */
  public void setData(Data data) {
    try {
      OperationFuture<Boolean> setOp;
      setOp = cli_data.set(data.getKey(), 0, data.getString());
      if (!setOp.get().booleanValue()) {
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
      }

//      // TODO -> to maintain as Subscription (array) -> to improve
//      // Update the SO stream with the data
//      data.getSO().update();
//      setOp = cli_so.set(data.getSO().getSOKey(), 0, data.getSO().getString());
//      if (!setOp.get().booleanValue()) {
//        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
//      }
    } catch (InterruptedException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    } catch (ExecutionException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
  }


  /**
   * @param dataId
   * @return
   */
  public Data getData(String dataId) {
    String storedData = (String)cli_data.get(dataId);
    if (storedData != null) {
      return new Data(dataId,storedData);
    }
    return null;
  }

  /**
   * @param soId
   * @param streamId
   * @param timestamp
   * @return
   */
  public Data getData(String SoID, String streamId, long timestamp) {
    String dataId = SoID+"-"+streamId+"-"+timestamp;
    System.out.println("Searching for "+dataId);
    String storedData = (String)cli_data.get(dataId);
    if (storedData != null) {
      return new Data(dataId,storedData);
    }
    return null;
  }

  /**
   * @param userId
   * @param data_id
   * @return
   */
  public Data getData(SO so, String streamId) {

    JsonNode stream = so.getStream(streamId);
    if (stream == null) return null;

    if (stream.path("data").isMissingNode()) return null;
    String dataId = stream.get("data").asText();
//    String storedData = getJsonNode(dataId).toString();
    JsonNode storedJsonData = getJsonNode(dataId);
    String storedData = (storedJsonData != null) ? storedJsonData.toString() : null;

    if (storedData != null) {
      return new Data(dataId, storedData);
    }
    return null;
  }

  public void deleteData(String id) {
      cli_data.delete(id);
  }


  public void deleteSO(String id) {
      cli_so.delete(id);
  }


  /**
   * @param key
   * @return JsonNode that represents the stored document
   */
  public JsonNode getJsonNode(String key) {
    ObjectMapper mapper = new ObjectMapper();
//    JsonNode json = mapper.createObjectNode();
    JsonNode json;
    try {
      json = mapper.readTree((String)cli_so.get(key));
    } catch (NullPointerException e) {
      return null;
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }

    if (json != null) {
      return json;
    }
    return null;

  }

  /** Set the OpId control flow.
   *
   * @param key
   * @param exp -> expiration time
   */
  public void setOpId(String key, int exp) {
    // Do an asynchronous set
    OperationFuture<Boolean> setOp = cli_private.set(key, exp, "{}");
    // Check to see if our set succeeded
    try {
      if (!setOp.get().booleanValue())
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    } catch (InterruptedException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    } catch (ExecutionException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
  }

  /**
   * @param key
   * @return the OpId as String
   */
  public String getOpId(String key) {
    return (String)cli_private.get(key);
  }


}

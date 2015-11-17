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

import org.apache.log4j.Logger;

import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationStatus;

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
import com.servioticy.api.commons.elasticsearch.SearchEngine;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.utils.Config;

public class CouchBase {

  private static CouchbaseClient cli_so = Config.cli_so;
  private static CouchbaseClient cli_data = Config.cli_data;
  private static CouchbaseClient cli_subscriptions = Config.cli_subscriptions;
  //private static CouchbaseClient cli_private = Config.cli_private;
  private static CouchbaseClient cli_actuations = Config.cli_actuations;

  private static Logger LOG = org.apache.log4j.Logger.getLogger(CouchBase.class);

  /**
   * @param soId
   * @return
   */
  public static SO getSO(String soId) {
    String storedSO = (String)cli_so.get(soId);
    if (storedSO != null) {
      return new SO(storedSO);
    }
    return null;
  }

  /**
   * @param so
   */
  public static void setSO(SO so) {
    // TODO check to insert unique so_id
    try {
      // Asynchronous set
      OperationFuture<Boolean> setOp = cli_so.set(so.getSOKey(), 0, so.getString());
      if (!setOp.get().booleanValue()) {
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Error accessing CouchBase");
      }
    } catch (InterruptedException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (ExecutionException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (Exception e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * @return all the Service Objects as String
   */
  public static String getAllSOs() {
    ArrayList<String> sos = new ArrayList<String>();

    try {
      View view = cli_so.getView("index", "byIndex");
      Query query = new Query();
      query.setStale(Stale.FALSE);
      ViewResponse result = cli_so.query(view, query);
      for(ViewRow row : result) {
        if (row.getKey() != null)
          sos.add(row.getKey());
        }
    } catch (Exception e){
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Accessing the view: "+e.getMessage());
    }

    ObjectMapper mapper = new ObjectMapper();
    String str_sos = null;
    try {
      str_sos = mapper.writeValueAsString(sos);
    } catch (JsonProcessingException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    return str_sos;
  }

  
  
  /**
   * @param userId
   * @return all the Service Objects as String
   */
  public static String getAllSOs(String userId) {
    ArrayList<String> sos = new ArrayList<String>();


    try {
      View view = cli_so.getView("user", "byUser");
      Query query = new Query();
      query.setStale(Stale.FALSE);
      ViewResponse result = cli_so.query(view, query);
      for(ViewRow row : result) {
        if (row.getKey() != null && row.getKey().equals(userId))
          sos.add(row.getValue());
        }
    } catch (Exception e){
      LOG.error(e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Accessing the view: "+e.getMessage());
    }


    ObjectMapper mapper = new ObjectMapper();
    String str_sos = null;
    try {
      str_sos = mapper.writeValueAsString(sos);
    } catch (JsonProcessingException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    return str_sos;
  }

  /**
   * @param subsId
   * @return
   */
  public static Subscription getSubscription(String subsId) {
    // Get the Subscriptions
    //String subsKey = SearchEngine.getSusbcriptionDocId(subsId);
    //if (subsKey == null)
    //    return null;

    String storedSubs = (String)cli_subscriptions.get(subsId);
    if (storedSubs != null) {
      return new Subscription(storedSubs, subsId);
    }
    return null;
  }

  /** Store the new subscriptions
   *
   * @param subs
   */
  public static void setSubscription(Subscription subs) {
    try {
      OperationFuture<Boolean> setOp;
      setOp = cli_subscriptions.set(subs.getKey(), 0, subs.getString());
      if (!setOp.get().booleanValue()) {
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Error storing subscription document");
      }

//      // Update the SO stream with the subscription
//      subs.getSO().update();
//      setOp = cli_so.set(subs.getSO().getSOKey(), 0, subs.getSO().getString());
//      if (!setOp.get().booleanValue()) {
//        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
//      }
    } catch (InterruptedException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (ExecutionException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (Exception e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
}

  /** Store new data
   *
   * @param data
   */
 public static void setData(Data data) {
    OperationFuture<Boolean> setOp;
    OperationStatus status;
    int backoffexp = 0;
    int tries = 10;

    try {
      do {
        if (backoffexp > tries) {
                LOG.error("Could not perform a set in CB after "+ tries + " tries.");
                throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Error storing data in CouchBase");
        }
        setOp = cli_data.set(data.getKey(), 0, data.getString());
        status = setOp.getStatus();
        if (status.isSuccess()) {
              break;
        }

        if (backoffexp > 0) {
              double backoffMillis = Math.pow(2, backoffexp);
              backoffMillis = Math.min(1000, backoffMillis); // 1 sec max
              Thread.sleep((int) backoffMillis);
              LOG.info("CB set backing off, tries so far: " + backoffexp);
        }

        backoffexp++;

        if (!status.isSuccess()) {
            LOG.error("CB set failed with status: " + status.getMessage());
        }

      } while (status.getMessage().equals("Temporary failure"));
//      // TODO -> to maintain as Subscription (array) -> to improve
//      // Update the SO stream with the data
//      data.getSO().update();
//      setOp = cli_so.set(data.getSO().getSOKey(), 0, data.getSO().getString());
//      if (!setOp.get().booleanValue()) {
//        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
//      }
    } catch (InterruptedException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (Exception e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /** Store new actuation
  *
  * @param actuation
  */
 public static void setActuation(Actuation actuation) {
   try {
     OperationFuture<Boolean> setOp;
     setOp = cli_actuations.set(actuation.getId(), 0, actuation.getStatus());
     if (!setOp.get().booleanValue()) {
       throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Error storing actuation in CouchBase");
     }

//     // TODO -> to maintain as Subscription (array) -> to improve
//     // Update the SO stream with the data
//     data.getSO().update();
//     setOp = cli_so.set(data.getSO().getSOKey(), 0, data.getSO().getString());
//     if (!setOp.get().booleanValue()) {
//       throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
//     }
   } catch (InterruptedException e) {
	 LOG.error(e.getMessage() ,e);
     throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
   } catch (ExecutionException e) {
	 LOG.error(e.getMessage() ,e);
     throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
   } catch (Exception e) {
	 LOG.error(e.getMessage() ,e);
     throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
   }
 }

 /**
  * @param dataId
  * @return
  */
 public static Actuation getActuation(String actuationId) {
   String storedData = (String)cli_actuations.get(actuationId);
   if (storedData != null) {
     return Actuation.getFromJson(actuationId, storedData);
   }
   return null;
 }


  /**
   * @param dataId
   * @return
   */
  public static Data getData(String dataId) {
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
  public static Data getData(String soId, String streamId, long timestamp) {
    String dataId = soId + "-" + streamId + "-" + timestamp;
    System.out.println("Searching for " + dataId); // TODO [David] system.out.println???
    String storedData = (String)cli_data.get(dataId);
    if (storedData != null) {
      return new Data(dataId,storedData);
    }
    return null;
  }

  // TODO Deprecated??? [Juan Luis]
  /**
   * @param userId    // TODO [David] params????
   * @param data_id
   * @return
   */
  public static Data getData(SO so, String streamId) {

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

  public static void deleteData(String id) {
      cli_data.delete(id);
  }


  public static void deleteSO(String id) {
      cli_so.delete(id);
  }

  public static void deleteSubscription(String subsKey) {
    try {
      // Asynchronous delete
      OperationFuture<Boolean> deleteOp = cli_subscriptions.delete(subsKey);
      if (!deleteOp.get().booleanValue()) {
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Error deleting subscription from CouchBase");
      }
    } catch (InterruptedException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (ExecutionException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (Exception e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * @param key
   * @return JsonNode that represents the stored document
   */
  public static JsonNode getJsonNode(String key) {
    ObjectMapper mapper = new ObjectMapper();
//    JsonNode json = mapper.createObjectNode();
    JsonNode json;
    try {
      json = mapper.readTree((String)cli_so.get(key));
    } catch (NullPointerException e) {
      return null;
    } catch (Exception e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
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
 /* public static void setOpId(String key, int exp) {
    // Do an asynchronous set
    OperationFuture<Boolean> setOp = cli_private.set(key, exp, "{}");
    // Check to see if our set succeeded
    try {
      if (!setOp.get().booleanValue())
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Error storing OpID Document");
    } catch (InterruptedException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (ExecutionException e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (Exception e) {
      LOG.error(e.getMessage() ,e);
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }*/

  /**
   * @param key
   * @return the OpId as String
   */
  /*public static String getOpId(String key) {
    return (String)cli_private.get(key);
  }*/
}

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
	private static CouchbaseClient cpublic;
	private static CouchbaseClient cprivate;
	
	public CouchBase() {
		cpublic = Config.cpublic;
		cprivate = Config.cprivate;
	}
	
  /**
   * @param user_id
   * @param so_id
   * @return
   */
  public SO getSO(String user_id, String so_id) {
    String stored_so = (String)cpublic.get(user_id + "-" + so_id);
    if (stored_so != null) {
      return new SO(user_id, so_id, stored_so);
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
	    OperationFuture<Boolean> setOp = cpublic.set(so.getSOKey(), 0, so.getString());
	    if (!setOp.get().booleanValue()) {
	      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
	    }
	  } catch (InterruptedException e) {
	    throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
	  } catch (ExecutionException e) {
	    throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
	  } catch (Exception e) {
	    throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
	  }
	}
	
  public String getAllSOs(String user_id) {
    ArrayList<String> sos = new ArrayList<String>();
    
    View view = cpublic.getView("user", "byUser");
    Query query = new Query();
    query.setRangeStart(user_id).setStale(Stale.FALSE);
    ViewResponse result = cpublic.query(view, query);
    
    for(ViewRow row : result) {
      sos.add(row.getKey().substring(row.getKey().lastIndexOf("-")+1));
    }
    
    ObjectMapper mapper = new ObjectMapper();
    String str_sos = null;
    try {
      str_sos = mapper.writeValueAsString(sos);
    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }
    
    return str_sos;
  }
  
  /** Store the new subscriptions
   * 
   * @param subs
   */
  public void setSubscription(Subscription subs) {
    try {
      OperationFuture<Boolean> setOp;
      setOp = cpublic.set(subs.getKey(), 0, subs.getString());
      if (!setOp.get().booleanValue()) {
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
      }

      // Update the SO stream with the subscription
      subs.getSO().update();
      setOp = cpublic.set(subs.getSO().getSOKey(), 0, subs.getSO().getString());
      if (!setOp.get().booleanValue()) {
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
      }
    } catch (InterruptedException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    } catch (ExecutionException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }
}

  /** Store new data
   * 
   * @param data
   */
  public void setData(Data data) {
    try {
      OperationFuture<Boolean> setOp;
      setOp = cpublic.set(data.getKey(), 0, data.getString());
      if (!setOp.get().booleanValue()) {
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
      }

      // TODO -> to maintain as Subscription (array) -> to improve
      // Update the SO stream with the subscription
      data.getSO().update();
      setOp = cpublic.set(data.getSO().getSOKey(), 0, data.getSO().getString());
      if (!setOp.get().booleanValue()) {
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
      }
    } catch (InterruptedException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    } catch (ExecutionException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }
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
      json = mapper.readTree((String)cpublic.get(key));
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }
    if (json != null) {
      return json;
    }
    return null;
    
  }
  
  /** Set the OpId control flow
   * 
   * @param key
   * @param exp -> expiration time
   */
  public void setOpId(String key, int exp) {
    // Do an asynchronous set
    OperationFuture<Boolean> setOp = cprivate.set(key, exp, "{}");
    // Check to see if our set succeeded
    try {
      if (!setOp.get().booleanValue())
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    } catch (InterruptedException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    } catch (ExecutionException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }
  }

  /**
   * @param user_id
   * @param data_id
   * @return
   */
//  public Data getData(String user_id, String data_id) {
//    String stored_data = (String)client.get(user_id + "-" + data_id);
//    if (stored_data != null) {
//      return new Data(user_id, data_id, stored_data);
//    }
//    return null;
//  }
  

}

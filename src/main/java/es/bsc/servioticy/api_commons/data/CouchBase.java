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
package es.bsc.servioticy.api_commons.data;

import java.util.concurrent.ExecutionException;

import javax.ws.rs.core.Response;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;

import es.bsc.servioticy.api_commons.exceptions.ServIoTWebApplicationException;
import es.bsc.servioticy.api_commons.utils.Config;

public class CouchBase {
	private static CouchbaseClient client;
	
	public CouchBase() {
		client = Config.client;
	}
	
	/**
	 * @param so
	 */
	public void setSO(SO so) {
		// TODO check to insert unique so_id
	  try {
	    // Asynchronous set
	    OperationFuture<Boolean> setOp = client.set(so.getSOKey(), 0, so.getString());
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
   * @param user_id
   * @param so_id
   * @return
   */
  public SO getSO(String user_id, String so_id) {
    String stored_so = (String)client.get(user_id + "-" + so_id);
    if (stored_so != null) {
      return new SO(user_id, so_id, stored_so);
    }
    return null;
  }
}

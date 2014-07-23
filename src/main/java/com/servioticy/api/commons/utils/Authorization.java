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
package com.servioticy.api.commons.utils;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.api.commons.data.SO;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

import de.passau.uni.sec.compose.pdp.servioticy.LocalPDP;
import de.passau.uni.sec.compose.pdp.servioticy.PDP;
import de.passau.uni.sec.compose.pdp.servioticy.PermissionCacheObject;
import de.passau.uni.sec.compose.pdp.servioticy.exception.PDPServioticyException;

/*
 * Class to manage REST API Authorization
 */
public class Authorization {
//  private ResultSet rs;
  private String userId;
  private String autorizationToken;

  protected static ObjectMapper mapper = new ObjectMapper();

  public Authorization() {}

  public Authorization(MultivaluedMap<String, String> headerParams) {
    // Check if exists request header Authorization
    autorizationToken = headerParams.getFirst("Authorization");
    if (autorizationToken == null)
      throw new ServIoTWebApplicationException(Response.Status.FORBIDDEN, "Missing Authorization Header");
//
//    // Check if exists user with token api
//    try {
//      SQLite db = new SQLite();
//      rs = db.queryDB("select * from user where api_token = '" + autorizationToken + "'");
//      if (!rs.next())
//        throw new ServIoTWebApplicationException(Response.Status.FORBIDDEN, null);
//
//      // Obtain the uuid from the autorizationToken
//      rs = db.queryDB("select uuid from user where api_token = '" + autorizationToken + "'");
//      if (!rs.next()) {
//        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
//      }
//      userId = rs.getString("uuid");
//
//      db.close(); } catch (ClassNotFoundException e) {
//      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
//    } catch (SQLException e) {
//      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
//    }
  }

  public void checkAuthorization(SO so) {
	try {
	  PDP pdp = new LocalPDP();

	  pdp.setIdmHost(Config.idm_host);
	  pdp.setIdmPort(Config.idm_port);
	  pdp.setIdmUser(Config.idm_user);
	  pdp.setIdmPassword(Config.idm_password);

	  pdp.checkAuthorization(autorizationToken, so.getSecurity(), null, null,
			  PDP.operationID.SendDataToServiceObject);
	} catch (PDPServioticyException e) {
      throw new ServIoTWebApplicationException(Response.Status.fromStatusCode(e.getStatus()),
    		  e.getMessage());
	}
  }

  public JsonNode checkAuthorizationPutSU(SO so) {
	PDP pdp = new LocalPDP();

	pdp.setIdmHost(Config.idm_host);
	pdp.setIdmPort(Config.idm_port);
	pdp.setIdmUser(Config.idm_user);
	pdp.setIdmPassword(Config.idm_password);

	PermissionCacheObject pco = new PermissionCacheObject();
    try {
		pco = pdp.checkAuthorization(autorizationToken, so.getSecurity(), null, null, PDP.operationID.SendDataToServiceObjectProv);
	} catch (PDPServioticyException e) {
      throw new ServIoTWebApplicationException(Response.Status.fromStatusCode(e.getStatus()),
    		  e.getMessage());
	}

    JsonNode ret;
    try {
	  ret = mapper.readTree(pco.getCache().toString());
    } catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
    }
    return ret;
  }

  public PermissionCacheObject checkAuthorizationGetData(SO so, JsonNode su_secutiry, PermissionCacheObject pco) {
	PDP pdp = new LocalPDP();

	pdp.setIdmHost(Config.idm_host);
	pdp.setIdmPort(Config.idm_port);
	pdp.setIdmUser(Config.idm_user);
	pdp.setIdmPassword(Config.idm_password);

	try {
      return pdp.checkAuthorization(autorizationToken, so.getSecurity(), su_secutiry,
    		  pco, PDP.operationID.RetrieveServiceObjectData);
	} catch (PDPServioticyException e) {
      throw new ServIoTWebApplicationException(Response.Status.fromStatusCode(e.getStatus()),
    		  e.getMessage());
	}
  }

  public String getUserId() {
    return userId;
  }

}

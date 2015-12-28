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

import java.io.IOException;
import java.sql.SQLException;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    private String userId;
    private String authToken;

    protected static ObjectMapper mapper = new ObjectMapper();

    private static Logger LOG = org.apache.log4j.Logger.getLogger(Authorization.class);

    public Authorization() {
    }

    public Authorization(MultivaluedMap<String, String> headerParams) {
        // Check if exists request header Authorization
        authToken = headerParams.getFirst("Authorization");
        if (authToken == null)
            throw new ServIoTWebApplicationException(Response.Status.FORBIDDEN, "Missing Authorization Header");
        
        // Obtain the userId
        try {
            userId = Config.mySQL.getUserId(authToken);
            if (userId == null)
                throw new ServIoTWebApplicationException(Response.Status.FORBIDDEN, null);
        } catch (SQLException e) {
			LOG.error("SQLException: " + e.getMessage());
            throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public String getUserId() {
        return userId;
    }

    public String getAcces_Token() {
        return authToken;
    }

    public void checkAuthorization(SO so) {
        if (!so.getUserId().equals(userId) && !so.isPublic()) {
            throw new ServIoTWebApplicationException(Response.Status.UNAUTHORIZED, "Not authorized to access SO");
        }
    }
    
    public void checkOwner(SO so) {
        if (!so.getUserId().equals(userId)) {
            throw new ServIoTWebApplicationException(Response.Status.UNAUTHORIZED, "Not authorized to access SO");
        }
    }

    public JsonNode checkAuthorizationPutSU(SO so, String streamId, String body) {
        JsonNode root;
        try {
            root = mapper.readTree(body);
        } catch (JsonProcessingException e) {
            LOG.error(e);
            throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
        } catch (IOException e) {
            LOG.error(e);
            throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        PDP pdp = new LocalPDP();

        pdp.setIdmHost(Config.idm_host);
        pdp.setIdmPort(Config.idm_port);
        pdp.setIdmUser(Config.idm_user);
        pdp.setIdmPassword(Config.idm_password);
        pdp.setServioticyPrivateHost(Config.encription_url);

        PermissionCacheObject pco = new PermissionCacheObject();
        try {
            pco = pdp.GenericSendDatatoServiceObjectProv(authToken, so.getSecurity(), null, null, streamId,
                    root);
        } catch (PDPServioticyException e) {
            throw new ServIoTWebApplicationException(Response.Status.fromStatusCode(e.getStatus()), e.getMessage());
        }

        JsonNode ret = pco.getDecryptedUpdate();
        ((ObjectNode) ret).put("security", pco.getSecurityMetaData());

        return ret;
    }

    public PermissionCacheObject checkAuthorizationData(SO so, JsonNode su_secutiry, PermissionCacheObject pco,
            PDP.operationID opID) {
        PDP pdp = new LocalPDP();

        pdp.setIdmHost(Config.idm_host);
        pdp.setIdmPort(Config.idm_port);
        pdp.setIdmUser(Config.idm_user);
        pdp.setIdmPassword(Config.idm_password);

        try {
            return pdp.checkAuthorization(authToken, so.getSecurity(), su_secutiry, pco, opID);
        } catch (PDPServioticyException e) {
            throw new ServIoTWebApplicationException(Response.Status.fromStatusCode(e.getStatus()), e.getMessage());
        }
    }
}

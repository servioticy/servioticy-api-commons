package com.servioticy.api.commons.security;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import javax.ws.rs.core.Response;

import org.apache.http.client.methods.CloseableHttpResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.utils.Config;

import de.passau.uni.sec.compose.pdp.servioticy.exception.PDPServioticyException;
import de.passau.uni.sec.compose.pdp.servioticy.idm.IDMCommunicator;

public class IDM {

    // POST /IDM/SO/
    // Headers:
    // Authorization: Bearer {auth_token}

    public static JsonNode PostSO(String auth_token, String soId, boolean requires_token,
            boolean data_provenance_collection, boolean payment, String host, int port) {

        IDMCommunicator com = new IDMCommunicator(Config.idm_user, Config.idm_password, host, port);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.createObjectNode();

        // Build json payload
        ((ObjectNode) root).put("authorization", "Bearer " + auth_token);
        ((ObjectNode) root).put("id", soId);
        ((ObjectNode) root).put("requires_token", requires_token);
        ((ObjectNode) root).put("data_provenance_collection", data_provenance_collection);
        ((ObjectNode) root).put("payment", payment);

        CloseableHttpResponse response;
        try {
            response = com.sendPostToIDM("/idm/serviceobject/", root.toString());
            int statusCode = response.getStatusLine().getStatusCode();

            BufferedReader rd;
            StringBuffer result = new StringBuffer();
            try {
                rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                String line = "";
                while ((line = rd.readLine()) != null) {
                    result.append(line);
                }

                root = mapper.readTree(result.toString());
            } catch (Exception e) {
                throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
            }

            if (statusCode == 409)
                throw new ServIoTWebApplicationException(Response.Status.CONFLICT, root.get("error").asText());
            if (statusCode == 401)
                throw new ServIoTWebApplicationException(Response.Status.UNAUTHORIZED, root.get("error").asText());
            // throw new
            // ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
            // "");
            if (statusCode == 403)
                throw new ServIoTWebApplicationException(Response.Status.FORBIDDEN, root.get("error").asText());
            if (statusCode == 201)
                return root;

        } catch (PDPServioticyException e1) {
            System.out.println("User message: " + e1.getMessage() + " Status: " + e1.getStatus() + " Logging info: "
                    + e1.getLogInfo());
            throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
        }

        return null;
    }

    public static void DeleteSO(String auth_token, String soId, String host, int port) {

        IDMCommunicator com = new IDMCommunicator(Config.idm_user, Config.idm_password, host, port);

        String access_token_user = "Bearer " + auth_token;
        try {
            // add the bearer key word.
            com.deleteSO("http", host, port, soId, access_token_user);
        } catch (PDPServioticyException e) {
            int statusCode = e.getStatus();
            // here code could be 401 or 403 or 404 and there is always a user
            // message...
            if (statusCode == 401)
                throw new ServIoTWebApplicationException(Response.Status.UNAUTHORIZED, e.getMessage());
            if (statusCode == 403)
                throw new ServIoTWebApplicationException(Response.Status.FORBIDDEN, e.getMessage());
            // if (statusCode == 404) POSSIBLE LOGGING
            // TODO handle:
            // System.out.println("User message: "+e.getMessage()+" Status:
            // "+e.getStatus()+" Logging info: "+e.getLogInfo());
        }

    }

    public static String random_auth_token(String accessToken) {
        ObjectMapper mapper = new ObjectMapper();

        IDMCommunicator com = new IDMCommunicator(Config.idm_user, Config.idm_password, Config.idm_host,
                Config.idm_port);

        String response = null;
        try {
            response = com.getInformationForUser(accessToken);
        } catch (PDPServioticyException e) {
            throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);
        }

        JsonNode root;
        try {
            root = mapper.readTree(response);
        } catch (Exception e) {
            throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        if (root.path("random_auth_token").isMissingNode())
            throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");

        return root.get("random_auth_token").asText();

    }

    // public static String getInformationForUser(String accessToken) {
    // IDMCommunicator com = new IDMCommunicator(Config.idm_user,
    // Config.idm_password,
    // Config.idm_host, Config.idm_port);
    //
    // String response = null;
    // try {
    // response = com.getInformationForUser(accessToken);
    // } catch (PDPServioticyException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // throw new
    // ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
    // null);
    // }
    //
    // System.out.println(response);
    //
    // return response;
    // }

}

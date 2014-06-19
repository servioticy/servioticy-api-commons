package com.servioticy.api.commons.security;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HTTP;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.utils.Config;


public class IDM {

//	POST /IDM/SO/
//	Headers:
//	Authorization: Bearer {auth_token}

	private static  HttpClient httpClient = new DefaultHttpClient();

	public static JsonNode PostSO(String auth_token,
								  String soId,
								  boolean requires_token,
								  boolean data_provenance_collection,
								  boolean payment,
								  String url) {

		HttpRequestBase httpMethod;
		StringEntity input;

		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.createObjectNode();

		// Build json payload
		((ObjectNode)root).put("authorization", "Bearer " + auth_token);
		((ObjectNode)root).put("id", soId);
		((ObjectNode)root).put("requires_token", requires_token);
		((ObjectNode)root).put("data_provenance_collection", data_provenance_collection);
		((ObjectNode)root).put("payment", payment);

		try {
			input = new StringEntity(root.toString());
		} catch (Exception e) {
			return null;
		}
		input.setContentType("application/json");
		HttpPost httpPost = new HttpPost(url + "/idm/serviceobject/");
		httpPost.setEntity(input);
		httpPost.setHeader(HTTP.CONTENT_TYPE, "application/json;charset=UTF-8");

		// Basic authorization -> TODO to improve
		// TODO to change talking with Juan David -> London hackathon
		String plainCreds = Config.idm_user + ":" + Config.idm_password;
        byte[] plainCredsBytes = plainCreds.getBytes();
        String base64Creds = DatatypeConverter.printBase64Binary(plainCredsBytes);
		httpPost.setHeader("Authorization", "Basic " + base64Creds);

		httpMethod = httpPost;

		HttpResponse response;
		try {
			response = httpClient.execute(httpMethod);
		} catch (Exception e) {
			return null;
		}

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
//			throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
		if (statusCode == 403)
			throw new ServIoTWebApplicationException(Response.Status.FORBIDDEN, root.get("error").asText());
		if (statusCode == 201)
			return root;

		return null;
	}


}

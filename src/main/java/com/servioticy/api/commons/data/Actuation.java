package com.servioticy.api.commons.data;

import java.io.IOException;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class Actuation {
	
	private static ObjectMapper mapper = new ObjectMapper();
	private SO soParent = null;
	private JsonNode actRoot = null;
	private String id =  UUID.randomUUID().toString().replaceAll("-", "");
	
	public static Actuation getFromJson(String Id, String storedData) {
		
		return new Actuation(Id, storedData);
		
	}
	
	
	public Actuation(SO so, String actuationName, String body) {
		try {
			soParent = so;
		    JsonNode actionObject = mapper.readTree(body);
		    actRoot = mapper.createObjectNode();
		    ObjectNode action = ((ObjectNode)actRoot).putObject("action");
		    ObjectNode currentStatus = ((ObjectNode)actRoot).putObject("currentStatus");
		    action.putAll((ObjectNode)actionObject);		    
		    currentStatus.put("status", "Submitted");
		    currentStatus.put("updatedAt", System.currentTimeMillis());		    
		} catch (Exception e) {
		      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);		      
		}
	}
	
	public Actuation(String Id, String storedData) {		
		try {
			actRoot = mapper.readTree(storedData);
			this.id = Id;
		} catch (Exception e) {
		      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);		      
		}
	}

	
	public void updateStatus(String newStatusIn) {
		try { 
			ObjectNode newStatus = (ObjectNode)mapper.readTree(newStatusIn);
			ObjectNode currentStatus = ((ObjectNode)actRoot).putObject("currentStatus");
			currentStatus.putAll(newStatus);
			currentStatus.put("updatedAt", System.currentTimeMillis());
		} catch (Exception e) {
		      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, null);		      
		}

	}
	
	
	public String getStatus() {
		if (actRoot.path("currentStatus").isMissingNode())
	        throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The lastUpdate field was not found");
		
		return actRoot.get("currentStatus").toString();
	}

	
	public String getId() {
		return id;
	}
		
	public String getParameters() {
		return null;
	}
}

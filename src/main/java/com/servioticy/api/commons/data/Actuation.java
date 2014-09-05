package com.servioticy.api.commons.data;

import java.util.UUID;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class Actuation {
	
	private static ObjectMapper mapper = new ObjectMapper();
	private JsonNode actRoot = mapper.createObjectNode();
	private String id =  UUID.randomUUID().toString().replaceAll("-", "");
	private static Logger LOG = org.apache.log4j.Logger.getLogger(Actuation.class);
	
	public static Actuation getFromJson(String Id, String storedData) {
		
		return new Actuation(Id, storedData);
		
	}
	
	public Actuation(SO so, String actuationName, String parametersString) {
		
			

			ObjectNode description = ((ObjectNode)actRoot).putObject("description");
			description.putAll((ObjectNode)so.getActuation(actuationName));
			
			ObjectNode currentStatus = ((ObjectNode)actRoot).putObject("currentStatus");		    		    
			currentStatus.put("status", "Submitted");
			currentStatus.put("updatedAt", System.currentTimeMillis());

			if(parametersString != null && !parametersString.isEmpty()) {				
				((ObjectNode)actRoot).put("parameters", parametersString);
				//putObject("parameters",);
				//ObjectNode parameters = ((ObjectNode)actRoot).putObject("parameters");
//				try {
//					parameters.putAll((ObjectNode)mapper.readTree(parametersString));
//				} catch (Exception e) {
//					throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
//							  "Actuation '"+actuationName+"' invoked with wrong formatted parameters: " + parametersString);
//				}
			} 

			System.out.println("Actuation: "+actRoot.toString());
		    
	}
	
	public Actuation(String Id, String storedData) {		
		try {
			actRoot = mapper.readTree(storedData);
			this.id = Id;
		} catch (Exception e) {
			  LOG.error(e);
		      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}

	
	public void updateStatus(String newStatusIn) {
		try { 
			ObjectNode currentStatus = ((ObjectNode)actRoot).putObject("currentStatus");
			currentStatus.put("status",newStatusIn);
			currentStatus.put("updatedAt", System.currentTimeMillis());
		} catch (Exception e) {
			  LOG.error(e);
		      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
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
	
	public String toString() {
		try {
			return mapper.writeValueAsString(actRoot);
		} catch (JsonProcessingException e) {
			throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "Malformed actRoot in Actuation");
		}
	}
}

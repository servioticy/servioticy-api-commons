package com.servioticy.api.commons.mapper;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.api.commons.data.Group;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class StreamsMapper {
	private static ObjectMapper mapper = new ObjectMapper();
	private static Logger LOG = org.apache.log4j.Logger.getLogger(StreamsMapper.class);

	public static JsonNode parse(JsonNode root) throws ServIoTWebApplicationException, IOException {
		Map<String, JsonNode> streams;
		try {
		  streams = mapper.readValue(root.traverse(), new TypeReference<Map<String, JsonNode>>() {});
		  for (Map.Entry<String, JsonNode> stream : streams.entrySet()) {
		    if (!stream.getValue().path("channels").isMissingNode()) {
			  ChannelsMapper.parseCreation(stream.getValue().get("channels"));
			} else {
			  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
					  "stream: " + stream.getKey() + " without channels");
			}
		  }
		} catch (JsonMappingException e) {
		  LOG.error(e);
  		  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
				  "Service Object with incorrect streams value: "+ e.getMessage());
		}

		return root;
	}

}

package com.servioticy.api.commons.mapper;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class ChannelsMapper {
	private static ObjectMapper mapper = new ObjectMapper();

	public static JsonNode parseCreation(JsonNode root) throws ServIoTWebApplicationException {
		if (!root.path("location").isMissingNode()){
			if (root.get("location").path("type").isMissingNode()) {
				throw new ServIoTWebApplicationException(
						Response.Status.BAD_REQUEST, "channel location without type field");
			} else {
				if (!root.get("location").get("type").asText().equals("geo_point")) {
					throw new ServIoTWebApplicationException(
							Response.Status.BAD_REQUEST, "location channel has to be geo_point type");
				}
			}

		}
		return root;
	}

	public static JsonNode parsePutData(JsonNode root) throws ServIoTWebApplicationException, JsonParseException, JsonMappingException, IOException {
		Map<String, JsonNode> channels;
		channels = mapper.readValue(root.traverse(), new TypeReference<Map<String, JsonNode>>() {});

		for (Map.Entry<String, JsonNode> channel : channels.entrySet()) {
			if (channel.getValue().path("current-value").isMissingNode()) {
				throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
						"channel " + channel.getKey() + " without current-value");
			} else {
				if (channel.getValue().get("current-value").isObject()) {
					throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
							"current-value cannot be an Object. channel: " + channel.getKey());
				}
			}
		}

		return root;
	}
}
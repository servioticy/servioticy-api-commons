package com.servioticy.api.commons.mapper;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class StreamsMapper {
	private static ObjectMapper mapper = new ObjectMapper();

	public static JsonNode parse(JsonNode root) throws ServIoTWebApplicationException, IOException {
		Map<String, JsonNode> streams;
		streams = mapper.readValue(root.traverse(), new TypeReference<Map<String, JsonNode>>() {});

		for (Map.Entry<String, JsonNode> stream : streams.entrySet()) {
			if (!stream.getValue().path("channels").isMissingNode()) {
//				((ObjectNode)root.path(stream.getKey()))
//					.put("channels", ChannelsMapper.parse(stream.getValue().get("channels")));
				ChannelsMapper.parseCreation(stream.getValue().get("channels"));
			}
		}

		return root;
	}

}

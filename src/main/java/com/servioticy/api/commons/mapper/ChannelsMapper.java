package com.servioticy.api.commons.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.elasticsearch.index.mapper.MapperParsingException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.utils.GeoPointFieldMapper;

import static com.servioticy.api.commons.exceptions.ExceptionHelper.detailedMessage;

public class ChannelsMapper {
	private static ObjectMapper mapper = new ObjectMapper();
    private static final String[] Types = new String[] {"string", "number", "boolean", "geo_point" };
    private static Logger LOG = org.apache.log4j.Logger.getLogger(ChannelsMapper.class);

	public static JsonNode parseCreation(JsonNode root) throws ServIoTWebApplicationException, IOException {

		Map<String, JsonNode> channels;
		try {
		  channels = mapper.readValue(root.traverse(), new TypeReference<Map<String, JsonNode>>() {});
		  for (Map.Entry<String, JsonNode> channel : channels.entrySet()) {
			  // Check channel has type
			  if (channel.getValue().path("type").isMissingNode()) {
         		throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
         				"channel " + channel + " without type info");
			  } else {
				// Enforcement type
			    if (Arrays.asList(Types).contains(channel.getValue().get("type").asText().toLowerCase()) == false) {
//			      System.out.println(channel.getValue().get("type"));
			      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
			    		  "Channel " + channel.getKey() + " with incorrect type: " + channel.getValue().get("type").asText());
			    }
			    // Enforcement location
  			    if (channel.getKey().equals("location")) {
				  if (!channel.getValue().get("type").asText().toLowerCase().equals("geo_point")) {
					throw new ServIoTWebApplicationException(
							Response.Status.BAD_REQUEST, "location channel has to be geo_point type");
				  }
  			    }
			  }
		  }
		} catch (JsonMappingException e) {
		  LOG.error(e);
  		  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
				  "Service Object with incorrect channels value: "+e.getMessage());
		}
		return root;
	}

	public static JsonNode parsePutData(JsonNode stream, JsonNode root) throws ServIoTWebApplicationException, JsonParseException, JsonMappingException, IOException {
		Map<String, JsonNode> channels;
		channels = mapper.readValue(root.traverse(), new TypeReference<Map<String, JsonNode>>() {});


		for (Map.Entry<String, JsonNode> channel : channels.entrySet()) {
		    // Check DB integrity -> TODO to delete when all the created data is checked
		    if (stream.path("channels").isMissingNode())
		      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
		    		  "The Service Object stream does not have channels stream stored");

		    // check existence of current-value
			if (channel.getValue().path("current-value").isMissingNode()) {
				throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
						"channel " + channel.getKey() + " without current-value");
			}
			// current-value not an Object
			if (channel.getValue().get("current-value").isObject()) {
			  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
					  "current-value cannot be an Object. channel: " + channel.getKey());
			}
			// check stream has the channel
			if (stream.get("channels").path(channel.getKey()).isMissingNode()) {
			  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
					  "The Service Object Stream does not have a channel: " + channel.getKey());
			}

			// parsing current-value
			String type = stream.get("channels").get(channel.getKey()).get("type").asText().toLowerCase();
			parseType(type, channel);
		}

		return root;
	}

	private static void parseType(String type, Map.Entry<String, JsonNode> channel) {
	  if (type.equals("string")) {
		if (!channel.getValue().get("current-value").isTextual())
		  throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
				  "The Service Object stream channel " + channel.getKey() + " data type does not match");

		} else if (type.equals("number")) {
        if (!channel.getValue().get("current-value").isNumber())
          throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
        		  "The Service Object stream channel " + channel.getKey() + " data type does not match");
	    } else if (type.equals("boolean")) {
	    if (!channel.getValue().get("current-value").isBoolean())
	      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
	    		  "The Service Object stream channel " + channel.getKey() + " data type does not match");
		} else if (type.equals("geo_point")) {
		  try {
		    GeoPointFieldMapper.parse(channel.getValue().get("current-value"));
		  } catch (IOException e) {
			LOG.error(e);
		    throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
		  } catch (MapperParsingException e) {
			LOG.error(e);
			throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, detailedMessage(e));
		  } catch (Exception e) {
			LOG.error(e);
		    throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
		  }
		}
	  }
	}
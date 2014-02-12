package es.bsc.servioticy.api_commons.data;

import java.io.IOException;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import es.bsc.servioticy.api_commons.exceptions.ServIoTWebApplicationException;

public class Subscription {
  private static ObjectMapper mapper = new ObjectMapper();

  private UUID uuid;
  private String subsKey, subsId;
  private SO so;
  private JsonNode subs_root = mapper.createObjectNode();

  /** Create a Subscription
   * 
   * @param user_id
   * @param soId
   * @param streamId
   * @param body
   */
  public Subscription(String user_id, String soId, String streamId, String body) {
    JsonNode root;

    try {
      root = mapper.readTree(body);
    } catch (JsonProcessingException e) {
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, e.getMessage());
    } catch (IOException e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "");
    }

		CouchBase cb = new CouchBase();
		
		// Check if exists soId in the database
		so = cb.getSO(user_id, soId);
		if (so == null)
			throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

		JsonNode stream = so.getStream(streamId);

		// Check if exists this streamId in the Service Object
		if (stream == null)
			throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "This Service Object does not have this stream.");

		// Check if exists callback field in body request
		if (root.path("callback").isMissingNode())
			throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No callback in the request");
		
		// Check if exists destination field in body request
		if (root.path("destination").isMissingNode())
			throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No destination in the request");
		
		// OK, create the subscription
		
		// servioticy key = user_uuid + "-" + subs_uuid
		// TODO improve key and subs_id generation
		uuid = UUID.randomUUID(); //UUID java library
		
		subsId= String.valueOf(System.currentTimeMillis()) + uuid.toString().replaceAll("-", "");
		subsKey= user_id + "-" + subsId;

		((ObjectNode)subs_root).put("id", subsId);
		long time = System.currentTimeMillis();
		((ObjectNode)subs_root).put("createdAt", time);
		((ObjectNode)subs_root).put("updatedAt", time);
		((ObjectNode)subs_root).put("callback", root.get("callback").asText());
		((ObjectNode)subs_root).put("source", soId);
		((ObjectNode)subs_root).put("destination", root.get("destination").asText());
	 	((ObjectNode)subs_root).put("stream", streamId);
		if (!root.path("customFields").isMissingNode())
		  ((ObjectNode)subs_root).put("customFields", root.get("customFields"));
		if (!root.path("delay").isMissingNode())
			((ObjectNode)subs_root).put("delay", root.get("delay").asInt());
		if (!root.path("expire").isMissingNode()) {
			((ObjectNode)subs_root).put("expire", root.get("expire").asInt());
		}
		
		// Put the subscription id in the so stream subscription array
		so.setSubscription(stream, subsId);

  }
  
  /**
   * @return the couchbase Key
   */
  public String getKey() {
    return subsKey;
  }
  
  /**
   * @return Subscription as String
   */
  public String getString() {
    return subs_root.toString();
  }

  /**
   * @return the Service Object
   */
  public SO getSO() {
    return so;
  }
  
  /**
   * @return the Subscription Id
   */
  public String getId() {
    return subsId;
  }

}

package com.servioticy.api.commons.mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;

public class ActionsMapper {

    public static JsonNode parse(JsonNode root) throws ServIoTWebApplicationException, IOException {
        if (!root.isArray()) {
            throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
                    "Actions has to be an array");
        }

        Set<String> s = new HashSet<String>();

        // Check correct names (exist, no blank, no repeated
        for (JsonNode action : root) {
            if (action.path("name").isMissingNode())
                throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "action without name");
            if (action.get("name").asText().isEmpty())
                throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "action with blank name");
            
            if (!s.add(action.get("name").asText()))
                throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST,
                        "There are actions with the same name");
        }

      return root;
    }

}

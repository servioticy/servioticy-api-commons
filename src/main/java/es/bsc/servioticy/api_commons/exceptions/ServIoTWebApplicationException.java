/*******************************************************************************
 * Copyright 2014 Barcelona Supercomputing Center (BSC)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package es.bsc.servioticy.api_commons.exceptions;

import java.util.Date;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class ServIoTWebApplicationException extends WebApplicationException{
	private static final long serialVersionUID = -7634740164511860950L;

	public ServIoTWebApplicationException(Response.Status status, String message) {
        super(Response.status(status)
        				.entity(new ErrorInfo(status.getStatusCode(), message))
        				.type(MediaType.APPLICATION_JSON)
        				.header("Date", new Date(System.currentTimeMillis()))
        				.header("Server", "api.servIoTicy")
        				.build());
    }

}

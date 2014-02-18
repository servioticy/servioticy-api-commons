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
package com.servioticy.api.commons.exceptions;

import javax.ws.rs.core.Response;

//@XmlRootElement
public class ErrorInfo {
	private int status;
	private String message;
	private String info;
	private String defaultInfo = "api.servIoTicy.com";
	
	public ErrorInfo() {}

	public ErrorInfo(int status, String message) {
		this.status = status;
		this.message = message;
		this.info = this.defaultInfo;
		
		customInfo(status);
	}
	
	// Custom error messages
	private void customInfo(int status){
		if (status == Response.Status.UNSUPPORTED_MEDIA_TYPE.getStatusCode()) {
			this.message = "Unsupported Media Type";
		} 
		else if (status == Response.Status.UNAUTHORIZED.getStatusCode()) {
			this.message = "Access denied, please check your credentials!";
		}
		else if (status == Response.Status.FORBIDDEN.getStatusCode()) {
			this.message = "Access was denied!";
		}
		else if (status == 405) { // 405 Method Not Allowed
			this.message = "Method not allowed. Check the documentation";
		}
		else if (status == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
			this.message = "Internal Server Error";
		}
	}
	
	public int getStatus() {
		return this.status;
	}
	
	public String getMessage() {
		return this.message;
	}
	
	public String getInfo() {
		return this.info;
	}
	
}

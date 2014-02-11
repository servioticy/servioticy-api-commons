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
package es.bsc.servioticy.api_commons.utils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

// [font] http://arnavawasthi.blogspot.co.uk/2011/09/writing-custom-filter-for-jersey.html
// [font] http://stackoverflow.com/questions/14287734/how-to-pass-data-from-containerrequestfilter-to-resource-in-jersey-framework

public class AuthorizationRequestFilter implements ContainerRequestFilter{
  @Context
  private transient HttpServletRequest servletRequest;

  @Override
  public ContainerRequest filter(ContainerRequest request) {

    // Check Authorization header
    Authorization aut = new Authorization();
    aut.checkAuthorization(request.getRequestHeaders());
    
    this.servletRequest.setAttribute("user_id", aut.getUserId());
    
    return request;
  }

}

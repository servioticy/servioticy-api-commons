/*******************************************************************************
 * Copyright 2016 Barcelona Supercomputing Center (BSC)
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.servioticy.api.commons.data;

import javax.ws.rs.core.Response;

import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.queueclient.QueueClient;
import com.servioticy.queueclient.QueueClientException;

public class Reputation {
	
	public static void setReputation(String soId, String streamId, String userId, String lastUpdate) {
		String reputationDoc =
				"{" +
					"\"src\": {" +
						"\"soid\": \""+ soId + "\"," +
						"\"streamid\": \"" + streamId + "\"" +
					"}," +
					"\"dest\": {" +
						"\"user_id\": \""+ userId + "\"" +
					"},"+
					"\"su\": " + lastUpdate +
				"}";
/*		// Now send root to Dispatcher
		QueueClient sqc; //soid, streamid, body
		try {
			sqc = QueueClient.factory("reputationq.xml");
			sqc.connect();

			boolean res = sqc.put(reputationDoc);

			if(!res){
				// TODO
//                    throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
//                            "Undefined error in SQueueClient ");
			}

			sqc.disconnect();

		} catch (QueueClientException e) {
			System.out.println("Found exception: "+e+"\nmessage: "+e.getMessage());
			throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
					"SQueueClientException " + e.getMessage());
		} catch (Exception e) {
			throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR,
					"Undefined error in SQueueClient");
		}
*/		
	}
}

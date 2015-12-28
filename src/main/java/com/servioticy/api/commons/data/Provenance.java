package com.servioticy.api.commons.data;

public class Provenance {

    public static void DeleteSO(String auth_token, String soId) {

/*        IDMCommunicator com = new IDMCommunicator(Config.idm_user, Config.idm_password, host, port);

        String access_token_user = "Bearer " + auth_token;
        try {
            // add the bearer key word.
            com.deleteSO("http", host, port, soId, access_token_user);
        } catch (PDPServioticyException e) {
            int statusCode = e.getStatus();
            // here code could be 401 or 403 or 404 and there is always a user
            // message...
            if (statusCode == 401)
                throw new ServIoTWebApplicationException(Response.Status.UNAUTHORIZED, e.getMessage());
            if (statusCode == 403)
                throw new ServIoTWebApplicationException(Response.Status.FORBIDDEN, e.getMessage());
            // if (statusCode == 404) POSSIBLE LOGGING
            // TODO handle:
            // System.out.println("User message: "+e.getMessage()+" Status:
            // "+e.getStatus()+" Logging info: "+e.getLogInfo());
        }
*/
    }

}

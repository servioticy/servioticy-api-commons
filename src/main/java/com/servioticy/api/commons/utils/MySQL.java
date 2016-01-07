package com.servioticy.api.commons.utils;

import java.sql.SQLException;
import java.util.UUID;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQL {

    private static Logger LOG = org.apache.log4j.Logger.getLogger(Config.class);

    public Connection conn = null;
    private ResultSet resultSet = null;
    private PreparedStatement preparedStatement = null;
    
    public MySQL(String identity_host, String identity_port, String identity_db, String identity_user, String identity_pass) throws SQLException {

        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }

        Connection conn = DriverManager.getConnection("jdbc:mysql://" + identity_host + ":" +
                                                      identity_port + "/" + identity_db + "?" +
                                                      "user=" + identity_user + "&password=" + identity_pass);
        this.conn = conn;
    }

    public String portalGetToken(String userMail) throws SQLException {
        String authToken = null;
        String userId = null;

        try {
            preparedStatement = conn.prepareStatement("SELECT * FROM User WHERE user_mail = ?");
            preparedStatement.setString(1, userMail);
            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                authToken = resultSet.getString("auth_token");
            } else {
                UUID uuid = UUID.randomUUID();
                userId    = String.valueOf(uuid.toString().replaceAll("-", ""));

                uuid = UUID.randomUUID();
                authToken = String.valueOf(uuid.toString().replaceAll("-", ""));
                
                // Insert the user
                preparedStatement = conn.prepareStatement("INSERT INTO User" +
                                                                 "(id, auth_token, user_mail, lastModified) VALUES" +
                                                                 "(?, ?, ?, ?)");
                preparedStatement.setString(1, userId);
                preparedStatement.setString(2, authToken);
                preparedStatement.setString(3, userMail);
                preparedStatement.setDate(4, new Date(System.currentTimeMillis()));
                preparedStatement.executeUpdate();
            }

        } finally {
            close();
        }

        return authToken;
    }

    public String userIdGetToken(String userId) throws SQLException {
        String authToken = null;

        try {
            preparedStatement = conn.prepareStatement("SELECT * FROM User WHERE id = ?");
            preparedStatement.setString(1, userId);
            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                authToken = resultSet.getString("auth_token");
            } 

        } finally {
            close();
        }

        return authToken;
    }

    public String getUserId(String authToken) throws SQLException {
        String userId = null;

        try {
            preparedStatement = conn.prepareStatement("SELECT * FROM User WHERE auth_token = ?");
            preparedStatement.setString(1, authToken);
            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                userId = resultSet.getString("id");
            }

        } finally {
            close();
        }

        return userId;
    }
    
    
    private void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        } catch (SQLException e) {
        }
    }

}

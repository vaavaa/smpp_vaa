package kz.smpp.utils;

import kz.smpp.mysql.MyDBConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AllUtils {

    public String getApplicationPath() {
        return System.getProperty("user.dir");
    }
    public String getSettings(String settingsName){
        String settingsValue=null;
        MyDBConnection mDBConnection = new MyDBConnection();
        try {
            String SQL_string = "SELECT value FROM smpp_settings WHERE name='"+settingsName+"'";
            ResultSet rs = mDBConnection.query(SQL_string);

            if (rs.next()) {
                settingsValue = rs.getString("value");
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }

        return settingsValue;
    }
    public void setOperativeActivity(String id_client, String status, String id_operation){
        String settingsValue=null;
        MyDBConnection mDBConnection = new MyDBConnection();
        try {
            String SQL_string = "INSERT INTO operative_activity" +
                    "(id_client, status, id_operation) " +
                    "VALUES ("+id_client+","+status+","+id_operation+")";
            mDBConnection.Update(SQL_string);

        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
    }


}

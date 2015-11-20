package kz.smpp.mysql;


import java.sql.Statement;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.*;


public class MyDBConnection {

    private static Connection myConnection;
    private Statement statement;

    public MyDBConnection() {
        init();
    }

    public void init(){

        try{

            Class.forName("com.mysql.jdbc.Driver");
            myConnection=DriverManager.getConnection(
                    "jdbc:mysql://localhost/smpp_clients","root", ""
            );
        }
        catch(Exception e){
            System.out.println("Failed to get connection");
            e.printStackTrace();
        }
    }


    public Connection getMyConnection(){
        return myConnection;
    }


    public void close(ResultSet rs){

        if(rs !=null){
            try{
                rs.close();
            }
            catch(Exception e){}

        }
    }

    public void close(java.sql.Statement stmt){

        if(stmt !=null){
            try{
                stmt.close();
            }
            catch(Exception e){}

        }
    }

    public void destroy(){

        if(myConnection !=null){

            try{
                myConnection.close();
            }
            catch(Exception e){}


        }
    }

    /**
     *
     * @param query String The query to be executed
     * @return a ResultSet object containing the results or null if not available
     * @throws SQLException
     */
    public ResultSet query(String query) throws SQLException{
        statement = myConnection.createStatement();
        ResultSet res = statement.executeQuery(query);
        return res;
    }
    /**
     * @desc Method to insert data to a table
     * @param insertQuery String The Insert query
     * @return boolean
     * @throws SQLException
     */
    public int insert(String insertQuery) throws SQLException {
        statement = myConnection.createStatement();
        int result = statement.executeUpdate(insertQuery);
        return result;
    }

}
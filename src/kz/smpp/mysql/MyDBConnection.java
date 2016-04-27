package kz.smpp.mysql;

import kz.smpp.rome.Feed;
import kz.smpp.rome.FeedMessage;
import kz.smpp.rome.RSSFeedParser;
import kz.smpp.utils.AllUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Statement;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


public class MyDBConnection {

    private static Connection myConnection;
    private PreparedStatement preparedStatement;
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(MyDBConnection.class);

    public MyDBConnection() {
        init();
    }

    public void init() {

        try {

            String db_connect_string = "jdbc:sqlserver://127.0.0.1:1433;databaseName=smpp_clients";
            DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
            myConnection = DriverManager.getConnection(db_connect_string,
                    "sa", "cdmu7htt");

            myConnection.setAutoCommit(false);
        } catch (Exception e) {
            System.out.println("Failed to get connection");
            e.printStackTrace();
        }
    }


    public Connection getMyConnection() {
        return myConnection;
    }

    public void destroy() {

        if (myConnection != null) {

            try {
                myConnection.close();
            } catch (Exception e) {
            }


        }
    }

    /**
     * @param query String The query to be executed
     * @return a ResultSet object containing the results or null if not available
     * @throws SQLException
     */
    public ResultSet query(String query) throws SQLException {
        preparedStatement = myConnection.prepareStatement(query);
        return preparedStatement.executeQuery();
    }

    /**
     * @param insertQuery String The Insert query
     * @return boolean
     * @throws SQLException
     * @desc Method to insert data to a table
     */
    public int Update(String insertQuery) throws SQLException {
        preparedStatement = myConnection.prepareStatement(insertQuery);
        int result = preparedStatement.executeUpdate();
        myConnection.commit();
        preparedStatement.close();
        preparedStatement = null;
        return result;
    }

    public boolean UpdateSystem_task(String SysQuery) throws SQLException {
        Statement statement = myConnection.createStatement();
        return statement.execute(SysQuery);
    }

    public String getSettings(String settingsName) {
        String settingsValue = null;
        try {
            String SQL_string = "SELECT value FROM smpp_settings WHERE name='" + settingsName + "'";
            ResultSet rs = query(SQL_string);
            if (rs.next()) {
                settingsValue = rs.getString("value");
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return settingsValue;
    }

    public void setSettings(String settingsName, String value) {
        String settingsValue = null;
        try {
            String SQL_string = "Update smpp_settings SET value = '" + value + "' WHERE name='" + settingsName + "'";
            Update(SQL_string);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public int getLastId() throws SQLException {
        int result = -1;
        String lastIdString = "SELECT @@IDENTITY as LIID";
        preparedStatement = myConnection.prepareStatement(lastIdString);
        ResultSet res = preparedStatement.executeQuery();
        if (res.next()) {
            result = res.getInt("LIID");
        }
        return result;
    }

    public client getClient(int id) {
        client l_client = new client();
        String sql_string = "SELECT * FROM clients WHERE id = " + id;
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                l_client.setId(rs.getInt("id"));
                l_client.setAddrs(rs.getLong("msisdn"));
                l_client.setStatus(rs.getInt("status"));
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return l_client;
    }

    public int lineCount(String select_date) {
        int iReturn = 0;
        String sql_string = "SELECT count(id_sms) as idSms FROM sms_line " +
                "WHERE (status = 0 or status = 4 or status = 2 or status = 3 or status = 5)" +
                " AND created_time > '" + select_date + "'";
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                iReturn = rs.getInt("idSms");
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            iReturn = 10;
        }
        return iReturn;
    }

    public int lineCountO(String select_date) {
        int iReturn = 0;
        String sql_string = "SELECT count(id_sms) as idSms FROM sms_line " +
                "WHERE status = 0" +
                " AND created_time > '" + select_date + "'";
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                iReturn = rs.getInt("idSms");
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            iReturn = 10;
        }
        return iReturn;
    }

    public int lineCountRequest(String select_date, int ServiceCode) {
        int iReturn = 0;
        String sql_string = "SELECT count(id_sms) as idSms FROM sms_line " +
                "WHERE status = " + ServiceCode +
                " AND created_time > '" + select_date + "'";
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                iReturn = rs.getInt("idSms");
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            iReturn = 10;
        }
        return iReturn;
    }

    public client getClient(long msisdn) {
        client l_client = new client();
        String sql_string = "SELECT * FROM clients WHERE msisdn = " + msisdn;
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                l_client.setId(rs.getInt("id"));
                l_client.setAddrs(rs.getLong("msisdn"));
                l_client.setStatus(rs.getInt("status"));
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return l_client;
    }

    public client setNewClient(long msisdn) {
        client l_client = new client();
        String sql_string = "SELECT * FROM clients WHERE msisdn= " + msisdn;
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                if (rs.getInt("status") != 0) {
                    sql_string = "UPDATE clients SET  status = 0 WHERE msisdn =" + msisdn;
                    this.Update(sql_string);
                }
                l_client.setStatus(0);
                l_client.setAddrs(msisdn);
                l_client.setId(rs.getInt("id"));
            } else {
                sql_string = "INSERT  INTO clients (msisdn, status) VALUES (" + msisdn + ", 0)";
                this.Update(sql_string);
                l_client.setId(this.getLastId());
                l_client.setAddrs(msisdn);
                l_client.setStatus(0);
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return l_client;
    }

    public LinkedList<ContentType> getClientsContentTypes(client l_client) {
        LinkedList<ContentType> lct = new LinkedList<>();
        String sql_string = "SELECT content_type.* FROM client_content_type left join content_type " +
                "on content_type.id = client_content_type.id_content_type WHERE client_content_type.status = 0 and client_content_type.id_client = " + l_client.getId();
        try {
            ResultSet rs = this.query(sql_string);
            while (rs.next()) {
                ContentType ct = new ContentType();
                ct.setId(rs.getInt("id"));
                ct.setName(rs.getString("name"));
                ct.setName_eng(rs.getString("name_eng"));
                ct.setTable_name(rs.getString("table_name"));
                ct.setService_code(rs.getString("service_code"));
                lct.add(ct);
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return lct;
    }

    public client setStopClient(long msisdn) {
        client l_client = new client();
        String sql_string = "SELECT * FROM clients WHERE msisdn= " + msisdn;
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                if (rs.getInt("status") != 0) {
                    sql_string = "UPDATE clients SET  status = -1 WHERE msisdn =" + msisdn;
                    this.Update(sql_string);
                } else {
                    l_client.setStatus(-1);
                    l_client.setAddrs(msisdn);
                    l_client.setId(rs.getInt("id"));
                }
            } else {
                sql_string = "INSERT INTO clients VALUES(null," + msisdn + ",-1)";
                this.Update(sql_string);
                l_client.setId(this.getLastId());
                l_client.setAddrs(msisdn);
                l_client.setStatus(-1);
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return l_client;
    }

    public LinkedList<ContentType> getClientsContentTypes(client l_client, ContentType contenttype) {
        LinkedList<ContentType> lct = new LinkedList<>();
        String sql_string = "SELECT content_type.* FROM client_content_type left join content_type " +
                "on content_type.id = client_content_type.id_content_type WHERE client_content_type.id_client = " + l_client.getId() + " " +
                " AND client_content_type.id_content_type = " + contenttype.getId();
        try {
            ResultSet rs = this.query(sql_string);
            while (rs.next()) {
                ContentType ct = new ContentType();
                ct.setId(rs.getInt("id"));
                ct.setName(rs.getString("name"));
                ct.setName_eng(rs.getString("name_eng"));
                ct.setTable_name(rs.getString("table_name"));
                lct.add(ct);
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return lct;
    }

    public List<client> getClientsFromContentType(int contentTypeCode, String date) {
        List<client> lct = new ArrayList<>();
        String sql_string = "SELECT id_client, msisdn, update_date FROM client_content_type left join clients " +
                "ON id_client=id WHERE clients.status= 0 AND id_content_type =" + contentTypeCode + " AND id_client NOT IN " +
                "(SELECT id_client FROM sms_line_main WHERE id_content_type = " + contentTypeCode + " AND date_send = '" + date + "')";
        try {
            ResultSet rs = this.query(sql_string);
            while (rs.next()) {
                client cl = new client();
                cl.setId(rs.getInt("id_client"));
                cl.setAddrs(rs.getLong("msisdn"));
                cl.setHelpDate(rs.getDate("update_date"));
                lct.add(cl);
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return lct;
    }

    public List<client> getClientsFromContentType(String date) {
        List<client> lct = new ArrayList<>();
        //Раз мы используем туже таблицу для анализа отправки сообщений, то  статусы отправленых сообщений мы сделали
        //101 и -101 в случае ошибки отправки.
        String sql_string = "SELECT id_client, msisdn, MIN(update_date) as updDte FROM clients left join client_content_type " +
                "ON id=id_client WHERE clients.status= 0 AND '" + date + "' >= DATEADD(mm,1,update_date) AND id_client NOT IN " +
                "(SELECT id_client FROM sms_line WHERE (status = 101 or status = -101) AND created_time between " +
                "DATEADD(mm,-1, '" + date + "') AND DATEADD(dd, 1, '" + date + "')) GROUP by id_client, msisdn";
        try {
            ResultSet rs = this.query(sql_string);
            while (rs.next()) {
                client cl = new client();
                cl.setId(rs.getInt("id_client"));
                cl.setAddrs(rs.getLong("msisdn"));
                cl.setHelpDate(rs.getDate("updDte"));
                lct.add(cl);
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return lct;
    }

    public List<client> getClientsFromContentTypeHidden(int contentTypeCode, String date) {
        List<client> lct = new ArrayList<>();
        String sql_string = "SELECT id_client, msisdn, update_date FROM client_content_type left join clients " +
                "ON id_client=id WHERE clients.status= 0 AND id_content_type =" + contentTypeCode + " AND update_date < DATEADD(dd,-3,GETDATE()) " +
                " AND id_client NOT IN " +
                "(SELECT id_client FROM sms_line_quiet WHERE id_content_type = " + contentTypeCode + " AND date_send = '" + date + "')";
        try {
            ResultSet rs = this.query(sql_string);
            while (rs.next()) {
                client cl = new client();
                cl.setId(rs.getInt("id_client"));
                cl.setAddrs(rs.getLong("msisdn"));
                cl.setHelpDate(rs.getDate("update_date"));
                lct.add(cl);
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return lct;
    }

    public List<ContentType> getAllContents() {
        List<ContentType> contentTypes = new ArrayList<>();
        String sql_string = "SELECT  id, name, table_name, name_eng from content_type";
        try {
            ResultSet rs = this.query(sql_string);
            while (rs.next()) {
                ContentType ct = new ContentType();
                ct.setId(rs.getInt("id"));
                ct.setName(rs.getString("name"));
                ct.setTable_name(rs.getString("table_name"));
                ct.setName_eng(rs.getString("name_eng"));
                contentTypes.add(ct);
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return contentTypes;
    }

    public SmsLine setSingleSMS(SmsLine smsLine, boolean Dfr) {

        String sql_string = "INSERT INTO sms_line(id_client, sms_body, status, transaction_id, rate) " +
                "VALUES (" + smsLine.getId_client() + ",'" + smsLine.getSms_body() + "'," + smsLine.getStatus() + ",'" + smsLine.getTransaction_id() + "', '" + smsLine.getRate() + "')";
        try {
            this.Update(sql_string);
            smsLine.setId_sms(getLastId());
            return smsLine;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return smsLine;
        }
    }

    public int setSingleSMS(SmsLine smsLine) {
        int ireturn = -1;
        String chk_sql_string = "SELECT id_client FROM sms_line_main WHERE id_content_type=" +
                smsLine.getStatus() + " AND date_send = '" + smsLine.getDate() + "' AND id_client = " + smsLine.getId_client();
        try {
            ResultSet rs = this.query(chk_sql_string);
            if (!rs.next()) {
                String sql_string = "INSERT INTO sms_line(id_client, sms_body, status, transaction_id, rate) " +
                        "VALUES (" + smsLine.getId_client() + ",'" + smsLine.getSms_body() + "'," + smsLine.getStatus() + ",'" + smsLine.getTransaction_id() + "', '" + smsLine.getRate() + "')";
                String sql_string1 = "INSERT INTO sms_line_main(id_client, id_content_type, date_send, rate) " +
                        "VALUES (" + smsLine.getId_client() + "," + smsLine.getStatus() + ",'" + smsLine.getDate() + "', " + smsLine.getRate() + ")";
                this.Update(sql_string);
                ireturn = getLastId();

                this.Update(sql_string1);
                return ireturn;
            } else {
                return ireturn;
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
            return ireturn;
        }
    }

    public boolean checkPayment(int ClientId, int conType, String date) {
        String sql_string = "SELECT TOP 1 id_client FROM sms_line_quiet WHERE " +
                "id_client = " + ClientId + " AND id_content_type = " + conType + " AND date_send <='" + date + "' " +
                "AND sms_line_quiet.status = 1";
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public boolean setUpdateSingleSMSHidden(SmsLine smsLine){
        String sql_string = "SELECT id_sms_line FROM sms_line_quiet WHERE " +
                "id_client = "+smsLine.getId_client() + " AND " +
                "id_content_type = " + smsLine.getRate() + " AND " +
                "date_send = '" + smsLine.getDate() + "'";
        try {
            ResultSet rs = this.query(sql_string);
            if (!rs.next()) {
                setSingleSMSHidden(smsLine);
                return true;
            } else {
                smsLine.setId_sms(rs.getInt("id_sms_line"));
                UpdateHiddenSMSLine(smsLine);
                return true;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }


    }

    public boolean setSingleSMSHidden(SmsLine smsLine) {
        String sql_string1 = "INSERT INTO sms_line_quiet( id_client, id_content_type, sum, status, date_send)" +
                " VALUES (" + smsLine.getId_client() + ", " + smsLine.getRate() + ", " + smsLine.getTransaction_id() + ", "
                + smsLine.getStatus() + ", '" + smsLine.getDate() + "')";
        try {
            this.Update(sql_string1);
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public boolean SetClientType(int id_client, int id_content_type) {
        String SQLString = "SELECT id_client FROM client_content_type WHERE id_client=" + id_client + " AND id_content_type = " + id_content_type;
        try {
            ResultSet rs = this.query(SQLString);
            if (rs.next()) {
                return true;
            } else {
                String sql_string = "INSERT INTO client_content_type(id_client, id_content_type, status) VALUES (" + id_client + "," + id_content_type + ",0)";
                this.Update(sql_string);
                return true;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }


    public boolean setSingleSMS(SmsLine smsLine, String sms_text) {
        String sql_string = "INSERT INTO sms_line(id_client, sms_body, status, transaction_id, id_service) " +
                "VALUES (" + smsLine.getId_client() + ",'"
                + smsLine.getSms_body() + "'," + smsLine.getStatus() + ",'" +
                smsLine.getTransaction_id() + "', "+smsLine.getServiceId()+")";
        try {
            this.Update(sql_string);
            sql_string = "INSERT INTO client_activity(id_client, activity_text)" +
                    " VALUES (" + smsLine.getId_client() + ",'" + sms_text + "')";
            this.Update(sql_string);
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public boolean setActivityLog(int client_id, String sms_text) {
        try {
            String sql_string = "INSERT INTO client_activity(id_client, activity_text)" +
                    " VALUES (" + client_id + ",'" + sms_text + "')";
            this.Update(sql_string);
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public SmsLine UpdateHiddenSMSLine(SmsLine smsLine) {
        String sql_string = "UPDATE sms_line_quiet" +
                " SET id_client=" + smsLine.getId_client() + "," +
                " id_content_type=" + smsLine.getRate() + "," +
                " status=" + smsLine.getStatus() + "," +
                " sum=" + smsLine.getTransaction_id() + ", " +
                " date_send='" + smsLine.getDate() + "'" +
                " WHERE id_sms_line=" + smsLine.getId_sms();
        try {
            this.Update(sql_string);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return smsLine;
    }

    public void MakeNewTarifLine(String date) {
        String sql_string = "UPDATE sms_line_quiet SET status=0  WHERE status=-1 and date_send='" + date + "'";
        try {
            this.Update(sql_string);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void setLastActivityTime(){
        Calendar calendar = Calendar.getInstance();
        setSettings("LastActivityTime",""+calendar.getTimeInMillis());
    }

    public List<SmsLine> getAllSingleHiddenSMS(String date) {
        List<SmsLine> lineList = new ArrayList<>();
        String sql_string = "SELECT top 300 id_sms_line, id_client, id_content_type, sum, " +
                " date_send FROM sms_line_quiet WHERE status = 0 AND date_send='" + date + "'";
        try {
            ResultSet rsm = this.query(sql_string);
            while (rsm.next()) {
                SmsLine sm = new SmsLine();
                sm.setId_sms(rsm.getInt("id_sms_line"));
                sm.setId_client(rsm.getInt("id_client"));
                sm.setRate("" + rsm.getInt("id_content_type"));
                sm.setStatus(-1);

                if (rsm.getInt("sum") == 0) sm.setTransaction_id(getSettings("service_sum"));
                else sm.setTransaction_id("" + rsm.getInt("sum"));

                sm.setDate(new SimpleDateFormat("yyyy-MM-dd").format(rsm.getDate("date_send")));
                lineList.add(sm);
            }
            rsm.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return lineList;
    }

    public SmsLine getSingleSMS(int sms_id) {
        String sql_string = "SELECT id_sms, id_client, sms_body, status, " +
                "transaction_id FROM sms_line WHERE id_sms=" + sms_id;
        SmsLine sm = new SmsLine();
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                sm.setId_sms(rs.getInt("id_sms"));
                sm.setId_client(rs.getInt("id_client"));
                sm.setStatus(rs.getInt("status"));
                sm.setTransaction_id(rs.getString("transaction_id"));
                sm.setSms_body(rs.getString("sms_body"));
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return sm;
    }

    public List<SmsLine> getSMSLine(int line_status) {
        List<SmsLine> smsLines = new ArrayList<>();
        String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        String sql_string = "SELECT id_sms, id_client, sms_body, status, " +
                "transaction_id, rate, id_service FROM sms_line WHERE status=" + line_status + " AND created_time > '" + date + "'";
        try {
            ResultSet rs = this.query(sql_string);
            while (rs.next()) {
                SmsLine sm = new SmsLine();
                sm.setId_sms(rs.getInt("id_sms"));
                sm.setId_client(rs.getInt("id_client"));
                sm.setStatus(rs.getInt("status"));
                sm.setTransaction_id(rs.getString("transaction_id"));
                sm.setSms_body(rs.getString("sms_body"));
                sm.setRate(rs.getString("rate"));
                sm.setServiceId(rs.getInt("id_service"));
                smsLines.add(sm);
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return smsLines;
    }

    public List<ActionClient> getClientsOperator() {
        List<ActionClient> Aclients = new ArrayList<>();
        String sql_string = "SELECT client_id,status,content_type,action_type_id FROM client_3200 WHERE status =0";
        try {
            ResultSet rs = this.query(sql_string);
            while (rs.next()) {
                ActionClient actionClient = new ActionClient();
                actionClient.setClientId(getClient(rs.getLong("client_id")).getId());
                actionClient.setActionStatus(rs.getInt("status"));
                actionClient.setContentType(rs.getInt("content_type"));
                actionClient.setActionType(rs.getInt("action_type_id"));
                Aclients.add(actionClient);
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return Aclients;
    }

    public void UpdateClientsOperator(long address, int err_code){
        String sql_string = "UPDATE client_3200 SET status = 1, err_code ="+err_code+"  WHERE client_id= "+address;
        try {
            this.Update(sql_string);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public boolean getFollowUpLine() {
        boolean result = false;
        String sql_string = "INSERT INTO sms_line(id_client, sms_body, status)" +
                " SELECT client_session_140.id_client, '" + this.getSettings("welcome_message_fail_session") + "', '0' FROM client_session_140 " +
                "WHERE GETDATE()> DATEADD(ss, 90, client_session_140.time)";
        try {
            this.Update(sql_string);
            result = true;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public boolean RemoveDeadSessions() {
        boolean result = false;
        String sql_string =
                "DELETE FROM client_session_140 WHERE GETDATE() > DATEADD(ss,90,client_session_140.time)";
        try {
            this.Update(sql_string);
            result = true;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public boolean RemoveClientContentType(int client_id, int id_type) {
        boolean result = false;
        String sql_string =
                "DELETE FROM client_content_type WHERE id_client = " + client_id + " and id_content_type="+ id_type;
        try {
            this.Update(sql_string);
            result = true;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;
    }


    public SmsLine UpdateSMSLine(SmsLine smsLine) {
        String sql_string = "UPDATE sms_line" +
                " SET id_client=" + smsLine.getId_client() + "," +
                " sms_body='" + smsLine.getSms_body() + "'," +
                " status=" + smsLine.getStatus() + "," +
                " transaction_id='" + smsLine.getTransaction_id() + "', " +
                " rate='" + smsLine.getRate() + "', " +
                " err_code='" + smsLine.getErr_code() + "'" +
                " WHERE id_sms=" + smsLine.getId_sms();
        try {
            this.Update(sql_string);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return smsLine;
    }

    public boolean RemoveClientsContentTypes(client l_client, ContentType contentType) {
        boolean result = false;

        String sql_string = "DELETE FROM client_content_type " +
                "WHERE  id_client = " + l_client.getId() + " AND id_content_type =" + contentType.getId() + "";
        try {
            this.Update(sql_string);
            result = true;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public boolean setNewClientsContentTypes(client l_client, ContentType contentType) {
        boolean result = false;

        String sql_string = "INSERT INTO client_content_type(id_client, id_content_type, status) " +
                "VALUES (" + l_client.getId() + "," + contentType.getId() + ",0)";
        try {
            this.Update(sql_string);
            result = true;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public ContentType getContentType(String table_name) {
        ContentType ct = new ContentType();
        String sql_string = "SELECT * FROM content_type WHERE table_name = '" + table_name + "'";
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                ct.setId(rs.getInt("id"));
                ct.setName(rs.getString("name"));
                ct.setName_eng(rs.getString("name_eng"));
                ct.setTable_name(rs.getString("table_name"));
                ct.setService_code(rs.getString("service_code"));
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return ct;
    }

    public ContentType getContentTypeById(int service_id) {
        ContentType ct = new ContentType();
        String sql_string = "SELECT * FROM content_type WHERE id = '" + service_id + "'";
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                ct.setId(rs.getInt("id"));
                ct.setName(rs.getString("name"));
                ct.setName_eng(rs.getString("name_eng"));
                ct.setTable_name(rs.getString("table_name"));
                ct.setService_code(rs.getString("service_code"));
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return ct;
    }
    public boolean RemoveServiceName(Long msisdn) {
        //Удаляем клиента из рассылки
        int idClient = getClient(msisdn).getId();
        String sql_string = "DELETE FROM client_content_type WHERE id_client = " + idClient;
        try {
            this.Update(sql_string);
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public String getServiceName(int id) {
        //Получаем доступные сервисы из настроек
        AllUtils settings = new AllUtils();
        String[] table_names;
        String serviceName = "";
        ContentType contentType;

        table_names = new String[Integer.parseInt(this.getSettings("ServicesCount"))];
        String services = this.getSettings("AvailableServices");
        String message_text = "";
        int i = 0;
        while (services.lastIndexOf(";") >= 0) {
            table_names[i] = services.substring(services.lastIndexOf(";") + 1, services.length());
            services = services.substring(0, services.lastIndexOf(";"));
            i++;
        }

        client clnt = getClient(id);
        LinkedList<ContentType> llct = getClientsContentTypes(clnt);
        if (llct.size() == Integer.parseInt(this.getSettings("ServicesCount"))) {
            serviceName = this.getSettings("AllServices");
        } else {
            for (int j = 0; j <= table_names.length - 1; j++) {
                for (ContentType ct : llct) {
                    if (ct.getTable_name().equals(table_names[j])) {
                        contentType = getContentType(table_names[j]);
                        if (serviceName.length() > 0) serviceName = serviceName + ", " + contentType.getName();
                        else serviceName = contentType.getName();
                    }
                }
            }
        }

        return serviceName;
    }

    public String SignServiceName(Long msisdn, String sms_text) {
        //Получаем доступные сервисы из настроек
        String[] table_names;
        table_names = new String[Integer.parseInt(this.getSettings("ServicesCount"))];
        String services = this.getSettings("AvailableServices");
        String message_text = "";
        int i = 0;
        while (services.lastIndexOf(";") >= 0) {
            table_names[i] = services.substring(services.lastIndexOf(";") + 1, services.length());
            services = services.substring(0, services.lastIndexOf(";"));
            i++;
        }

        ContentType contentType;
        contentType = getContentType("content_anecdot");
        client clnt = setNewClient(msisdn);
        LinkedList<ContentType> llct = getClientsContentTypes(clnt);

        String serviceName;
        if (llct.size() == Integer.parseInt(this.getSettings("ServicesCount"))) {
            serviceName = this.getSettings("AllServices");
        } else {
            if (llct.size() == 0) {
                contentType = getContentType(this.getSettings("FirstService"));
                setNewClientsContentTypes(clnt, contentType);
            } else {
                for (int j = 0; j <= table_names.length - 1; j++) {
                    for (ContentType ct : llct) {
                        if (ct.getTable_name().equals(table_names[j])) {
                            table_names[j] = "";
                            break;
                        }
                    }
                }
                for (String str : table_names) {
                    if (str.length() > 0) {
                        contentType = getContentType(str);
                        setNewClientsContentTypes(clnt, contentType);
                        break;
                    }
                }
            }
            serviceName = contentType.getName();
        }

        return serviceName;
    }

    public String getAnecdoteFromDate(String dte) {

        //Выбираем из контент тайпа на эту дату
        //Вставляем его в исходящие сообщения со статусом 2 - анекдот, по всем клиентам, кто подписался на анекдот
        //Отправляем. Смотрим когда клиент подписался на сервис, если текущая дата больше чем 3 дня то отправляем за деньги,
        //если нет, то отправляем пустым.
        //Результат отправки пишем в исходящие, в двух таблицах

        String sql_string = "SELECT top 1 value FROM content_anecdote WHERE _date='" + dte + "'";
        String vle = "";
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) vle = rs.getString("value");
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return vle;
    }

    public String getHoroscopeFromDate(String dte) {

        //Выбираем из контент тайпа на эту дату
        //Вставляем его в исходящие сообщения со статусом 2 - анекдот, по всем клиентам, кто подписался на анекдот
        //Отправляем. Смотрим когда клиент подписался на сервис, если текущая дата больше чем 3 дня то отправляем за деньги,
        //если нет, то отправляем пустым.
        //Результат отправки пишем в исходящие, в двух таблицах

        String sql_string = "SELECT top 1 value FROM content_ascendant WHERE _date='" + dte + "'";
        String vle = "";
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) vle = rs.getString("value");
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return vle;
    }

    public String getRateFromDate(Date dte) {
        //переводим дату в формат mySQL
        String date_string = new SimpleDateFormat("yyyy-MM-dd").format(dte);
        //Иницииуруем новый Calendar
        Calendar cal = Calendar.getInstance();
        //Загружаем в него переданую дату
        cal.setTime(dte);

        //Если этол суббота, то прибавим два дня к дате, потому что у нас дата есть только за понедельник
        if (cal.get(Calendar.DAY_OF_WEEK) == 7) {
            cal.add(Calendar.DATE, 2);
            date_string = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
        }
        //Если этол воскресенье, то прибавим один день  к дате, потому что у нас дата есть только за понедельник
        if (cal.get(Calendar.DAY_OF_WEEK) == 1) {
            cal.add(Calendar.DATE, 1);
            date_string = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
        }

        String sql_string = "SELECT top 1 currency FROM content_rate WHERE status = 0 and rate_date='" + date_string + "'";
        String vle = "";
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) vle = rs.getString("currency");
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return vle;
    }

    public String getMetcastFromDate(String dte) {
        String sql_string = "SELECT _text, city_name FROM content_metcast " +
                "left join city_directory on content_metcast.id_city = city_directory.id_city " +
                "WHERE content_metcast.status =0 AND _date = '" + dte + "'";
        String vle = "";
        try {
            ResultSet rs = this.query(sql_string);
            while (rs.next()) {
                vle = vle.concat(rs.getString("city_name") + " - " + rs.getString("_text") + "; ");
            }
            vle = vle.trim();
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return vle;
    }


    public boolean backupData(String crnt_hr, String crnt_date) {
        boolean status = false;
        try {
            String sql_step2 = "BACKUP DATABASE [smpp_clients] TO  DISK = N'E:\\smpp\\bkp\\smpp_clients_" + crnt_date + "_" + crnt_hr + ".bak',  DISK = N'C:\\SMPP\\backups\\smpp_clients_" + crnt_date + "_" + crnt_hr + ".bak' WITH NOFORMAT, INIT,  NAME = N'smpp_clients_Backup', SKIP, NOREWIND, NOUNLOAD,  STATS = 10";
            status = !this.UpdateSystem_task(sql_step2);
        } catch (Exception e) {
            log.error(e.toString() + "/" + e.getCause().toString());
            status = false;
        }
        return status;
    }

    public boolean metcast() {
        String StringToClear = this.getSettings("StringToClear");
        String BaseURL = this.getSettings("weather_link");
        try {
            String SQL_string = "SELECT city_get_arrg, id_city FROM city_directory WHERE status =0";
            ResultSet rs = this.query(SQL_string);
            //Держим только на 3 дня прогноз
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(System.currentTimeMillis());
            calendar.add(Calendar.DATE, 1);
            Date dat_plus3 = calendar.getTime();

            while (rs.next()) {
                String city_get_arrg = rs.getString("city_get_arrg");
                int id_city = rs.getInt("id_city");
                RSSFeedParser parser = new RSSFeedParser(BaseURL.concat(city_get_arrg));
                Feed feed = parser.readFeed();
                for (FeedMessage message : feed.getMessages()) {
                    String rate_date = parser.Convert_Date(message.getPubDate(), "EEE, dd MMM yyyy HH:mm:ss Z", "");

                    DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                    Date date = format.parse(rate_date);

                    if (dat_plus3.getTime() >= date.getTime()) {
                        SQL_string = "SELECT * FROM content_metcast WHERE _date = '" + rate_date
                                + "' AND id_city = " + id_city;
                        ResultSet rs_check = this.query(SQL_string);
                        if (rs_check.next()) {
                            SQL_string = "DELETE FROM content_metcast WHERE _date = '" + rate_date
                                    + "' AND id_city = " + id_city;
                            this.Update(SQL_string);
                        }
                        rs_check.close();
                        message.setDescription(message.getDescription().replaceAll("\\<[^>]*>", ""));
                        message.setDescription(message.getDescription().replaceAll(StringToClear, ""));
                        message.setDescription(message.getDescription().replaceAll("\\s+", " ").trim());
                        message.setDescription(message.getDescription().replace(" .. ", "..").trim());

                        SQL_string = "INSERT INTO content_metcast VALUES (5, " + id_city + ", '" + message.getDescription() + "', "
                                + "'" + rate_date + "', 0)";
                        this.Update(SQL_string);

                    }
                }
            }
            rs.close();
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        } catch (ParseException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public boolean rate() {
        RSSFeedParser parser = new RSSFeedParser(this.getSettings("rate_link"));
        Feed feed = parser.readFeed();
        String rate_value = "";
        String rate_date = "";
        try {
            for (FeedMessage message : feed.getMessages()) {
                rate_date = parser.Convert_Date(message.getPubDate(), "", "");

                String SQL_string = "SELECT * FROM content_rate WHERE rate_date = '" + rate_date
                        + "' AND currency = '" + message.getTitle() + "' AND status = -1";
                ResultSet rs = this.query(SQL_string);
                if (!rs.next()) {
                    SQL_string = "SELECT top 1 Rate FROM content_rate WHERE currency = '" + message.getTitle() + "' AND status = -1 ORDER BY rate_date DESC";
                    ResultSet rs_step = this.query(SQL_string);
                    if (rs_step.next()) {
                        float lastStep = rs_step.getFloat("Rate");
                        float currentStep = Float.parseFloat(message.getDescription());
                        float result = currentStep - lastStep;
                        if (result >= 0) message.setStep("+" + result);
                        else message.setStep("" + result);
                    } else message.setStep("+0");
                    int limit = message.getStep().length();
                    if (limit > 5) limit = 5;
                    //Склееиваем в одну строку все курсы
                    rate_value = rate_value.concat(" :: " + message.getTitle() + " " + message.getDescription() + " " + message.getStep().substring(0, limit));
                    if (!rs.next()) {
                        SQL_string = "INSERT INTO content_rate VALUES (3, '" + rate_date + "', '"
                                + message.getTitle() + "', " + message.getDescription() + ", '" + message.getStep().substring(0, limit) + "',-1)";
                        this.Update(SQL_string);
                    }
                    rs_step.close();
                }
                rs.close();
            }
            if (rate_value.length() > 0) {
                String SQL_string = "INSERT INTO content_rate VALUES (3, '" + rate_date + "', '"
                        + rate_value.concat(" ::").trim() + "',0, '0',0)";
                this.Update(SQL_string);
            }
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }

    }

    public boolean ascendant() {
        RSSFeedParser parser = new RSSFeedParser(this.getSettings("ascendent"));
        Feed feed = parser.readFeed();
        try {
            for (FeedMessage message : feed.getMessages()) {
                String rate_date = message.getTitle();

                String SQL_string = "SELECT * FROM content_ascendant WHERE _date = '" + rate_date + "'";
                ResultSet rs = this.query(SQL_string);
                if (!rs.next()) {
                    if (message.getDescription().length() > 255)
                        message.setDescription(message.getDescription().substring(0, 255));
                    SQL_string = "INSERT INTO content_ascendant VALUES (4, '" + rate_date + "', '"
                            + message.getDescription() + "')";
                    this.Update(SQL_string);
                }
                rs.close();
            }
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }


    public boolean wasClientTariff(int id_client) {
        String Sqlstring = "SELECT top 1 id_client FROM sms_line WHERE id_client=" + id_client + " AND " +
                "STATUS =-99 AND created_time> DATEADD(hh, -2, GETDATE())";
        boolean rVle;
        try {
            ResultSet rs = this.query(Sqlstring);
            rVle = rs.next();
            rs.close();
            //Если есть запись, то возвращеме труе
            //Если нет записи возвращеме фалсе
            return rVle;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public void MarkClientsInactive() {
        String SqlString = "Update clients " +
                "SET status = -1 " +
                "WHERE status <>-1 AND id in( " +
                "select id_client FROM sms_line_quiet WHERE status=1 GROUP BY id_client HAVING MAX(date_send) <= (DATE_ADD(CURDATE(), INTERVAL -30 DAY)))";
        try {
            this.Update(SqlString);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void MarkClientsActive() {
        String SqlString = "Update clients " +
                "SET status = 0 " +
                "WHERE status = -1";
        try {
            this.Update(SqlString);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    //KPIs sector
    public String BCR() {
        String sql_return = "";
        String SQL_string = "SELECT count(*) as succeed FROM sms_line WHERE (status = 1 and " +
                "(rate = '" + getSettings("tarif_1") + "' " +
                "or rate = '" + getSettings("tarif_2") + "' " +
                "or rate = '" + getSettings("tarif_3") + "' " +
                "or rate = '" + getSettings("tarif_4") + "'))";
        String SQL_string1 = "SELECT count(*) as all_recd FROM sms_line WHERE " +
                "(rate = '" + getSettings("tarif_1") + "' " +
                "or rate = '" + getSettings("tarif_2") + "' " +
                "or rate = '" + getSettings("tarif_3") + "' " +
                "or rate = '" + getSettings("tarif_4") + "')";
        int BCR = 0;
        try {
            ResultSet rs = this.query(SQL_string);
            ResultSet rs1 = this.query(SQL_string1);
            if (rs.next() && rs1.next()) {
                if (rs1.getInt("all_recd") > 0) {
                    BCR = (int) ((rs.getFloat("succeed") / rs1.getFloat("all_recd")) * 100);
                }
            }
            rs.close();
            rs1.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return "BCR: " + BCR;
    }
}

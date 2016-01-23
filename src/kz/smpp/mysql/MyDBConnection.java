package kz.smpp.mysql;


import com.cloudhopper.smpp.SmppSession;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Date;


public class MyDBConnection {

    private static Connection myConnection;
    private Statement statement;
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(MyDBConnection.class);

    public MyDBConnection() {
        init();
    }

    public void init(){

        try{

            Class.forName("com.mysql.jdbc.Driver");
            myConnection=DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1/smpp_clients?characterEncoding=utf8","root", ""
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
    public int Update(String insertQuery) throws SQLException {
        statement = myConnection.createStatement();
        int result = statement.executeUpdate(insertQuery);
        return result;
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

    public int getLastId() throws SQLException {
        int result = -1;
        String lastIdString = "SELECT LAST_INSERT_ID() as LIID";
        statement = myConnection.createStatement();
        ResultSet res = statement.executeQuery(lastIdString);
        if (res.next()){
            result = res.getInt("LIID");
        }
        return result;
    }

    public client getClient(int id){
        client l_client = new client();
        String sql_string = "SELECT * FROM clients WHERE id = "+ id;
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                l_client.setId(rs.getInt("id"));
                l_client.setAddrs(rs.getLong("msisdn"));
                l_client.setStatus(rs.getInt("status"));
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return l_client;
    }

    public client getClient(long msisdn){
        client l_client = new client();
        String sql_string = "SELECT * FROM clients WHERE msisdn = "+ msisdn;
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                l_client.setId(rs.getInt("id"));
                l_client.setAddrs(rs.getLong("msisdn"));
                l_client.setStatus(rs.getInt("status"));
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return l_client;
    }

    public client setNewClient(long msisdn, String sms_text){
        client l_client = new client();
        String sql_string = "SELECT * FROM clients WHERE msisdn= "+msisdn;
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                if (rs.getInt("status") != 0){
                    sql_string = "UPDATE clients SET  status = 0 WHERE msisdn ="+ msisdn;
                    this.Update(sql_string);
                }
                l_client.setStatus(0);
                l_client.setAddrs(msisdn);
                l_client.setId(rs.getInt("id"));
            }
            else{
                sql_string = "INSERT INTO clients VALUES(null,"+ msisdn+",0)";
                this.Update(sql_string);
                l_client.setId(this.getLastId());
                l_client.setAddrs(msisdn);
                l_client.setStatus(0);
            }
            sql_string = "INSERT INTO client_activity(id_client, activity_text)" +
                    " VALUES ("+l_client.getId()+",'"+sms_text+"')";
            this.Update(sql_string);
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return l_client;
    }

    public LinkedList<ContentType> getClientsContentTypes(client l_client) {
        LinkedList <ContentType> lct = new LinkedList<>();
        String sql_string = "SELECT content_type.* FROM client_content_type left join content_type " +
                "on content_type.id = client_content_type.id_content_type WHERE client_content_type.status = 0 and client_content_type.id_client = "+ l_client.getId();
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
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return lct;
    }

    public client setStopClient(long msisdn){
        client l_client = new client();
        String sql_string = "SELECT * FROM clients WHERE msisdn= "+msisdn;
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                if (rs.getInt("status") != 0){
                    sql_string = "UPDATE clients SET  status = -1 WHERE msisdn ="+ msisdn;
                    this.Update(sql_string);
                }
                else {
                    l_client.setStatus(-1);
                    l_client.setAddrs(msisdn);
                    l_client.setId(rs.getInt("id"));
                }
            }
            else{
                sql_string = "INSERT INTO clients VALUES(null,"+ msisdn+",-1)";
                this.Update(sql_string);
                l_client.setId(this.getLastId());
                l_client.setAddrs(msisdn);
                l_client.setStatus(-1);
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return l_client;
    }

    public LinkedList<ContentType> getClientsContentTypes(client l_client, ContentType contenttype) {
        LinkedList <ContentType> lct = new LinkedList<>();
        String sql_string = "SELECT content_type.* FROM client_content_type left join content_type " +
                "on content_type.id = client_content_type.id_content_type WHERE client_content_type.id_client = "+ l_client.getId() +" " +
                " AND client_content_type.id_content_type = "+contenttype.getId();
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
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return lct;
    }

    public boolean setSingleSMS(SmsLine smsLine, String sms_text) {
        String sql_string = "INSERT INTO sms_line(id_client, sms_body, status, transaction_id) " +
                "VALUES ("+smsLine.getId_client()+",'"+smsLine.getSms_body()+"',"+smsLine.getStatus()+",'"+smsLine.getTransaction_id() +"')";
        SmsLine sm = new SmsLine();
        try {
            this.Update(sql_string);
            sql_string = "INSERT INTO client_activity(id_client, activity_text)" +
                    " VALUES ("+smsLine.getId_client()+",'"+sms_text+"')";
            this.Update(sql_string);
            return true;
        }
        catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public SmsLine getSingleSMS(int sms_id) {
        String sql_string = "SELECT id_sms, id_client, sms_body, status, " +
                "transaction_id FROM sms_line WHERE id_sms="+sms_id;
        SmsLine sm = new SmsLine();
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next())  {
                sm.setId_sms(rs.getInt("id_sms"));
                sm.setId_client(rs.getInt("id_client"));
                sm.setStatus(rs.getInt("status"));
                sm.setTransaction_id(rs.getString("transaction_id"));
                sm.setSms_body(rs.getString("sms_body"));
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return sm;
    }
    public List<SmsLine> getSMSLine(int line_status) {
        List <SmsLine> smsLines = new ArrayList<>();
        String sql_string = "SELECT id_sms, id_client, sms_body, status, " +
                "transaction_id FROM sms_line WHERE status="+line_status;
        try {
            ResultSet rs = this.query(sql_string);
            while (rs.next()) {
                SmsLine sm = new SmsLine();
                sm.setId_sms(rs.getInt("id_sms"));
                sm.setId_client(rs.getInt("id_client"));
                sm.setStatus(rs.getInt("status"));
                sm.setTransaction_id(rs.getString("transaction_id"));
                sm.setSms_body(rs.getString("sms_body"));
                smsLines.add(sm);
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return smsLines;
    }

    public boolean getFollowUpLine() {
        boolean result = false;
        AllUtils settings = new AllUtils();
        String sql_string = "INSERT INTO sms_line(id_client, sms_body, status)"+
        " SELECT client_session_140.id_client, '"+this.getSettings("welcome_message_fail_session")+"', '0' FROM client_session_140 " +
                "WHERE now()>  DATE_ADD(client_session_140.time, INTERVAL 120 SECOND)";
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
        "DELETE FROM client_session_140 WHERE NOW() > DATE_ADD(client_session_140.time, INTERVAL 120 SECOND)";
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
                " SET id_client="+smsLine.getId_client()+","+
                " sms_body='"+smsLine.getSms_body()+"',"+
                " status="+smsLine.getStatus()+","+
                " transaction_id='"+smsLine.getTransaction_id()+"'" +
                " WHERE id_sms="+ smsLine.getId_sms();
        try{
            this.Update(sql_string);
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return smsLine;
    }
    public boolean RemoveClientsContentTypes(client l_client, ContentType contentType) {
        boolean result= false;

        String sql_string = "DELETE FROM client_content_type " +
                "WHERE  id_client = "+ l_client.getId() + " AND id_content_type ="+contentType.getId() +"";
        try {
            this.Update(sql_string);
            result = true;
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;
    }
    public boolean setNewClientsContentTypes(client l_client, ContentType contentType) {
        boolean result= false;

        String sql_string = "INSERT INTO client_content_type(id_client, id_content_type, status) " +
                "VALUES ("+ l_client.getId() + ","+contentType.getId() +",0)";
        try {
            this.Update(sql_string);
            result = true;
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public ContentType getContentType(int idtype){
        ContentType ct = new ContentType();
        String sql_string = "SELECT * FROM content_type WHERE id = "+ idtype;
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                ct.setId(rs.getInt("id"));
                ct.setName(rs.getString("name"));
                ct.setName_eng(rs.getString("name_eng"));
                ct.setTable_name(rs.getString("table_name"));
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return ct;
    }
    public ContentType getContentType(String table_name){
        ContentType ct = new ContentType();
        String sql_string = "SELECT * FROM content_type WHERE table_name = '"+ table_name+"'";
        try {
            ResultSet rs = this.query(sql_string);
            if (rs.next()) {
                ct.setId(rs.getInt("id"));
                ct.setName(rs.getString("name"));
                ct.setName_eng(rs.getString("name_eng"));
                ct.setTable_name(rs.getString("table_name"));
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return ct;
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

    public String RemoveServiceName(Long msisdn){
        //Получаем доступные сервисы из настроек
        AllUtils settings = new AllUtils();
        String[] table_names;
        table_names= new String[Integer.parseInt(this.getSettings("ServicesCount"))];
        String services =  this.getSettings("AvailableServices");
        String message_text="";
        String serviceName = "";
        int i = 0;
        while (services.lastIndexOf(";")>=0)
        {
            table_names[i]=services.substring(services.lastIndexOf(";")+1,services.length());
            services = services.substring(0,services.lastIndexOf(";"));
            i++;
        }
        client clnt = getClient(msisdn);
        LinkedList<ContentType> llct= getClientsContentTypes(clnt);
        //Ничего нет удалять.
        if (llct.size()!=0){
            for (int j = 0; j<=table_names.length-1;j++) {
                for (ContentType ct : llct) {
                    if (ct.getTable_name().equals(table_names[j])){
                        serviceName = ct.getName();
                        RemoveClientsContentTypes(clnt,ct);
                        break;
                    }

                }
            }
        }
        return serviceName;
    }


    public String SignServiceName(Long msisdn, String sms_text){
        //Получаем доступные сервисы из настроек
        AllUtils settings = new AllUtils();
        String[] table_names;
        table_names= new String[Integer.parseInt(this.getSettings("ServicesCount"))];
        String services =  this.getSettings("AvailableServices");
        String message_text="";
        int i = 0;
        while (services.lastIndexOf(";")>=0)
        {
            table_names[i]=services.substring(services.lastIndexOf(";")+1,services.length());
            services = services.substring(0,services.lastIndexOf(";"));
            i++;
        }

        ContentType contentType;
        contentType= getContentType("content_anecdot");
        client clnt = setNewClient(msisdn, sms_text);
        LinkedList<ContentType> llct= getClientsContentTypes(clnt);
        if (llct.size()==0){
           contentType = getContentType(this.getSettings("FirstService"));
           setNewClientsContentTypes(clnt, contentType);
        }
        else {
             for (int j = 0; j<=table_names.length-1;j++) {
                 for (ContentType ct : llct) {
                     if (ct.getTable_name().equals(table_names[j])){
                        table_names[j] = "";
                        break;
                     }

                 }
             }
             for (String str:table_names) {
                 if (str.length()>0) {
                    contentType = getContentType(str);
                    setNewClientsContentTypes(clnt, contentType);
                    break;
                 }
             }
        }

        String serviceName;
        if (contentType.getName().length()==0) serviceName = this.getSettings("AllServices");
        else serviceName = contentType.getName();

        return serviceName;
    }
    public void setHoroscopeLine(Date dte) {
        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");



    }

    public boolean backupData(String dumpExePath, String host, String port, String user, String password, String database, String backupPath) {
        boolean status = false;
        try {
            Process p = null;

            DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
            Date date =new Date(System.currentTimeMillis());
            String filepath = "backup(without_DB)-" + database + "-" + host + "-(" + dateFormat.format(date) + ").sql";

            String batchCommand = "";
            if (password != "") {
                //only backup the data not included create database
                batchCommand = dumpExePath + " -h " + host + " --port " + port + " -u " + user + " --password=" + password + " " + database + " -r \"" + backupPath + "" + filepath + "\"";
            } else {
                batchCommand = dumpExePath + " -h " + host + " --port " + port + " -u " + user + " " + database + " -r \"" + backupPath + "" + filepath + "\"";
            }

            Runtime runtime = Runtime.getRuntime();
            p = runtime.exec(batchCommand);
            int processComplete = p.waitFor();

            if (processComplete == 0) {
                status = true;
                log.info("Backup created successfully for without DB " + database + " in " + host + ":" + port);
            } else {
                status = false;
                log.info("Could not create the backup for without DB " + database + " in " + host + ":" + port);
            }

        } catch (IOException ioe) {
            log.error(ioe.toString() +"/"+ ioe.getCause().toString());
        } catch (Exception e) {
            log.error(e.toString()+"/"+ e.getCause().toString());
        }
        return status;
    }



    public boolean metcast(){
        AllUtils settings = new AllUtils();
        String StringToClear = this.getSettings("StringToClear");
        String BaseURL = this.getSettings("weather_link");
        try {
            String SQL_string = "SELECT city_get_arrg, id_city FROM city_directory";
            ResultSet rs = this.query(SQL_string);
            while(rs.next()) {
                String city_get_arrg =  rs.getString("city_get_arrg");
                int id_city =  rs.getInt("id_city");
                RSSFeedParser parser = new RSSFeedParser(BaseURL.concat(city_get_arrg));
                Feed feed = parser.readFeed();
                for (FeedMessage message : feed.getMessages()) {
                    String rate_date = parser.Convert_Date(message.getPubDate(), "EEE, dd MMM yyyy HH:mm:ss Z", "");

                    SQL_string ="SELECT * FROM content_metcast WHERE forecast_date = '"+ rate_date
                            + "' AND id_city = " + id_city;
                    ResultSet rs_check = this.query(SQL_string);
                    if (rs_check.next()) {
                        SQL_string = "DELETE FROM content_metcast WHERE forecast_date = '"+ rate_date
                                + "' AND id_city = " + id_city;
                        this.Update(SQL_string);
                    }
                    message.setDescription(message.getDescription().replaceAll("\\<[^>]*>",""));
                    message.setDescription(message.getDescription().replaceAll(StringToClear,""));

                    SQL_string = "INSERT INTO content_metcast VALUES (NULL, 5, '"+ rate_date +"', "
                            +id_city+", '"+ message.getDescription()+"')";
                    this.Update(SQL_string);

                }

            }
            rs.close();
            return true;
        }
        catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }
    public boolean rate(){
        RSSFeedParser parser = new RSSFeedParser(this.getSettings("rate_link"));
        Feed feed = parser.readFeed();
        try {
            for (FeedMessage message : feed.getMessages()) {
                String rate_date =  parser.Convert_Date(message.getPubDate(),"","");

                String SQL_string ="SELECT * FROM content_rate WHERE rate_date = '"+ rate_date
                        + "' AND currency = '" + message.getTitle()+"'";
                ResultSet rs = this.query(SQL_string);
                SQL_string = "SELECT Rate FROM content_rate WHERE currency = '" + message.getTitle()+"' ORDER BY rate_date DESC LIMIT 1";
                ResultSet rs_step = this.query(SQL_string);
                if (rs_step.next()) {
                    float lastStep =  rs_step.getFloat("Rate");
                    float currentStep = Float.parseFloat(message.getDescription());
                    float result = currentStep - lastStep;
                    if (result >= 0) message.setStep("+"+result);
                    else message.setStep(""+result);
                }
                else message.setStep("+0");
                int limit = message.getStep().length();
                if (limit>5) limit = 5;
                if (!rs.next()) {
                    SQL_string = "INSERT INTO content_rate VALUES (NULL, 3, '"+ rate_date +"', '"
                            +message.getTitle()+"', "+ message.getDescription()+", '"+message.getStep().substring(0,limit)+"')";
                    this.Update(SQL_string);
                }
            }
        return true;
        }
        catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }

    }
    public boolean ascendant(){
        RSSFeedParser parser = new RSSFeedParser(this.getSettings("ascendent"));
        Feed feed = parser.readFeed();
        try {
            for (FeedMessage message : feed.getMessages()) {
                String rate_date = message.getPubDate();

                String SQL_string ="SELECT * FROM content_ascendant WHERE created_date = '"+ rate_date + "'";
                ResultSet rs = this.query(SQL_string);
                if (!rs.next()) {
                    SQL_string = "INSERT INTO content_ascendant VALUES (NULL, 4, '"+ rate_date +"', '"
                            + message.getDescription()+"')";
                    this.Update(SQL_string);
                }
            }
            return true;
        }
        catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }
}

package kz.smpp.va;

import com.cloudhopper.commons.util.FileAlreadyExistsException;
import com.cloudhopper.commons.util.windowing.WindowFuture;
import com.cloudhopper.smpp.SmppBindType;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.type.LoggingOptions;
import com.cloudhopper.smpp.type.SmppInvalidArgumentException;
import kz.smpp.client.Client;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.rome.*;
import kz.smpp.utils.AllUtils;
import kz.smpp.jsoup.ParseHtml;
import org.ini4j.Ini;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class Main {

	public static org.slf4j.Logger log = LoggerFactory.getLogger(Main.class);
    static Client client;
    static ExecutorService pool;
    static AllUtils settings = new AllUtils();

	private static void log(WindowFuture<Integer, PduRequest, PduResponse> future) {
		SubmitSm req = (SubmitSm)future.getRequest();
		SubmitSmResp resp = (SubmitSmResp)future.getResponse();

		log.debug("Got response with MSG ID={} for APPID={}", resp.getMessageId(), req.getReferenceObject());
	}

	public static void main(String[] args) throws SmppInvalidArgumentException {
        //Просто держим приложение до того как его прервут любым словом
        Scanner terminalInput = new Scanner(System.in);
        String command ="";
        boolean running = true;
        while (running) {
            command = terminalInput.nextLine();
            switch (command){
                case "exit":
                    //Вышли
                    running = false;
                    break;
                case "start":
                    start_3200();
                    log.debug("Started");
                    break;
                case "stop":
                    if (client.getSession() != null){
                        client.stop();
                        pool.shutdownNow();
                        log.debug("Stopped");
                    }
                    break;
                case "get rate":
                    rate();
                    log.debug("Done. DB is updated with rate");
                    break;
                case "get metcast":
                    metcast();
                    log.debug("Done. DB is updated with metcast");
                    break;
                case "get anecdote":
                    ParseHtml phtml = new ParseHtml(settings.getSettings("anecdote"));
                    phtml.close();
                    log.debug("Done. DB is updated with anecdote");
                    break;
                case "get horoscope":
                    ascendant();
                    log.debug("Done. DB is updated with horoscope");
                    break;
                default:
                    break;
            }


            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void start_3200(){

            SmppSessionConfiguration sessionConfig = new SmppSessionConfiguration();

            sessionConfig.setType(SmppBindType.TRANSCEIVER);
            sessionConfig.setHost(settings.getSettings("ipadress"));
            sessionConfig.setPort(Integer.parseInt(settings.getSettings("port")));
            sessionConfig.setSystemId(settings.getSettings("partner_id"));
            sessionConfig.setPassword(settings.getSettings("partner_pws"));

            LoggingOptions loggingOptions = new LoggingOptions();
            sessionConfig.setLoggingOptions(loggingOptions);

            client = new Client(sessionConfig);
            client.setSessionHandler(new MySmppSessionHandler(client));
            pool = Executors.newFixedThreadPool(2);
            pool.submit(client);

            client.start();

            log.debug("Wait to bound");
            while (client.getSession() == null|| !client.getSession().isBound()) {
                if (client.getSession() != null) log.debug("Session is {}", client.getSession().isBound());
                else log.debug("Null session");
                try {TimeUnit.SECONDS.sleep(1);}
                catch (InterruptedException ex)
                {java.util.logging.Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);}
            }
    }

    public static void metcast(){
        String StringToClear = settings.getSettings("StringToClear");
        MyDBConnection mDBConnection = new MyDBConnection();
        String BaseURL = settings.getSettings("weather_link");
        try {
            String SQL_string = "SELECT city_get_arrg, id_city FROM city_directory";
            ResultSet rs = mDBConnection.query(SQL_string);
            while(rs.next()) {
                String city_get_arrg =  rs.getString("city_get_arrg");
                int id_city =  rs.getInt("id_city");
                RSSFeedParser parser = new RSSFeedParser(BaseURL.concat(city_get_arrg));
                Feed feed = parser.readFeed();
                for (FeedMessage message : feed.getMessages()) {
                    String rate_date = parser.Convert_Date(message.getPubDate().substring(0, 16), "EEE, dd MMM YYYY", "");

                    SQL_string ="SELECT * FROM content_metcast WHERE forecast_date = '"+ rate_date
                            + "' AND id_city = " + id_city;
                    ResultSet rs_check = mDBConnection.query(SQL_string);
                    if (rs_check.next()) {
                        SQL_string = "DELETE FROM content_metcast WHERE forecast_date = '"+ rate_date
                                + "' AND id_city = " + id_city;
                        mDBConnection.Update(SQL_string);
                    }
                    message.setDescription(message.getDescription().replaceAll("\\<[^>]*>",""));
                    message.setDescription(message.getDescription().replaceAll(StringToClear,""));

                    SQL_string = "INSERT INTO content_metcast VALUES (NULL, 5, '"+ rate_date +"', "
                            +id_city+", '"+ message.getDescription()+"')";
                    mDBConnection.Update(SQL_string);

                }

            }
            rs.close();
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
    public static void rate(){
        RSSFeedParser parser = new RSSFeedParser(settings.getSettings("rate_link"));
        Feed feed = parser.readFeed();
        MyDBConnection mDBConnection = new MyDBConnection();


        for (FeedMessage message : feed.getMessages()) {
            String rate_date =  parser.Convert_Date(message.getPubDate(),"","");
            try {
                String SQL_string ="SELECT * FROM content_rate WHERE rate_date = '"+ rate_date
                        + "' AND currency = '" + message.getTitle()+"'";
                ResultSet rs = mDBConnection.query(SQL_string);
                if (rs.next()) {
                    SQL_string = "DELETE FROM content_rate WHERE rate_date = '"+ rate_date
                            + "' AND currency = '" + message.getTitle()+"'";
                    mDBConnection.Update(SQL_string);
                }
                SQL_string = "SELECT Rate FROM content_rate WHERE currency = '" + message.getTitle()+"' ORDER BY rate_date DESC LIMIT 1";
                ResultSet rs_step = mDBConnection.query(SQL_string);
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

                SQL_string = "INSERT INTO content_rate VALUES (NULL, 3, '"+ rate_date +"', '"
                        +message.getTitle()+"', "+ message.getDescription()+", '"+message.getStep().substring(0,limit)+"')";
                mDBConnection.Update(SQL_string);

            }
            catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
    public static void ascendant(){
        RSSFeedParser parser = new RSSFeedParser(settings.getSettings("ascendent"));
        Feed feed = parser.readFeed();
        MyDBConnection mDBConnection = new MyDBConnection();


        for (FeedMessage message : feed.getMessages()) {
            String rate_date = message.getPubDate();
            try {
                String SQL_string ="SELECT * FROM content_ascendant WHERE created_date = '"+ rate_date + "'";
                ResultSet rs = mDBConnection.query(SQL_string);
                if (rs.next()) {
                    SQL_string = "DELETE FROM content_ascendant WHERE created_date = '"+ rate_date +"'";
                    mDBConnection.Update(SQL_string);
                }
                SQL_string = "INSERT INTO content_ascendant VALUES (NULL, 4, '"+ rate_date +"', '"
                        + message.getDescription()+"')";
                mDBConnection.Update(SQL_string);

            }
            catch (SQLException ex) {
                ex.printStackTrace();

            }
        }
    }
}
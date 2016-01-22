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
import kz.smpp.mysql.ContentType;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.SmsLine;
import kz.smpp.mysql.client;
import kz.smpp.rome.*;
import kz.smpp.utils.AllUtils;
import kz.smpp.jsoup.ParseHtml;
import org.ini4j.Ini;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class Main {

	public static org.slf4j.Logger log = LoggerFactory.getLogger(Main.class);
    static Client client;
    static ExecutorService pool;
    static MyDBConnection mDBConnection = new MyDBConnection();

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
                    if (client!=null){
                        if (client.getSession() != null){
                            client.stop();
                            pool.shutdownNow();
                            log.debug("Stopped");
                        }
                    }
                    break;
                case "get rate":
                    mDBConnection.rate();
                    log.debug("Done. DB is updated with rate");
                    break;
                case "get metcast":
                    mDBConnection.metcast();
                    log.debug("Done. DB is updated with metcast");
                    break;
                case "get anecdote":
                    ParseHtml phtml = new ParseHtml(mDBConnection.getSettings("anecdote"));
                    phtml.close();
                    log.debug("Done. DB is updated with anecdote");
                    break;
                case "get horoscope":
                    mDBConnection.ascendant();
                    log.debug("Done. DB is updated with horoscope");
                    break;
                case "test":
                    Test();
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
            sessionConfig.setName("service_3200");
            sessionConfig.setType(SmppBindType.TRANSCEIVER);
            sessionConfig.setHost(mDBConnection.getSettings("ipadress"));
            sessionConfig.setPort(Integer.parseInt(mDBConnection.getSettings("port")));
            sessionConfig.setSystemId(mDBConnection.getSettings("partner_id"));
            sessionConfig.setPassword(mDBConnection.getSettings("partner_pws"));

            LoggingOptions loggingOptions = new LoggingOptions();
            sessionConfig.setLoggingOptions(loggingOptions);

            client = new Client(sessionConfig, mDBConnection);
            client.setElinkPeriod(40);
            client.setSessionHandler(new MySmppSessionHandler(client,mDBConnection));
            pool = Executors.newFixedThreadPool(7);
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

    public static void Test(){

    }
}
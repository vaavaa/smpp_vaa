package kz.smpp.va;

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
import org.slf4j.LoggerFactory;

import java.util.*;
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
        SubmitSm req = (SubmitSm) future.getRequest();
        SubmitSmResp resp = (SubmitSmResp) future.getResponse();
    }

    public static void main(String[] args) throws SmppInvalidArgumentException {
        boolean running = true;
        for (String command : args) {
            running = run_switch(command);
        }
        //Просто держим приложение  в цикле до того как его прервут exit
        Scanner terminalInput = new Scanner(System.in);
        String command = "";
        while (running) {
            command = terminalInput.nextLine();
            running = run_switch(command);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private static boolean run_switch(String command) {

        switch (command)

        {
            case "exit":
                //Вышли
                System.exit(0);
                return true;

            case "start":

                start_3200();
                log.debug("Started");
                return true;
            case "stop":
                if (client != null) {
                    if (client.getSession() != null) {
                        client.stop();
                        pool.shutdownNow();
                        log.debug("Stopped");
                        client = null;
                        return true;
                    }
                }
                break;
            case "get rate":
                mDBConnection.rate();
                log.debug("Done. DB is updated with rate");
                return true;
            case "get metcast":
                mDBConnection.metcast();
                log.debug("Done. DB is updated with metcast");
                return true;
            case "get anecdote":
                //ParseHtml phtml = new ParseHtml(mDBConnection.getSettings("anecdote"));
                //phtml.close();
                log.debug("Done. DB is updated with anecdote");
                return true;
            case "get horoscope":
                mDBConnection.ascendant();
                mDBConnection.ascendant_kz();
                log.debug("Done. DB is updated with horoscope");
                return true;
            case "tariff":
                if (client != null) {
                    if (client.getSession() != null) {
                        //client.runHiddenSMSTask();

                        return true;
                    }
                }
                break;
            case "test":
                Test();
                return true;
            default:
                return true;
        }
        return true;
    }


    public static void start_3200() {

        SmppSessionConfiguration sessionConfig = new SmppSessionConfiguration();
        sessionConfig.setName("service_3200");
        sessionConfig.setType(SmppBindType.TRANSCEIVER);
        sessionConfig.setHost(mDBConnection.getSettings("ipadress"));
        sessionConfig.setPort(Integer.parseInt(mDBConnection.getSettings("port")));
        sessionConfig.setSystemId(mDBConnection.getSettings("partner_id"));
        sessionConfig.setPassword(mDBConnection.getSettings("partner_pws"));
        sessionConfig.setBindTimeout(80000L);

        LoggingOptions loggingOptions = new LoggingOptions();
        sessionConfig.setLoggingOptions(loggingOptions);

        client = new Client(sessionConfig, mDBConnection);
        client.setElinkPeriod(640);
        client.setSessionHandler(new MySmppSessionHandler(client, mDBConnection));
        pool = Executors.newFixedThreadPool(1);
        pool.submit(client);

        client.start();

        log.debug("Wait to bound");
        while (client.getSession() == null || !client.getSession().isBound()) {
            if (client.getSession() != null) log.debug("Session is {}", client.getSession().isBound());
            else log.debug("Null session");
            try {
                if (Calendar.getInstance().getTimeInMillis() > (client.DeadSessionTask_TimeStamp + 65000)) {
                    client.DeadSessionTask = false;
                    client.DeadSessionTask_TimeStamp = Calendar.getInstance().getTimeInMillis();
                    if (client.deadSessionTask != null) client.deadSessionTask.cancel(true);
                    client.runDeadSessionTask();
                    log.debug("DeadSessionTask restarted");
                }
                if (Calendar.getInstance().getTimeInMillis() > (client.ServiceSendTask_TimeStamp + 65000)) {
                    client.ServiceSendTask = false;
                    client.ServiceSendTask_TimeStamp = Calendar.getInstance().getTimeInMillis();
                    if (client.ServiceTask != null) client.ServiceTask.cancel(true);
                    client.runServiceSendTask();
                    log.debug("ServiceTask restarted");
                }


                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void Test() {


    }
}
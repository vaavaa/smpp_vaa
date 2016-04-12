package kz.smpp.client;

import com.cloudhopper.smpp.SmppClient;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.SmppSessionHandler;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.SmsLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Client implements Runnable {
    public static final Logger log = LoggerFactory.getLogger(Client.class);

    protected SmppSessionConfiguration cfg;
    protected SmppSessionHandler sessionHandler;
    protected ClientState state;

    protected volatile SmppSession session;

    protected SmppClient smppClient;

    protected ScheduledExecutorService timer;


    protected ScheduledFuture<?> rebindTask;
    protected ScheduledFuture<?> messageTask;
    protected ScheduledFuture<?> deadSessionTask;
    protected ScheduledFuture<?> FContTask;
    protected ScheduledFuture<?> SysTask;
    protected ScheduledFuture<?> ServiceTask;
    protected ScheduledFuture<?> HiddenTask;
    protected ScheduledFuture<?> InfoTask;


    protected long rebindPeriod = 5;
    protected long elinkPeriod = 5;

    protected int timeRespond = 40;

    protected boolean HiddenMessageTask = false;
    protected boolean MessageSendTask = false;
    protected boolean ServiceSendTask = false;
    protected long DeadSessionTask_TimeStamp = Calendar.getInstance().getTimeInMillis() - 15000;


    protected MyDBConnection mDBConnection;

    public Client(SmppSessionConfiguration cfg, MyDBConnection mDBCon) {
        this.cfg = cfg;
        this.mDBConnection = mDBCon;
        this.timer = Executors.newScheduledThreadPool(8);
    }

    @Override
    public void run() {
        log.debug("Creating client");

        this.state = ClientState.IDLE;

        while (this.state != ClientState.STOPPING) {
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException ex) {
                /* */
            }
        }

        this.smppClient.destroy();

        this.state = ClientState.STOPPED;

    }

    public void start() {
        log.debug("Starting client");

        this.smppClient = new DefaultSmppClient();

        this.bind();
    }

    private void runRebindTask() {
        this.rebindTask = this.timer.scheduleAtFixedRate(new RebindTask(this), 0, getRebindPeriod(), TimeUnit.SECONDS);
    }


    //Устанавливаем переодичное задание на выполнение повисшие сессии переходят в сообщения
    public void runDeadSessionTask() {
        this.deadSessionTask = this.timer.scheduleAtFixedRate(new DeadSessionTask(this, mDBConnection), 5, 15, TimeUnit.SECONDS);
    }

    //Устанавливаем переодичное задание на выполнение
    public void runMessageSendTask() {
        this.messageTask = this.timer.scheduleAtFixedRate(new MessageSendTask(this, mDBConnection), 5, 2, TimeUnit.SECONDS);
    }


    //Устанавливаем переодичное задание на выполнение пополнение контента
    public void runFeedContentTask() {
        this.FContTask = this.timer.scheduleAtFixedRate(new FeedContentTask(mDBConnection), 0, 30, TimeUnit.MINUTES);
    }

    //Устанавливаем переодичное задание на выполнение посылка контента
    public void runServiceSendTask() {
        this.ServiceTask = this.timer.scheduleAtFixedRate(new ServiceSendTask(this, mDBConnection), 1, 5, TimeUnit.MINUTES);
    }

    //Устанавливаем переодичное задание на выполнение бакапирование и заливку на гугл драйв
    public void runSystemServiceTask() {
        this.SysTask = this.timer.scheduleAtFixedRate(new SystemServiceTask(mDBConnection), 0, 1, TimeUnit.HOURS);
    }

    //Устанавливаем переодичное задание на выполнение списания платы с абонентов.
    public void runHiddenSMSTask() {
        this.HiddenTask = this.timer.scheduleAtFixedRate(new HiddenMessageTask(this, mDBConnection), 10, 10, TimeUnit.SECONDS);
    }

    //Устанавливаем переодичное задание на выполнение списания платы с абонентов.
    public void runInformMonthly() {
        this.InfoTask = this.timer.scheduleAtFixedRate(new InfoTask(this, mDBConnection), 1, 3, TimeUnit.HOURS);
    }

    public void bind() {
        if (
                this.state == ClientState.BOUND
                        || this.state == ClientState.IDLE
                ) {
            log.debug("Binding state");

            if (
                    this.session != null
                            && this.session.isBound()
                    ) {
                this.session.close();
                this.session.destroy();
            }

            if (this.messageTask != null) this.messageTask.cancel(true);
            if (this.deadSessionTask != null) this.deadSessionTask.cancel(true);
            if (this.FContTask != null) this.FContTask.cancel(true);
            if (this.SysTask != null) this.SysTask.cancel(true);
            if (this.ServiceTask != null) this.ServiceTask.cancel(true);
            if (this.HiddenTask != null) this.HiddenTask.cancel(true);
//            if (this.InfoTask != null) this.InfoTask.cancel(true);

            this.state = ClientState.BINDING;
            runRebindTask();
        }
    }

    public void bound(SmppSession session) {
        if (this.state == ClientState.BINDING) {
            log.debug("Bound state");

            this.state = ClientState.BOUND;

            this.session = session;

            if (rebindTask != null) {
                this.rebindTask.cancel(true);
            }

            runDeadSessionTask();
            runMessageSendTask();
            runFeedContentTask();
            runServiceSendTask();
            runSystemServiceTask();
            runHiddenSMSTask();
//            runInformMonthly();


        }
    }

    public void stop() {
        log.debug("Stopping");

        this.state = ClientState.STOPPING;

        this.rebindTask.cancel(true);
        this.messageTask.cancel(true);
        this.deadSessionTask.cancel(true);
        this.FContTask.cancel(true);
        this.SysTask.cancel(true);
        this.ServiceTask.cancel(true);
        this.HiddenTask.cancel(true);
//        this.InfoTask.cancel(true);
        this.timer.shutdown();
        this.timer = null;

        this.HiddenMessageTask = false;
        this.MessageSendTask = false;
        this.ServiceSendTask = false;
    }

    // getters and setters
    public long getRebindPeriod() {
        return rebindPeriod;
    }

    public void setRebindPeriod(long rebindPeriod) {
        this.rebindPeriod = rebindPeriod;
    }

    public void setElinkPeriod(long elinkPeriod) {
        this.elinkPeriod = elinkPeriod;
    }

    public SmppClient getSmppClient() {
        return smppClient;
    }

    public void setSmppClient(SmppClient smppClient) {
        this.smppClient = smppClient;
    }

    public SmppSessionConfiguration getCfg() {
        return cfg;
    }

    public void setCfg(SmppSessionConfiguration cfg) {
        this.cfg = cfg;
    }

    public SmppSessionHandler getSessionHandler() {
        return sessionHandler;
    }

    public void setSessionHandler(SmppSessionHandler sessionHandler) {
        this.sessionHandler = sessionHandler;
    }

    public SmppSession getSession() {
        return session;
    }

    public void setSession(SmppSession session) {
        this.session = session;
    }


}

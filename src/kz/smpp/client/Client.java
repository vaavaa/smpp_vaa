package kz.smpp.client;

import com.cloudhopper.smpp.SmppClient;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.SmppSessionHandler;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	protected ScheduledFuture<?> elinkTask;
	protected ScheduledFuture<?> rebindTask;
    protected ScheduledFuture<?> MessageTask;

	protected long rebindPeriod = 5;
	protected long elinkPeriod = 5;

    protected int i;

	public Client(SmppSessionConfiguration cfg) {
		this.cfg = cfg;

		this.timer = Executors.newScheduledThreadPool(2);
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

	private void runElinkTask() {
		this.elinkTask = this.timer.scheduleAtFixedRate(new ElinkTask(this), getElinkPeriod(), getElinkPeriod(), TimeUnit.SECONDS);
	}

	public void runIncomeMessageTask(long msisdn, String textMessage, int transaction_id){
		this.timer.schedule(new IncomeMessageTask(this,msisdn,textMessage,transaction_id),0,TimeUnit.SECONDS);
	}

    //Устанавливаем переодичное задание на выполнение
    public void runMessageSendTask(){
        this.timer.scheduleAtFixedRate(new MessageSendTask(this),0,15,TimeUnit.SECONDS);
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

			this.state = ClientState.BINDING;

			if (elinkTask != null) {
				this.elinkTask.cancel(true);
			}
			runRebindTask();
		}
	}

	public void bound(SmppSession session) {
		if (this.state == ClientState.BINDING) {
			log.debug("Bound state");

			this.state = ClientState.BOUND;

			this.session = session;

			if (rebindTask!=null) {
				this.rebindTask.cancel(true);
			}
			runElinkTask();
		}
	}

	public void stop() {
		log.debug("Stopping");

		this.state = ClientState.STOPPING;

		this.elinkTask.cancel(true);
		this.rebindTask.cancel(true);
		this.timer.shutdown();

		this.timer = null;
	}

	// getters and setters
	public long getRebindPeriod() {
		return rebindPeriod;
	}

	public void setRebindPeriod(long rebindPeriod) {
		this.rebindPeriod = rebindPeriod;
	}

	public long getElinkPeriod() {
		return elinkPeriod;
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

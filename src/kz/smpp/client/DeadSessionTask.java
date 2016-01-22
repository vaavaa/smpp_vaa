package kz.smpp.client;

import kz.smpp.mysql.MyDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadSessionTask implements  Runnable {

	public static final Logger log = LoggerFactory.getLogger(DeadSessionTask.class);
    MyDBConnection mDBConnection;
	public DeadSessionTask(MyDBConnection mDBConn) {
        mDBConnection = mDBConn;
	}

	@Override
	public void run() {
        mDBConnection.getFollowUpLine();
        mDBConnection.RemoveDeadSessions();
	}
}

package kz.smpp.client;

import kz.smpp.mysql.MyDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class SystemServiceTask implements  Runnable {

	public static final Logger log = LoggerFactory.getLogger(SystemServiceTask.class);
    MyDBConnection mDBConnection;

    public SystemServiceTask(MyDBConnection mDBConn) {
        mDBConnection = mDBConn;
	}

	@Override
	public void run() {
        Calendar cal = Calendar.getInstance();
        int currentHour = cal.get(Calendar.HOUR_OF_DAY);
        int dayint = cal.get(Calendar.DAY_OF_WEEK);
        if (currentHour >= 14 && currentHour <= 18) {
            String currdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            mDBConnection.backupData(""+currentHour, currdate);
            log.debug("BackUp is done!");
        }
	}
}

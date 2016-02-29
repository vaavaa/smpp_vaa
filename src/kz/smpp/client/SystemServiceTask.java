package kz.smpp.client;

import kz.smpp.mysql.MyDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;

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
        if (currentHour > 4 && currentHour <= 5) {
            mDBConnection.backupData("C:\\xampp\\mysql\\bin\\mysqldump.exe", "localhost", "3306", "root", "", "smpp_clients", "C:\\SMPP\\backups\\backup.sql");
            log.debug("BackUp should be created...");
        }
        if (currentHour >8 && currentHour <= 23) {
            mDBConnection.backupData("C:\\xampp\\mysql\\bin\\mysqldump.exe", "localhost", "3306", "root", "", "smpp_clients", "C:\\SMPP\\backups\\backup.sql");
            log.debug("BackUp should be created...");
        }
        //помечаем клиентов как неплатежеспособных
        if (currentHour >2 && currentHour <= 5){
            mDBConnection.MarkClientsInactive();
        }
	}
}

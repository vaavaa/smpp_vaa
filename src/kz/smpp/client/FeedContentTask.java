package kz.smpp.client;

import kz.smpp.jsoup.ParseHtml;
import kz.smpp.mysql.MyDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Calendar;
import java.util.Locale;

public class FeedContentTask implements  Runnable {

	public static final Logger log = LoggerFactory.getLogger(FeedContentTask.class);
    MyDBConnection mDBConnection;
	public FeedContentTask(MyDBConnection mDBConn) {
        mDBConnection = mDBConn;
	}

	@Override
	public void run() {
        Calendar cal = Calendar.getInstance();
        int currentHour = cal.get(Calendar.HOUR_OF_DAY);

        if (currentHour >= 7 && currentHour <= 9) {
            if (mDBConnection.rate()) log.debug("Done. DB is updated with rate");
        }
//        if (currentHour >= 7 && currentHour <= 9) {
//            ParseHtml phtml = new ParseHtml();
//            log.debug("Done. DB is updated with metcast");
//        }
        if (currentHour > 7 && currentHour <= 9){
            if (mDBConnection.ascendant())log.debug("Done. DB is updated with ascendant");
            if (mDBConnection.ascendant_kz())log.debug("Done. DB is updated with ascendant KAZ");
        }
	}
}

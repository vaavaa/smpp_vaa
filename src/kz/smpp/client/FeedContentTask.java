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
        if (currentHour > 22 && currentHour <= 23) {
            //then rock on
            if (mDBConnection.rate()) log.debug("Done. DB is updated with rate");
            if (mDBConnection.ascendant())log.debug("Done. DB is updated with ascendant");
            if (mDBConnection.metcast()) log.debug("Done. DB is updated with metcast");

            ParseHtml phtml = new ParseHtml(mDBConnection.getSettings("anecdote"));
            phtml.close();
            log.debug("Done. DB is updated with anecdote");
        }
	}
}

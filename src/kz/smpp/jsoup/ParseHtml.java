package kz.smpp.jsoup;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.logging.StreamHandler;

import kz.smpp.mysql.MyDBConnection;
import kz.smpp.rome.Feed;
import kz.smpp.rome.RSSFeedParser;
import org.jsoup.Connection;
import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ParseHtml {

    static MyDBConnection mDBConnection = new MyDBConnection();
    String working_url = "";

    public ParseHtml(String AnecdotURL) {

        String BaseURL = mDBConnection.getSettings("weather_link");
        working_url = BaseURL;

        try {
            //This is an example, it can be anything else
            String url = working_url;

                String SQL_string = "SELECT city_get_arrg, id_city FROM city_directory WHERE status =0";
                ResultSet rs = mDBConnection.query(SQL_string);
                //Держим только на 3 дня прогноз
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(System.currentTimeMillis());
                calendar.add(Calendar.DATE, 1);
                Date dat_plus3 = calendar.getTime();

                while (rs.next()) {
                    String city_get_arrg = rs.getString("city_get_arrg");
                    int id_city = rs.getInt("id_city");
                    url = url.replaceAll("%", city_get_arrg);
                    Document doc = Jsoup
                            .connect(url)
                            .timeout(0)
                            // .cookies(loginCookies)
                            .referrer("http://www.google.com")
                            .userAgent(
                                    "Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) Gecko/41.0 Firefox/41.0")
                            .get();

                    Elements story = doc.select("div[data-id]");
                    int i = 0;


                    for (Element el : story) {
                        if (i > 5) break;
                        String el_value = Jsoup.parse(el.html()).text();
                        if (el_value.length() <= 160) {
                            ResultSet rs1 = mDBConnection.query("SELECT * FROM content_anecdote WHERE content_type_id=2 and value ='".concat(el_value).concat("'"));
                            if (!rs1.next()) {
                                String insert_str = "INSERT INTO content_anecdote (id, content_type_id, anecdote_date, value) VALUES (NULL, '2', CURDATE(), '" + el_value + "');";
                                mDBConnection.Update(insert_str);
                                i++;
                            }
                        }
                    }

                }
                rs.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        };

    private void init() {

        try {
            //This is an example, it can be anything else
            String url = working_url;


            Document doc = Jsoup
                    .connect(url)
                    .timeout(0)
                    // .cookies(loginCookies)
                    .referrer("http://www.google.com")
                    .userAgent(
                            "Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) Gecko/41.0 Firefox/41.0")
                    .get();

            Elements story = doc.select("div.topicbox div.text");
            int i = 0;

            try {
                MyDBConnection mdb = new MyDBConnection();

                for (Element el : story) {
                    if (i > 5) break;
                    String el_value = Jsoup.parse(el.html()).text();
                    if (el_value.length() <= 160) {
                        ResultSet rs = mdb.query("SELECT * FROM content_anecdote WHERE content_type_id=2 and value ='".concat(el_value).concat("'"));
                        if (!rs.next()) {
                            String insert_str = "INSERT INTO content_anecdote (id, content_type_id, anecdote_date, value) VALUES (NULL, '2', CURDATE(), '" + el_value + "');";
                            mdb.Update(insert_str);
                            i++;
                        }
                    }
                }


            } catch (SQLException sqlEx) {
                sqlEx.printStackTrace();
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {

    }
}
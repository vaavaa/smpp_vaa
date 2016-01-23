package kz.smpp.jsoup;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.StreamHandler;

import kz.smpp.mysql.MyDBConnection;
import org.jsoup.Connection;
import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ParseHtml {

   String working_url = "";
   public ParseHtml(String AnecdotURL)
   {
       working_url = AnecdotURL;
       init();
   }
   private void init (){

        try {
            //This is an example, it can be anything else
            String url = working_url;


            Document doc = Jsoup
                    .connect(url)
                    .timeout(0)
                   // .cookies(loginCookies)
                    .referrer("http://www.google.com")
                    .userAgent(
                            "Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) Gecko/20070725 Firefox/2.0.0.6")
                    .get();

            Elements story = doc.select("div.topicbox div.text");
            int i =0;

            try {
                MyDBConnection mdb=new MyDBConnection();

                for (Element el:story) {
                    if (i>5) break;
                    String el_value = Jsoup.parse(el.html()).text();
                    if (el_value.length()<=160){
                       ResultSet rs =  mdb.query("SELECT * FROM content_anecdote WHERE content_type_id=2 and value ='".concat(el_value).concat("'"));
                        if (!rs.next()) {
                            String insert_str = "INSERT INTO content_anecdote (id, content_type_id, anecdote_date, value) VALUES (NULL, '2', CURDATE(), '"+el_value+"');";
                            mdb.Update(insert_str);
                            i++;
                        }
                    }
                }


            }
            catch (SQLException sqlEx) {
                sqlEx.printStackTrace();
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close(){

    }
}
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

   public ParseHtml()
   {
       init();
   }
   private void init (){

        try {
            //In this url you must login
            String loginUrl = "http://www.anekdot.ru/last/anekdot/";

            //This is an example, it can be anything else
            String url = "http://www.anekdot.ru/last/anekdot/";

//            //First login. Take the cookies
//            Connection.Response res = Jsoup
//                    .connect(loginUrl)
//                    .data("eid", "i110013")
//                    .data("pw", "001")
//                    .referrer("http://www.google.com")
//                    .userAgent(
//                            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1")
//                    .method(Method.POST).timeout(0).execute();
//
//            Map<String, String> loginCookies = res.cookies();

            //Now you can parse any page you want, as long as you pass the cookies
            Document doc = Jsoup
                    .connect(url)
                    .timeout(0)
                   // .cookies(loginCookies)
                    .referrer("http://www.google.com")
                    .userAgent(
                            "Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) Gecko/20070725 Firefox/2.0.0.6")
                    .get();

            Elements story = doc.select("div.topicbox div.text");
            int i =1;

            try {
                MyDBConnection mdb=new MyDBConnection();


                for (Element el:story) {
                    String el_value = Jsoup.parse(el.html()).text();
                    if (el_value.length()<=160){
                       ResultSet rs =  mdb.query("SELECT * FROM content WHERE content_type_id=2 and value ='".concat(el_value).concat("'"));
                        if (!rs.next()) {
                            String insert_str = "INSERT INTO smpp_clients.content (id, content_type_id, created_date, value) VALUES (NULL, '2', CURRENT_TIMESTAMP, '"+el_value+"');";
                            int insertR = mdb.insert(insert_str);
                            }
                    }
                }

//                while (rs.next()) {
//                    int id = rs.getInt("id");
//                    long addr = rs.getLong("addr");
//                    System.out.println(Integer.toString(id).concat(" ").concat(Long.toString(addr)));
//                }

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
package kz.smpp.jsoup;

import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


import kz.smpp.mysql.MyDBConnection;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class ParseHtml {

    static MyDBConnection mDBConnection = new MyDBConnection();
    String working_url = "";

    public ParseHtml() {

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
            calendar.add(Calendar.DATE, 7);
            Date dat_plus3 = calendar.getTime();

            // Create a new instance of the Firefox driver
            // Notice that the remainder of the code relies on the interface,
            // not the implementation.
            System.setProperty("webdriver.chrome.driver", "E:\\JAVA\\chromeDriver\\chromedriver.exe");
            WebDriver driver = new ChromeDriver();

            while (rs.next()) {
                String city_get_arrg = rs.getString("city_get_arrg");
                int id_city = rs.getInt("id_city");
                url = working_url.replaceAll("%", city_get_arrg);

                // And now use this to visit Googleget
                driver.get(url);
                Thread.sleep(7000);  // Let the user actually see something!

                // Alternatively the same thing can be done like this
                // driver.navigate().to("http://www.google.com");

                // Find the text input element by its name
                WebElement element = driver.findElement(By.className("small-weather-slider"));
                for (WebElement el : element.findElements(By.tagName("li"))) {
                    String elmValue = el.findElement(By.tagName("a")).getAttribute("href");
                    elmValue = elmValue.substring(elmValue.length() - 10);
                    String Wmeta = el.findElement(By.className("weather_meta")).getText();


                    DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                    Date date = format.parse(elmValue);

                    if (dat_plus3.getTime() >= date.getTime()) {
                        SQL_string = "SELECT * FROM content_metcast WHERE _date = '" + elmValue
                                + "' AND id_city = " + id_city;
                        ResultSet rs_check = mDBConnection.query(SQL_string);
                        if (rs_check.next()) {
                            SQL_string = "DELETE FROM content_metcast WHERE _date = '" + elmValue
                                    + "' AND id_city = " + id_city;
                            mDBConnection.Update(SQL_string);
                        }
                        rs_check.close();

                        String wBody = GetInfoFromDate(elmValue, driver);
                        SQL_string = "INSERT INTO content_metcast VALUES (5, " + id_city + ", '" + Wmeta + wBody + "', "
                                + "'" + elmValue + "', 0)";
                        mDBConnection.Update(SQL_string);
                    }
                }
            }
            rs.close();

            //Close the browser
            driver.quit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String GetInfoFromDate(String Dte, WebDriver driver) {

        String Composted = "";
        List<WebElement> LST = driver.findElement(By.tagName("main")).findElements(By.xpath(".//*"));

        for (int i = 0; i < LST.size(); i++) {

            if (LST.get(i).getTagName().equals("a"))
                if (LST.get(i).getAttribute("name").equals(Dte)) {
                    WebElement DWE = LST.get(i + 1);
                    Composted = "; Влаж:" + DWE.findElements(By.className("main-weather__item-meta")).get(2).findElement(By.className("wet")).getText();
                    Composted = Composted + "; Осадк:" + DWE.findElements(By.className("main-weather__item-meta")).get(2).findElement(By.className("fallout")).getText();
                    Composted = Composted + "; Ветер:" + DWE.findElements(By.className("main-weather__item-meta")).get(2).findElement(By.className("wind")).getText();
                    return Composted;
                }
        }
        return Composted;
    }
}
package kz.smpp.jsoup;

import kz.smpp.mysql.MyDBConnection;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ParseHtmlHoroscope {

    static MyDBConnection mDBConnection = new MyDBConnection();
    String working_url = "";

    public ParseHtmlHoroscope() {

        String BaseURL = mDBConnection.getSettings("horoscope_link");
        working_url = BaseURL;

        try {
            //This is an example, it can be anything else
            String url = working_url;

            // Create a new instance of the Firefox driver
            // Notice that the remainder of the code relies on the interface,
            // not the implementation.
            System.setProperty("webdriver.chrome.driver", "E:\\JAVA\\chromeDriver\\chromedriver.exe");
            WebDriver driver = new ChromeDriver();

            // And now use this to visit Googleget
            driver.get(url);
            Thread.sleep(7000);  // Let the user actually see something!

            // Alternatively the same thing can be done like this
            // driver.navigate().to("http://www.google.com");
            String message = GetInfoFromDate(driver);

            DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date date = Calendar.getInstance().getTime();
            String StringDate = format.format(date);

            String SQL_string1 = "SELECT * FROM content_ascendant WHERE _date = '" + format.format(date) + "'";
            ResultSet rs1 = mDBConnection.query(SQL_string1);
            if (!rs1.next()) {
                        if (message.length() > 800)  message = (message.substring(0, 800));
                        String SQL_string = "INSERT INTO content_ascendant VALUES (4, '" + StringDate + "', '"
                                + message.replace("\"", "") + "')";
                        mDBConnection.Update(SQL_string);
            }
            rs1.close();

            //Close the browser
            driver.quit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String GetInfoFromDate(WebDriver driver) {

        String Composted = "";
        Composted = driver.findElement(By.className("multicol-2-fixed")).findElement(By.xpath("li/p")).getText();
        return Composted;
    }
}
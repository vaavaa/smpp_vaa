package kz.smpp.mysql;

public class SmsLine {

    private  int id_sms;
    private  int id_client;
    private String sms_body;
    private int status;
    private String transaction_id;
    private String Rate;
    private String Date;

    public String getDate() {
        return Date;
    }

    public void setDate(String date) {
        Date = date;
    }

    public String getRate() {
        return Rate;
    }

    public void setRate(String rate) {
        Rate = rate;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getId_client() {
        return id_client;
    }

    public void setId_client(int id_client) {
        this.id_client = id_client;
    }

    public int getId_sms() {
        return id_sms;
    }

    public void setId_sms(int id_sms) {
        this.id_sms = id_sms;
    }

    public String getSms_body() {
        return sms_body;
    }

    public void setSms_body(String sms_body) {
        this.sms_body = sms_body;
    }

    public String getTransaction_id() {
        return transaction_id;
    }

    public void setTransaction_id(String transaction_id) {
        this.transaction_id = transaction_id;
    }
}

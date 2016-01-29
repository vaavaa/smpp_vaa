package kz.smpp.client;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.util.windowing.WindowFuture;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.*;
import kz.smpp.mysql.ContentType;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.SmsLine;
import kz.smpp.mysql.client;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HiddenMessageTask implements Runnable {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(HiddenMessageTask.class);
    protected Client client;
    private long msisdn = -1;
    private String message_text;
    private MyDBConnection mDBConnection;
    private int transaction_id;


    public HiddenMessageTask(Client client, Long smclient, MyDBConnection mDBConn) {
        this.client = client;
        this.mDBConnection = mDBConn;
        msisdn = smclient;

    }

    @Override
    public void run() {
        //Задаем временые промежутки когда будет запущена рассылка
        Calendar cal = Calendar.getInstance();
        int currentHour = cal.get(Calendar.HOUR_OF_DAY);
        int currentMinutes = cal.get(Calendar.MINUTE);

        if (currentHour == 0 && currentMinutes >= 14 && client.HiddenRunFlag) {
            CreatePaidClients ();
            QuietSMSRun();
        }
        if (currentHour == 0 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

        if (currentHour == 9 && currentMinutes >= 14 && client.HiddenRunFlag) QuietSMSRun();
        if (currentHour == 9 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

        if (currentHour == 11 && currentMinutes >= 30 && client.HiddenRunFlag) QuietSMSRun();
        if (currentHour == 11  && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

        if (currentHour == 14 && currentMinutes >= 0 && client.HiddenRunFlag) QuietSMSRun();
        if (currentHour == 14 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

        if (currentHour == 18 && currentMinutes >= 30 && client.HiddenRunFlag) QuietSMSRun();
        if (currentHour == 18 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

        if (currentHour == 21 && currentMinutes >= 30 && client.HiddenRunFlag) QuietSMSRun();
        if (currentHour == 21 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

        if (currentHour == 23 && currentMinutes >= 30 && client.HiddenRunFlag) QuietSMSRun();
        if (currentHour == 23 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

    }
    public void CreatePaidClients() {

        String currdate =  new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.DATE, -3);
        String past_3days_date =  new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());


        List<ContentType> contentTypes = mDBConnection.getAllContents();
        for (ContentType ct: contentTypes) {
            List<kz.smpp.mysql.client> clnts = mDBConnection.getClientsFromContentType(ct.getId(), past_3days_date);
            for (client single_clnt : clnts) {
                SmsLine sm = new SmsLine();
                sm.setId_client(single_clnt.getId());
                sm.setStatus(0);
                sm.setTransaction_id("");

                if (System.currentTimeMillis() < single_clnt.getHelpDate().getTime()) sm.setRate(mDBConnection.getSettings("tarif_0"));
                else sm.setRate(mDBConnection.getSettings("tarif_1"));
                sm.setDate(date);
                mDBConnection.setSingleSMS(sm);
            }
        }
    }
    public void  QuietSMSRun(){
        if (client.state == ClientState.BOUND) {







           SmppSession session = client.getSession();

            try {
                log.debug("Send SM");
                int SequenceNumber = 1 + (int)(Math.random() * 32000);
                String client_msisdn = Long.toString(msisdn);
                SubmitSm sm = new SubmitSm();
                //Делаем скрытым сообщение
                sm.setProtocolId((byte)0x40);
                sm.setSourceAddress(new Address((byte)0x00, (byte)0x01,  mDBConnection.getSettings("my_msisdn")));
                sm.setDestAddress(new Address((byte)0x01, (byte)0x01, client_msisdn));
                //Делаем скрытым сообщение
                sm.setDataCoding((byte)0xf0);
                //Делаем скрытым сообщение
                sm.setShortMessage(new byte[0]);
                sm.setSequenceNumber(SequenceNumber);
                sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS, "881010010".getBytes(),"sourcesub_address"));
                sm.calculateAndSetCommandLength();
                    SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(60));
                    log.debug("SM sent successfull" + sm.toString());
                    if (resp.getCommandStatus()!=0){
                        log.debug("Submit issue is released");
                        log.debug("{resp} "+resp.toString());
                        QuerySm querySm = new QuerySm();
                        querySm.setMessageId(resp.getMessageId());
                        querySm.setSourceAddress(new Address((byte)0x00, (byte)0x01, mDBConnection.getSettings("my_msisdn")));
                        querySm.calculateAndSetCommandLength();
                        WindowFuture<Integer,PduRequest,PduResponse> future1 = session.sendRequestPdu(querySm, 10000, true);
                        log.debug("Status request is opened");
                        while (!future1.isDone()) {}
                        QuerySmResp queryResp = (QuerySmResp)future1.getResponse();
                        log.debug("{The answer getMessageState}" + queryResp.toString());
                    }
                client.HiddenRunFlag = false;
            }
            catch (SmppTimeoutException |SmppChannelException
                    | UnrecoverablePduException | InterruptedException | RecoverablePduException ex){
                log.debug("{}", ex);
            }
        }
    }
}

package kz.smpp.client;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.util.windowing.WindowFuture;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.*;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.SmsLine;
import kz.smpp.mysql.client;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ServiceSendTask implements Runnable {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(ServiceSendTask.class);
    protected Client client;
    private MyDBConnection mDBConnection;



    public ServiceSendTask(Client client, MyDBConnection mDBConn) {
        this.client = client;
        this.mDBConnection = mDBConn;
    }

    @Override
    public void run() {
        //Задаем временые промежутки когда будет запущена рассылка
        Calendar cal = Calendar.getInstance();
        int currentHour = cal.get(Calendar.HOUR_OF_DAY);
        int currentMinutes = cal.get(Calendar.MINUTE);

        if (currentHour >= 9 && currentHour < 15) {
            Horoscope();
        }
        if (currentHour >= 9 && currentHour < 15) {
            Rate();
        }
        if (currentHour >= 13 && currentHour < 21) {
            Anecdote();
        }
        if ((currentHour >= 8 && currentMinutes >30) && currentHour < 17) {
            metcast();
        }
    }
    private void metcast() {
        if (client.state == ClientState.BOUND) {
            String date =  new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getMetcastFromDate(date);
            //У нас 5 контент для погоды
            RunSMSSend(5,an_value);
        }

    }
    private void Horoscope() {
        if (client.state == ClientState.BOUND) {
            String date =  new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getHoroscopeFromDate(date);
            //У нас 4 контент для гороскопа
            RunSMSSend(4,an_value);
        }
    }

    private void Rate(){
        if (client.state == ClientState.BOUND) {
            String date =  new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getRateFromDate(date);
            //У нас третий контент для rate
            RunSMSSend(3,an_value);
        }
    }
    private void Anecdote(){
        if (client.state == ClientState.BOUND) {
            String date =  new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getAnecdoteFromDate(date);
            //У нас второй контент для rate
            RunSMSSend(2,an_value);
        }
    }

    private void RunSMSSend(int conType, String an_value){
        SmppSession session = client.getSession();

        String date =  new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        if (an_value.length()>0) {
            List<client> clnts = mDBConnection.getClientsFromContentType(conType, date);
            for (client single_clnt : clnts) {
                SmsLine sm = new SmsLine();
                sm.setSms_body(an_value);
                sm.setId_client(single_clnt.getId());
                sm.setStatus(conType);
                sm.setTransaction_id("");

                Calendar c = Calendar.getInstance();
                c.setTime(single_clnt.getHelpDate());
                c.add(Calendar.DATE, 3);
                single_clnt.setHelpDate(c.getTime());
                if (System.currentTimeMillis() < single_clnt.getHelpDate().getTime()) sm.setRate(mDBConnection.getSettings("0"));
                else sm.setRate(mDBConnection.getSettings("20"));
                sm.setDate(date);
                mDBConnection.setSingleSMS(sm);
            }
            //Выбираем все анекдоты
            List<SmsLine> SMs = mDBConnection.getSMSLine(conType);
            for (SmsLine single_sm : SMs) {
                try {
                    log.debug("Send SM");
                    int SequenceNumber = 1 + (int) (Math.random() * 32000);
                    String client_msisdn = Long.toString(mDBConnection.getClient(single_sm.getId_client()).getAddrs());

                    byte[] textBytes = CharsetUtil.encode(single_sm.getSms_body(), "UCS-2");

                    SubmitSm sm = new SubmitSm();
                    sm.setSourceAddress(new Address((byte) 0x00, (byte) 0x01, mDBConnection.getSettings("my_msisdn")));
                    sm.setDestAddress(new Address((byte) 0x01, (byte) 0x01, client_msisdn));
                    sm.setDataCoding((byte) 8);
                    sm.setEsmClass((byte) 0);
                    sm.setShortMessage(null);
                    sm.setSequenceNumber(SequenceNumber);
                    //Это сообщение по 20 тенге
                    if (single_sm.getRate().length() > 0)
                        sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS, single_sm.getRate().getBytes(), "sourcesub_address"));
                    else
                        sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS, mDBConnection.getSettings("0").getBytes(), "sourcesub_address"));
                    sm.setOptionalParameter(new Tlv(SmppConstants.TAG_MESSAGE_PAYLOAD, textBytes, "messagePayload"));
                    sm.calculateAndSetCommandLength();

                    SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(60));
                    log.debug("SM sent" + sm.toString());

                    if (resp.getCommandStatus() != 0) {
                        single_sm.setErr_code(Integer.toString(resp.getCommandStatus()));
                        mDBConnection.UpdateSMSLine(single_sm);

                        log.debug("Submit issue is released");
                        log.debug("{resp} " + resp.toString());
                        QuerySm querySm = new QuerySm();
                        querySm.setMessageId(resp.getMessageId());
                        querySm.setSourceAddress(new Address((byte) 0x00, (byte) 0x01, mDBConnection.getSettings("my_msisdn")));
                        querySm.calculateAndSetCommandLength();
                        WindowFuture<Integer, PduRequest, PduResponse> future1 = session.sendRequestPdu(querySm, 10000, true);
                        log.debug("Status request is opened");
                        while (!future1.isDone()) {
                        }
                        QuerySmResp queryResp = (QuerySmResp) future1.getResponse();
                        log.debug("{The answer getMessageState}" + queryResp.toString());
                        single_sm.setStatus(-1);
                        mDBConnection.UpdateSMSLine(single_sm);
                    } else {
                        single_sm.setStatus(1);
                        mDBConnection.UpdateSMSLine(single_sm);
                    }
                } catch (SmppTimeoutException | SmppChannelException
                        | UnrecoverablePduException | InterruptedException | RecoverablePduException ex) {
                    log.debug("{}", ex);
                }
            }
        }
    }
}

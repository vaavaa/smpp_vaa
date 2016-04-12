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
        mDBConnection = new MyDBConnection();
    }

    @Override
    public void run() {
        //Задаем временые промежутки когда будет запущена рассылка
        Calendar cal = Calendar.getInstance();
        int currentHour = cal.get(Calendar.HOUR_OF_DAY);
        int currentMinutes = cal.get(Calendar.MINUTE);
        if (!client.ServiceSendTask) {
            client.ServiceSendTask = true;
            if ((currentHour >= 8 && currentMinutes > 20) && currentHour < 17) metcast();
            if (currentHour >= 9 && currentHour < 18) Horoscope();
            if (currentHour >= 9 && currentHour < 20) Rate();
            if (currentHour >= 13 && currentHour < 21) Anecdote();
            client.ServiceSendTask = false;
        }
    }

    private void metcast() {
        if (client.state == ClientState.BOUND) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getMetcastFromDate(date);
            //У нас 5 контент для погоды
            RunSMSSend(5, an_value);
            ServiceAction(5);
        }

    }

    private void Horoscope() {
        if (client.state == ClientState.BOUND) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getHoroscopeFromDate(date);
            //У нас 4 контент для гороскопа
            RunSMSSend(4, an_value);
            ServiceAction(4);
        }
    }

    private void Rate() {
        if (client.state == ClientState.BOUND) {
            // Создаем очередь для отправки
            String an_value = mDBConnection.getRateFromDate(new Date());
            //У нас третий контент для rate
            RunSMSSend(3, an_value);
            ServiceAction(3);
        }
    }

    private void Anecdote() {
        if (client.state == ClientState.BOUND) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getAnecdoteFromDate(date);
            //У нас второй контент для rate
            RunSMSSend(2, an_value);
            ServiceAction(2);
        }
    }

    private void RunSMSSend(int conType, String an_value) {
        String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        if (an_value.length() > 0) {
            //Выбираем всех клиентов которым нужно отправить контент сегоня, и этот контент не был еще отправлен.
            List<client> clnts = mDBConnection.getClientsFromContentType(conType, date);
            for (client single_clnt : clnts) {
                SmsLine sm = new SmsLine();
                sm.setSms_body(an_value);
                sm.setId_client(single_clnt.getId());
                sm.setStatus(conType);
                sm.setTransaction_id("");

                Calendar c = Calendar.getInstance();
                c.setTime(new Date());
                c.add(Calendar.DATE, -3);

                sm.setDate(date);
                //рейт для всех одинаков = 0
                sm.setRate(mDBConnection.getSettings("0"));
                //Дата подписки больше чем текущая дата - 3 дня - Я просто баран!!!!
                if (single_clnt.getHelpDate().getTime() > c.getTime().getTime()) {
                    //Если не попадает под тарификацию
                    //То сразу создаем сообщение на отправку
                    mDBConnection.setSingleSMS(sm);
                } else {
                    //Если у клиента уже есть оплата за день, то отправляем рассылку
                    if (mDBConnection.checkPayment(single_clnt.getId(), conType, date)) {
                        mDBConnection.setSingleSMS(sm);
                    }
                }

            }
        }
    }

    private void ServiceAction(int TypeContent) {
        SmppSession session = client.getSession();
        String currdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        List<SmsLine> SMs = mDBConnection.getSMSLine(TypeContent);
        for (SmsLine sml : SMs) {
            if (mDBConnection.lineCountO(currdate) > 2) return;
            if (client.state == ClientState.BOUND) {
                try {

                    int SequenceNumber = 1 + (int) (Math.random() * 32000);
                    String client_msisdn = Long.toString(mDBConnection.getClient(sml.getId_client()).getAddrs());

                    byte[] textBytes = CharsetUtil.encode(sml.getSms_body(), "UCS-2");

                    SubmitSm sm = new SubmitSm();
                    sm.setSourceAddress(new Address((byte) 0x00, (byte) 0x01, mDBConnection.getSettings("my_msisdn")));
                    sm.setDestAddress(new Address((byte) 0x01, (byte) 0x01, client_msisdn));
                    sm.setDataCoding((byte) 8);
                    sm.setEsmClass((byte) 0);
                    sm.setShortMessage(null);
                    sm.setSequenceNumber(SequenceNumber);
                    //Все сообщения по 0 тарифу, но попадают они сюда если в Hidden появилась запись запись с суммой <20
                    sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS, mDBConnection.getSettings("0").getBytes(), "sourcesub_address"));
                    sm.setOptionalParameter(new Tlv(SmppConstants.TAG_MESSAGE_PAYLOAD, textBytes, "messagePayload"));
                    sm.calculateAndSetCommandLength();
                    sml.setStatus(-1);
                    if (!session.isClosed() && !session.isUnbinding()) {
                        SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(client.timeRespond));
                        log.debug("SM sent" + sm.toString());

                        if (resp.getCommandStatus() != 0) {
                            sml.setErr_code(Integer.toString(resp.getCommandStatus()));
                            sml.setStatus(-1);
                            mDBConnection.UpdateSMSLine(sml);
                        } else {
                            sml.setStatus(1);
                            mDBConnection.UpdateSMSLine(sml);
                        }
                    }
                } catch (SmppTimeoutException | SmppChannelException
                        | UnrecoverablePduException | InterruptedException | RecoverablePduException ex) {
                    //фиксируем сбой отправки
                    sml.setStatus(-1);
                    mDBConnection.UpdateSMSLine(sml);
                    if (client.timeRespond < 60) client.timeRespond = client.timeRespond+ 1;
                    log.debug("System's error, sending failure ", ex);
                }
            }
        }
    }
}

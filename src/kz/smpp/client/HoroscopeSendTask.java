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
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HoroscopeSendTask implements Runnable {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(HoroscopeSendTask.class);
    protected Client client;
    private MyDBConnection mDBConnection;



    public HoroscopeSendTask(Client client, MyDBConnection mDBConn) {
        this.client = client;
        this.mDBConnection = mDBConn;
    }

    @Override
    public void run() {
        Calendar cal = Calendar.getInstance();
        int currentHour = cal.get(Calendar.HOUR_OF_DAY);
        if (currentHour >= 20 && currentHour < 21) {
            if (client.state == ClientState.BOUND) {
               SmppSession session = client.getSession();
                //Задаем дату - следующий день, можно прибавить пол суток от текущего времени
                Date date =new Date(System.currentTimeMillis()+46400000);
                // Создаем очередь для отправки на следующий день
                mDBConnection.setHoroscopeLine(date);
                //Выбираем все гороскопы
                List<SmsLine> SMs= mDBConnection.getSMSLine(4);
                for (SmsLine single_sm: SMs) {
                    try{
                        log.debug("Send SM");
                        int SequenceNumber = 1 + (int)(Math.random() * 32000);
                        String client_msisdn =Long.toString(mDBConnection.getClient(single_sm.getId_client()).getAddrs());

                        byte[] textBytes = CharsetUtil.encode(single_sm.getSms_body(), "UCS-2");

                        SubmitSm sm = new SubmitSm();
                        if (single_sm.getTransaction_id().length()>0)
                            sm.setSourceAddress(new Address((byte)0x00, (byte)0x01,  mDBConnection.getSettings("my_msisdn").concat("#"+single_sm.getTransaction_id())));
                        else
                            sm.setSourceAddress(new Address((byte)0x00, (byte)0x01,  mDBConnection.getSettings("my_msisdn")));
                        sm.setDestAddress(new Address((byte)0x01, (byte)0x01, client_msisdn));
                        sm.setDataCoding((byte)8);
                        sm.setEsmClass((byte)0);
                        sm.setShortMessage(null);
                        sm.setSequenceNumber(SequenceNumber);
                        sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS, "881010000".getBytes(),"sourcesub_address"));
                        sm.setOptionalParameter(new Tlv(SmppConstants.TAG_MESSAGE_PAYLOAD, textBytes,"messagePayload"));
                        sm.calculateAndSetCommandLength();

                        SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(60));
                        log.debug("SM sent" + sm.toString());

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
                            single_sm.setStatus(-1);
                            mDBConnection.UpdateSMSLine(single_sm);
                        }
                        else {
                            single_sm.setStatus(1);
                            mDBConnection.UpdateSMSLine(single_sm);
                        }
                    }
                    catch (SmppTimeoutException |SmppChannelException
                            | UnrecoverablePduException | InterruptedException | RecoverablePduException ex){
                        log.debug("{}", ex);
                    }
                }
            }
        }
    }
}

package kz.smpp.client;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
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

public class InfoTask implements Runnable {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(InfoTask.class);
    protected Client client;
    private MyDBConnection mDBConnection;



    public InfoTask(Client client, MyDBConnection mDBConn) {
        this.client = client;
        this.mDBConnection = mDBConn;
    }

    @Override
    public void run() {
        if (client.state == ClientState.BOUND) {
            //Задаем временые промежутки когда будет запущена рассылка
            Calendar cal = Calendar.getInstance();
            int currentHour = cal.get(Calendar.HOUR_OF_DAY);
            int currentMinutes = cal.get(Calendar.MINUTE);

            if (currentHour >= 9 && currentHour < 20) {
                SendInfo();
            }
        }
    }

    public void SendInfo(){
        SmppSession session = client.getSession();
        //Месячное сообщение о подписке
        String date =  new SimpleDateFormat("yyyy-MM-dd").format(new Date());

        //All clients with signed date more then month ago
        List<kz.smpp.mysql.client> clnts = mDBConnection.getClientsFromContentType(date);
        for (client single_clnt : clnts) {
            String info_msg = "";
            //Получили шаблон сообщения о месячном информировании
            String msg_info = mDBConnection.getSettings("monthly_update");
            //All client's services
            String servs = mDBConnection.getServiceName(single_clnt.getId());
            switch (servs) {
                case "all":
                    info_msg = mDBConnection.getSettings("AllServices_message_mnth");
                    break;
                default:
                    info_msg = msg_info.replace("?", servs);
                    break;
            }
            SmsLine sm = new SmsLine();
            sm.setSms_body(info_msg);
            sm.setId_client(single_clnt.getId());
            sm.setStatus(100);
            sm.setTransaction_id("");
            sm.setDate(date);
            sm.setRate(mDBConnection.getSettings("0"));
            mDBConnection.setSingleSMS(sm);
        }


        List<SmsLine> SMs= mDBConnection.getSMSLine(100);
        for (SmsLine single_sm: SMs) {
            try{
                log.debug("Send SM");
                int SequenceNumber = 1 + (int)(Math.random() * 32000);
                String client_msisdn =Long.toString(mDBConnection.getClient(single_sm.getId_client()).getAddrs());

                byte[] textBytes = CharsetUtil.encode(single_sm.getSms_body(), "UCS-2");

                SubmitSm sm = new SubmitSm();
                sm.setSourceAddress(new Address((byte)0x00, (byte)0x01,  mDBConnection.getSettings("my_msisdn")));
                sm.setDestAddress(new Address((byte)0x01, (byte)0x01, client_msisdn));
                sm.setDataCoding((byte)8);
                sm.setEsmClass((byte)0);
                sm.setShortMessage(null);
                sm.setSequenceNumber(SequenceNumber);
                sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS,  mDBConnection.getSettings("0").getBytes(),"sourcesub_address"));
                sm.setOptionalParameter(new Tlv(SmppConstants.TAG_MESSAGE_PAYLOAD, textBytes,"messagePayload"));
                sm.calculateAndSetCommandLength();

                SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(client.timeRespond));
                log.debug("SM sent" + sm.toString());

                if (resp.getCommandStatus()!=0){
                    single_sm.setErr_code(Integer.toString(resp.getCommandStatus()));
                    single_sm.setStatus(-101);
                    mDBConnection.UpdateSMSLine(single_sm);
                }
                else {
                    single_sm.setStatus(101);
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

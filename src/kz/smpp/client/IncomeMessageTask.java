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
import kz.smpp.mysql.client;
import kz.smpp.utils.AllUtils;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class IncomeMessageTask implements Runnable {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(IncomeMessageTask.class);
    protected Client client;
    private long msisdn = -1;
    private String message_text;
    private MyDBConnection mDBConnection;
    private AllUtils settings = new AllUtils();
    private int transaction_id;


    public IncomeMessageTask(Client client, long imsisdn, String imessage_text, int tran_id) {
        this.client = client;
        msisdn = imsisdn;
        message_text = imessage_text;
        transaction_id = tran_id;
        mDBConnection = new MyDBConnection();
    }

    @Override
    public void run() {
        if (client.state == ClientState.BOUND) {
           SmppSession session = client.getSession();

            try {
                log.debug("Send SM");
                int SequenceNumber = 1 + (int)(Math.random() * 32000);

                String client_msisdn = Long.toString(msisdn);
                List<SubmitSm> list_sm= settings.CreateLongMessage(settings.getSettings("my_msisdn").concat("#"+transaction_id),
                        client_msisdn,
                        CharsetUtil.encode(message_text, "UCS-2"), SequenceNumber);
                //SubmitSm sm = settings.createSubmitSm( settings.getSettings("my_msisdn").concat("#"+transaction_id),client_msisdn , message_text, "UCS-2", SequenceNumber);
                for (int i =0;i<list_sm.size();i++){
                    SubmitSmResp resp = session.submit(list_sm.get(i), TimeUnit.SECONDS.toMillis(60));
                    log.debug("SM sent successfull",list_sm.get(i).toString());
                    if (resp.getCommandStatus()!=0){
                        log.debug("Submit issue is released");
                        log.debug("{resp}", ""+resp.toString());
                        QuerySm querySm = new QuerySm();
                        querySm.setMessageId(resp.getMessageId());
                        querySm.setSourceAddress(new Address((byte)0x00, (byte)0x01, settings.getSettings("my_msisdn")));
                        querySm.calculateAndSetCommandLength();
                        WindowFuture<Integer,PduRequest,PduResponse> future1 = session.sendRequestPdu(querySm, 10000, true);
                        log.debug("Status request is opened");
                        while (!future1.isDone()) {}
                        QuerySmResp queryResp = (QuerySmResp)future1.getResponse();
                        log.debug("{The answer getMessageState}", ""+ queryResp.toString());
                    }
                }
            }
            catch (SmppTimeoutException |SmppChannelException
                    | UnrecoverablePduException | InterruptedException | RecoverablePduException ex){
                log.debug("{}", ex);
            }
        }
    }
}

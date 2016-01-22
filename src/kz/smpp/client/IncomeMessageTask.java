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
    private int transaction_id;


    public IncomeMessageTask(Client client, long imsisdn, String imessage_text, int tran_id, MyDBConnection mDBConn) {
        this.client = client;
        msisdn = imsisdn;
        message_text = imessage_text;
        transaction_id = tran_id;
        this.mDBConnection = mDBConn;
    }

    @Override
    public void run() {
        if (client.state == ClientState.BOUND) {
           SmppSession session = client.getSession();

            try {
                log.debug("Send SM");
                int SequenceNumber = 1 + (int)(Math.random() * 32000);
                String client_msisdn = Long.toString(msisdn);
                byte[] textBytes = CharsetUtil.encode(message_text, "UCS-2");
                SubmitSm sm = new SubmitSm();
                sm.setSourceAddress(new Address((byte)0x00, (byte)0x01,  mDBConnection.getSettings("my_msisdn").concat("#"+transaction_id)));
                sm.setDestAddress(new Address((byte)0x01, (byte)0x01, client_msisdn));
                sm.setDataCoding((byte)8);
                sm.setEsmClass((byte)0);
                sm.setShortMessage(null);
                sm.setSequenceNumber(SequenceNumber);
                sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS, "881010000".getBytes(),"sourcesub_address"));
                sm.setOptionalParameter(new Tlv(SmppConstants.TAG_MESSAGE_PAYLOAD, textBytes,"messagePayload"));
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
            }
            catch (SmppTimeoutException |SmppChannelException
                    | UnrecoverablePduException | InterruptedException | RecoverablePduException ex){
                log.debug("{}", ex);
            }
        }
    }
}

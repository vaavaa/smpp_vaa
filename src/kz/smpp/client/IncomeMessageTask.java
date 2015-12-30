package kz.smpp.client;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.EnquireLink;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.type.*;
import kz.smpp.mysql.ContentType;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.client;
import kz.smpp.utils.AllUtils;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class IncomeMessageTask implements Runnable {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(ElinkTask.class);
    protected Client client;
    private long msisdn = -1;
    private String message_text;
    private MyDBConnection mDBConnection;
    private AllUtils settings = new AllUtils();

    public IncomeMessageTask(Client client, long imsisdn, String imessage_text) {
        this.client = client;
        msisdn = imsisdn;
        message_text = imessage_text;
        mDBConnection = new MyDBConnection();
    }

    @Override
    public void run() {
        if (client.state == ClientState.BOUND) {
            SmppSession session = client.getSession();

            log.debug("Send SM");

            try {
                kz.smpp.mysql.client clnt = mDBConnection.setNewClient(msisdn);
                LinkedList<ContentType> llct= mDBConnection.getClientsContentTypes(clnt);
                ContentType contentType = mDBConnection.getContentType("content_ascendant");
                if (llct.size()==0){
                    mDBConnection.setNewClientsContentTypes(clnt, contentType);
                }
                SubmitSm sm = createSubmitSm( settings.getSettings("my_msisdn"), Long.toString(msisdn), message_text, "UCS-2");
                session.submit(sm, TimeUnit.SECONDS.toMillis(60));

                log.debug("SM sent successfull");
            } catch (RecoverablePduException ex) {
                log.debug("{}", ex);}
        }
    }
    public SubmitSm createSubmitSm(String src, String dst, String text, String charset) throws SmppInvalidArgumentException {
        SubmitSm sm = new SubmitSm();

        // Для цифровых номеров указывается TON=0, NPI=1 (source_addr)
        // TON=0
        // NPI=1
        sm.setSourceAddress(new Address((byte)0, (byte)1, src));

        // For national numbers will use
        // TON=1
        // NPI=1
        sm.setDestAddress(new Address((byte)1, (byte)1, dst));

        // Set datacoding to UCS-2
        sm.setDataCoding((byte)8);

        // Encode text
        sm.setShortMessage(CharsetUtil.encode(text, charset));

        return sm;
    }
}

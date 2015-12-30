package kz.smpp.va;


import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.type.*;
import kz.smpp.mysql.ContentType;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.client;
import kz.smpp.utils.AllUtils;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class Activity3200 implements Runnable {
    private long msisdn = -1;
    private String message_text;
    private MyDBConnection mDBConnection;
    private SmppSession session;
    private AllUtils settings = new AllUtils();

    public Activity3200(long imsisdn, String imessage_text, SmppSession isession ){
        msisdn = imsisdn;
        message_text = imessage_text;
        mDBConnection = new MyDBConnection();
        session = isession;
    }

    @Override
    public void run() {

        client clnt = mDBConnection.setNewClient(msisdn);
        LinkedList<ContentType> llct= mDBConnection.getClientsContentTypes(clnt);
        ContentType contentType = mDBConnection.getContentType("content_ascendant");
        if (llct.size()==0){
            mDBConnection.setNewClientsContentTypes(clnt, contentType);
        }
        SubmitSm sm = createSubmitSm( settings.getSettings("my_msisdn"), Long.toString(msisdn), message_text, "UCS-2");
    }
    public SubmitSm createSubmitSm(String src, String dst, String text, String charset) {
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

        try {
            // Encode text
            sm.setShortMessage(CharsetUtil.encode(text, charset));
            session.submit(sm, TimeUnit.SECONDS.toMillis(60));
        }
        catch (SmppInvalidArgumentException ex){}
        catch (SmppTimeoutException ex) {}
        catch (SmppChannelException ex) {}
        catch (SmppBindException ex) {}
        catch (UnrecoverablePduException ex) {}
        catch (InterruptedException ex) {}
        catch (RecoverablePduException ex) {
            System.err.print(ex);
        }

        return sm;
    }
    
}

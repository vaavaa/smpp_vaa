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
import java.util.concurrent.TimeUnit;

public class IncomeMessageTask implements Runnable {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(IncomeMessageTask.class);
    protected Client client;
    private long msisdn = -1;
    private String message_text;
    private MyDBConnection mDBConnection;
    private AllUtils settings = new AllUtils();
    private String[] table_names;
    private int transaction_id;


    public IncomeMessageTask(Client client, long imsisdn, String imessage_text, int tran_id) {
        this.client = client;
        msisdn = imsisdn;
        message_text = imessage_text;
        transaction_id = tran_id;
        mDBConnection = new MyDBConnection();
        table_names= new String[Integer.parseInt(settings.getSettings("ServicesCount"))];
        String services =  settings.getSettings("AvailableServices");
        int i = 0;
        while (services.lastIndexOf(";")>=0)
        {
            table_names[i]=services.substring(services.lastIndexOf(";")+1,services.length());
            services = services.substring(0,services.lastIndexOf(";"));
            i++;
        }
    }

    @Override
    public void run() {
        if (client.state == ClientState.BOUND) {
            ContentType contentType;
            contentType=  mDBConnection.getContentType("content_anecdot");
            SmppSession session = client.getSession();

            try {
                client clnt = mDBConnection.setNewClient(msisdn);
                LinkedList<ContentType> llct= mDBConnection.getClientsContentTypes(clnt);
                if (llct.size()==0){
                    contentType = mDBConnection.getContentType(settings.getSettings("FirstService"));
                    mDBConnection.setNewClientsContentTypes(clnt, contentType);
                }
                else {
                    for (int j = 0; j<=table_names.length-1;j++) {
                        for (ContentType ct : llct) {
                            if (ct.getTable_name().equals(table_names[j])){
                                table_names[j] = "";
                                break;
                            }

                        }
                    }
                    for (String str:table_names) {
                        if (str.length()>0)
                        {
                            contentType = mDBConnection.getContentType(str);
                            mDBConnection.setNewClientsContentTypes(clnt, contentType);
                            break;
                        }
                    }
                }
                log.debug("Send SM");
                String serviceName;
                if (contentType.getName().length()==0) serviceName = settings.getSettings("AllServices");
                else serviceName = contentType.getName();
                message_text = "test";//message_text.replaceAll("/?",serviceName);
                int SequenceNumber = 1 + (int)(Math.random() * 32000);

                String client_msisdn = Long.toString(msisdn);
                SubmitSm sm = createSubmitSm( settings.getSettings("my_msisdn").concat("#"+transaction_id),client_msisdn , message_text, "UCS-2", SequenceNumber);
                Tlv tlv = new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS, "3200".getBytes(),"3200");
                sm.setOptionalParameter(tlv);

                //Эта жопа не считала.
                sm.calculateAndSetCommandLength();
                log.debug("print SM "+sm.toString());
                SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(60));
                log.debug("SM sent successfull",sm);
                if (resp.getCommandStatus()!=0){
                    log.debug("Submit issue is released");
                    log.debug("{getCommandStatus}", ""+resp.getCommandStatus());
                    log.debug("{getMessageId}", ""+ resp.getMessageId());
                    QuerySm querySm = new QuerySm();
                    querySm.setMessageId(resp.getMessageId());
                    querySm.setSourceAddress(new Address((byte)0x00, (byte)0x01, settings.getSettings("my_msisdn")));

                    WindowFuture<Integer,PduRequest,PduResponse> future1 = session.sendRequestPdu(querySm, 10000, true);
                    log.debug("Status request is opened");
                    while (!future1.isDone()) {}
                    QuerySmResp queryResp = (QuerySmResp)future1.getResponse();
                    log.debug("{The answer getMessageState}", ""+ queryResp.getMessageState());
                }

            }
            catch (SmppTimeoutException |SmppChannelException
                    | UnrecoverablePduException | InterruptedException | RecoverablePduException ex){
                log.debug("{}", ex);
            }
        }
    }
    public SubmitSm createSubmitSm(String src, String dst, String text, String charset, int SequenceNumber) throws SmppInvalidArgumentException {
        SubmitSm sm = new SubmitSm();
        // Для цифровых номеров указывается TON=0, NPI=1 (source_addr)
        // TON=0
        // NPI=1
        sm.setSourceAddress(new Address((byte)0x00, (byte)0x01, src));
        // For national numbers will use
        // TON=1
        // NPI=1
        sm.setDestAddress(new Address((byte)0x01, (byte)0x01, dst));
        // Set datacoding to UCS-2
        sm.setDataCoding((byte)0);
        sm.setSequenceNumber(SequenceNumber);

        // Encode text
        sm.setShortMessage(CharsetUtil.encode(text, charset));
        sm.setEsmClass((byte)0);

        return sm;
    }
}

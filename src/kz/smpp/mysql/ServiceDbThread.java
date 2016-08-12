package kz.smpp.mysql;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.util.ByteArrayUtil;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.*;
import kz.smpp.client.Client;
import kz.smpp.client.ClientState;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ServiceDbThread implements Callable<Integer> {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(ServiceDbThread.class);

    private List<SmsLine> Acl;
    protected Client client;
    private MyDBConnection mDBConnection;
    private int content_type;

    public ServiceDbThread(List<SmsLine> AcL, Client client, int content_type) {
        this.Acl = AcL;
        this.client = client;
        mDBConnection = new MyDBConnection();
        this.content_type = content_type;
    }

    @Override
    public Integer call() {
        if (Acl.size() > 0) {
            SmppSession session = client.getSession();
            for (SmsLine sml : Acl) {
                try {

                    int SequenceNumber = 1 + (int) (Math.random() * 32000);
                    String client_msisdn = Long.toString(mDBConnection.getClient(sml.getId_client()).getAddrs());

                    byte[] textBytes = CharsetUtil.encode(sml.getSms_body(), "UCS-2");

                    String source_address = mDBConnection.getContentTypeById(content_type).getService_code();
                    client.ServiceSendTask_TimeStamp = Calendar.getInstance().getTimeInMillis();

                    SubmitSm sm = new SubmitSm();
                    sm.setSourceAddress(new Address((byte) 0x00, (byte) 0x01, source_address));
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
                        SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(40000));
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
                    log.debug("System's error, sending failure ", ex);
                }
            }
        }
        return 1;
    }
}

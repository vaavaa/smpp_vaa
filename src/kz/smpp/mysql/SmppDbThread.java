package kz.smpp.mysql;

import com.cloudhopper.commons.util.ByteArrayUtil;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.*;
import kz.smpp.client.Client;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class SmppDbThread implements Callable<Integer> {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(SmppDbThread.class);

    private List<ActionClient> Acl;
    protected Client client;
    private MyDBConnection mDBConnection;

    public SmppDbThread(List<ActionClient> AcL, Client client) {
        this.Acl = AcL;
        this.client = client;
        mDBConnection = new MyDBConnection();
    }

    @Override
    public Integer call() {
        if (Acl.size() > 0) {
            SmppSession session = client.getSession();
            for (ActionClient clnt : Acl) {
                //получаем первый попавшийся адресс подписки абонента
                String source_address = mDBConnection.getContentTypeById(clnt.getContentType()).getService_code();
                client.DeadSessionTask_TimeStamp = Calendar.getInstance().getTimeInMillis();
                if (source_address != null) {
                    try {
                        int SequenceNumber = 1 + (int) (Math.random() * 32000);

                        long client_address = mDBConnection.getClient(clnt.getClientId()).getAddrs();

                        SubmitSm sm = new SubmitSm();
                        sm.setSourceAddress(new Address((byte) 0x01, (byte) 0x01, source_address));
                        sm.setDestAddress(new Address((byte) 0x00, (byte) 0x01, "" + client_address));
                        sm.setDataCoding((byte) 8);
                        sm.setEsmClass((byte) 0);
                        sm.setShortMessage(null);
                        sm.setSequenceNumber(SequenceNumber);
                        //Sing and Quit
                        if (clnt.getActionType() == 1)
                            //подписка
                            sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_PORT, ByteArrayUtil.toByteArray((short) 3)));
                        else
                            //Отписка
                            sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_PORT, ByteArrayUtil.toByteArray((short) 4)));

                        sm.calculateAndSetCommandLength();

                        log.debug("Send SM to operator");

                        //Указываем сразу ошибку отправки на случай неконтролируемого сбоя
                        if (!session.isClosed() && !session.isUnbinding()) {
                            if (clnt.getActionType() == 2) {
                                if (mDBConnection.RemoveClientContentType(clnt.getClientId(), clnt.getContentType())) {
                                    String text_message = mDBConnection.getSettings("goodbye_message_3200");
                                    text_message = text_message.replace("?", "");
                                    FillSmsLine(clnt.getClientId(), "", text_message,
                                            "Sdel " + clnt.getContentType(), clnt.getClientId());
                                }
                            }
                            SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(40000));
                            if (resp.getCommandStatus() == 0) {
                            }
                            if (clnt.getActionType() == 1) {
                                ContentType ct1 = mDBConnection.getContentType("content_ascendant_31");
                                String text_message = mDBConnection.getSettings("ascendant_welcome_kz_31");
                                FillSmsLine(clnt.getClientId(), "", text_message,
                                        text_message, ct1.getId());
                            }


                            mDBConnection.UpdateClientsOperator(client_address, resp.getCommandStatus());
                        }
                    } catch (SmppTimeoutException | SmppChannelException
                            | UnrecoverablePduException | InterruptedException | RecoverablePduException ex) {
                    }
                }
            }
        }
        return 1;
    }

    private void FillSmsLine(int client_id, String transaction_id, String MessageToSend, String StcMsg, int service_id) {
        SmsLine StopSms = new SmsLine();
        StopSms.setStatus(0);
        StopSms.setSms_body(MessageToSend);
        StopSms.setId_client(client_id);
        StopSms.setTransaction_id(transaction_id);
        StopSms.setServiceId(service_id);
        mDBConnection.setSingleSMS(StopSms, StcMsg);
    }
}

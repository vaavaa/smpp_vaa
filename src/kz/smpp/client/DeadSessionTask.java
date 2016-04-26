package kz.smpp.client;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.util.ByteArrayUtil;
import com.cloudhopper.commons.util.windowing.WindowFuture;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.*;
import com.sun.javaws.exceptions.InvalidArgumentException;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.SmsLine;
import kz.smpp.mysql.client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DeadSessionTask implements Runnable {

    public static final Logger log = LoggerFactory.getLogger(DeadSessionTask.class);
    MyDBConnection mDBConnection;
    Client client;

    public DeadSessionTask(Client client, MyDBConnection mDBConn) {
        mDBConnection = new MyDBConnection();
        this.client = client;
    }

    @Override
    public void run() {
        //Помечаем к отправке информационных сообщений устаревшие сессии
        if (Calendar.getInstance().getTimeInMillis() > (client.DeadSessionTask_TimeStamp + 15000)) {
            //mDBConnection.getFollowUpLine();
            mDBConnection.RemoveDeadSessions();
            log.debug("Session line is cleared");

            List<kz.smpp.mysql.client> clientList = mDBConnection.getClientsOperator();
            if (clientList.size() > 0) {
                if (client.state == ClientState.BOUND) {
                    SmppSession session = client.getSession();
                    for (kz.smpp.mysql.client clnt : clientList) {
                        //получаем первый попавшийся адресс подписки абонента
                        String sourceAddress = "32004"; //mDBConnection.getClientsContentTypes(clnt).getFirst().getService_code();
                        if (sourceAddress != null) {
                            try {
                                int SequenceNumber = 1 + (int) (Math.random() * 32000);

                                SubmitSm sm = new SubmitSm();
                                sm.setSourceAddress(new Address((byte) 0x01, (byte) 0x01, sourceAddress));
                                sm.setDestAddress(new Address((byte) 0x00, (byte) 0x01, "" + clnt.getAddrs()));
                                sm.setDataCoding((byte) 8);
                                sm.setEsmClass((byte) 0);
                                sm.setShortMessage(null);
                                sm.setSequenceNumber(SequenceNumber);

                                sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_PORT, ByteArrayUtil.toByteArray((short) 3)));
                                sm.calculateAndSetCommandLength();

                                log.debug("Send SM");

                                //Указываем сразу ошибку отправки на случай неконтролируемого сбоя
                                if (!session.isClosed() && !session.isUnbinding()) {
                                    SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(client.timeRespond));
                                    mDBConnection.UpdateClientsOperator(clnt);
                                }
                            } catch (SmppTimeoutException | SmppChannelException
                                    | UnrecoverablePduException | InterruptedException | RecoverablePduException ex) {

                            }
                        }
                    }
                }
            }

            client.DeadSessionTask_TimeStamp = Calendar.getInstance().getTimeInMillis();
        }

//        if (client.state == ClientState.BOUND) {
//            SmppSession session = client.getSession();
//            List<SmsLine> SMs = mDBConnection.getClientsOperator();
//            for (SmsLine single_sm : SMs) {
//                try {
//                    int SequenceNumber = 1 + (int) (Math.random() * 32000);
//                    String client_msisdn = Long.toString(mDBConnection.getClient(single_sm.getId_client()).getAddrs());
//
//                    byte[] textBytes = CharsetUtil.encode(single_sm.getSms_body(), "UCS-2");
//
//                    SubmitSm sm = new SubmitSm();
//                    sm.setSourceAddress(new Address((byte) 0x00, (byte) 0x01, "3200"));
//                    sm.setDestAddress(new Address((byte) 0x00, (byte) 0x01, "1529"));
//                    sm.setDataCoding((byte) 8);
//                    sm.setEsmClass((byte) 0);
//                    sm.setShortMessage(null);
//                    sm.setSequenceNumber(SequenceNumber);
//                    sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS, mDBConnection.getSettings("0").getBytes(), "sourcesub_address"));
//                    sm.setOptionalParameter(new Tlv(SmppConstants.TAG_MESSAGE_PAYLOAD, textBytes, "messagePayload"));
//                    sm.calculateAndSetCommandLength();
//
//                    log.debug("Send SM to 1529");
//
//                    //Указываем сразу ошибку отправки на случай неконтролируемого сбоя
//                    if (!session.isClosed() && !session.isUnbinding()){
//                        SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(client.timeRespond));
//                    }
//
//                } catch (SmppTimeoutException | SmppChannelException
//                        | UnrecoverablePduException | InterruptedException | RecoverablePduException ex) {
//                    log.debug("System's error, sending failure ", ex);
//                }
//            }
//        }
    }

    static byte[] bigIntToByteArray(final int i) {
        BigInteger bigInt = BigInteger.valueOf(i);
        return bigInt.toByteArray();
    }
}


package kz.smpp.va;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.PduAsyncResponse;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.type.Address;
import kz.smpp.client.Client;
import kz.smpp.utils.AllUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySmppSessionHandler extends DefaultSmppSessionHandler {
    public static Logger log = LoggerFactory.getLogger(MySmppSessionHandler.class);
    protected Client client;
    private AllUtils settings = new AllUtils();

    public MySmppSessionHandler(Client client) {
        this.client = client;
    }

    @Override
    public PduResponse firePduRequestReceived(PduRequest pduRequest) {
        if (pduRequest.isRequest() && pduRequest.getClass() == DeliverSm.class) {
            log.debug("Got DELIVER_SM");

            DeliverSm dlr = (DeliverSm)pduRequest;

            byte[] textMessage = dlr.getShortMessage();

            //Получили текст сообщения
            String textBytes = CharsetUtil.decode(textMessage, CharsetUtil.CHARSET_UCS_2);
            String transaction_id = dlr.getDestAddress().getAddress();
            transaction_id = transaction_id.substring(transaction_id.lastIndexOf("#")+1,transaction_id.length());

            //Запускаем цепочку обработки входящего сообщения и ответа не него
            String arrd = dlr.getSourceAddress().getAddress();
            client.runIncomeMessageTask(Long.parseLong(arrd),settings.getSettings("welcome_message_durt"),Integer.parseInt(transaction_id));

            PduResponse DSR = pduRequest.createResponse();
            //Set back SequenceNumber
            DSR.setSequenceNumber(dlr.getSequenceNumber());
            return DSR;
        }
        if (pduRequest.isRequest() && pduRequest.getClass() == EnquireLink.class) {
            EnquireLink el = (EnquireLink)pduRequest;
            EnquireLinkResp elr  = new EnquireLinkResp();
            elr.setSequenceNumber(el.getSequenceNumber());
            return elr;
        }

        return super.firePduRequestReceived(pduRequest);
    }

    @Override
    public void fireExpectedPduResponseReceived(PduAsyncResponse pduAsyncResponse) {
        if (pduAsyncResponse.getResponse().getClass() == SubmitSmResp.class) {
            SubmitSm req = (SubmitSm)pduAsyncResponse.getRequest();
            log.debug("Got response for APPID={}", req.getReferenceObject());

            SubmitSmResp ssmr = (SubmitSmResp)pduAsyncResponse.getResponse();

            log.debug("Got response with MSG ID={} for seqnum={}", ssmr.getMessageId(), ssmr.getSequenceNumber());
        }
    }

    @Override
    public void fireChannelUnexpectedlyClosed() {
        client.bind();
    }

}

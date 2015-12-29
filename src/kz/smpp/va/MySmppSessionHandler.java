package kz.smpp.va;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.PduAsyncResponse;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.*;
import kz.smpp.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySmppSessionHandler extends DefaultSmppSessionHandler {
    public static Logger log = LoggerFactory.getLogger(MySmppSessionHandler.class);

    protected Client client;

    public MySmppSessionHandler(Client client) {
        this.client = client;
    }

    @Override
    public PduResponse firePduRequestReceived(PduRequest pduRequest) {
        if (
                pduRequest.isRequest()
                        && pduRequest.getClass() == DeliverSm.class
                ) {
            log.debug("Got DELIVER_SM");

            DeliverSm dlr = (DeliverSm)pduRequest;

            byte[] textMessage = dlr.getShortMessage();

            //Получили текст сообщения
            String textBytes ="";
            textBytes = CharsetUtil.decode(textMessage, CharsetUtil.CHARSET_UCS_2);

            dlr.getSequenceNumber()

            return pduRequest.createResponse();
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

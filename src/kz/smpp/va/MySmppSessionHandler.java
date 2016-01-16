package kz.smpp.va;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.PduAsyncResponse;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.type.Address;
import kz.smpp.client.Client;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.utils.AllUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySmppSessionHandler extends DefaultSmppSessionHandler {
    public static Logger log = LoggerFactory.getLogger(MySmppSessionHandler.class);
    protected Client client;
    private AllUtils settings = new AllUtils();
    MyDBConnection mDBConnection = new MyDBConnection();
    String text_message="";

    public MySmppSessionHandler(Client client) {
        this.client = client;
    }

    @Override
    public PduResponse firePduRequestReceived(PduRequest pduRequest) {
        if (pduRequest.isRequest() && pduRequest.getClass() == DeliverSm.class) {
            log.debug("Got DELIVER_SM");

            DeliverSm dlr = (DeliverSm)pduRequest;

            Integer command_respond = 0x600; //по умолчанию ошибка на нашей стороне
//            0x00 — если абонент подписался на услугу;
//            0x551 — если абонент запрашивает повторную подписку или отписку на услугу;
//            0x552 — если абонент запрашивает подписку или отписку на услугу с указанием неправильного номера услуги;
//            0x553 — если абонент запрашивает подписку или отписку на услугу с указанием неверных опциональных параметров;
//            0x554 — если абонент запрашивает подписку или отписку на услугу, но подписка/отписка в данный момент не может быть произведена;
//            0x555 — если абонент запрашивает подписку или отписку на услугу, но в подписке/отписке отказано;
//            0x556 — если абонент запрашивает подписку или отписку на услугу при помощи некорректного запроса;
//            0x600 — используется в случае сетевой или системной ошибки на стороне партнера.

            byte[] textMessage = dlr.getShortMessage();

            String transaction_id = dlr.getDestAddress().getAddress();
            transaction_id = transaction_id.substring(transaction_id.lastIndexOf("#")+1,transaction_id.length());
            String arrd = dlr.getSourceAddress().getAddress();
            //Переводим в лонг что бы тысячу раз не переводить
            Long l_addr = Long.parseLong(arrd);


            //Получили текст сообщения
            String textBytes = CharsetUtil.decode(textMessage, CharsetUtil.CHARSET_UCS_2);
            switch (textBytes){
                case "STOP":
                    //Запускаем цепочку обработки входящего сообщения в случае если стоп
                    client.runIncomeMessageTask(l_addr,settings.getSettings("message_stop"),Integer.parseInt(transaction_id));
                    break;
                default:
                    //Получаем на что абонент подписался
                    String service = mDBConnection.SignServiceName(l_addr);
                    //Если он на все подписан
                    if (service.equals("all")) {
                        command_respond = 0x551;
                        text_message = settings.getSettings("AllServices_message");
                    }
                    else {
                        command_respond = 0x00;
                        text_message = settings.getSettings("welcome_message_3200");
                        text_message = text_message.replace("?",service);
                    }
                    //Запускаем цепочку обработки входящего сообщения и ответа не него в случае если подписаться
                    client.runIncomeMessageTask(l_addr,text_message,Integer.parseInt(transaction_id));
                    break;
            }


            PduResponse DSR = pduRequest.createResponse();
            //Set back SequenceNumber
            DSR.setSequenceNumber(dlr.getSequenceNumber());
            DSR.setCommandStatus(command_respond);
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

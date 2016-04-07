package kz.smpp.va;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.PduAsyncResponse;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.type.Address;
import kz.smpp.client.Client;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.SmsLine;
import kz.smpp.utils.AllUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySmppSessionHandler extends DefaultSmppSessionHandler {
    public static Logger log = LoggerFactory.getLogger(MySmppSessionHandler.class);
    protected Client client;
    MyDBConnection mDBConnection;
    String text_message = "";

    public MySmppSessionHandler(Client client, MyDBConnection mDBCon) {
        this.client = client;
        this.mDBConnection = mDBCon;
    }

    @Override
    public PduResponse firePduRequestReceived(PduRequest pduRequest) {
        if (pduRequest.isRequest() && pduRequest.getClass() == DeliverSm.class) {
            log.debug("Got DELIVER_SM");

            //кодовое слово о выводе информации о сервисе
            String service_info_word = mDBConnection.getSettings("service_info_word");
            DeliverSm dlr = (DeliverSm) pduRequest;
            int command_respond = 0x00;

            PduResponse DSR = pduRequest.createResponse();
            DSR.setSequenceNumber(dlr.getSequenceNumber());


//           Integer command_respond = 0x600; //по умолчанию ошибка на нашей стороне
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
            transaction_id = transaction_id.substring(transaction_id.lastIndexOf("#") + 1, transaction_id.length());
            String arrd = dlr.getSourceAddress().getAddress();
            //Переводим в лонг что бы тысячу раз не переводить
            Long l_addr = Long.parseLong(arrd);


            //Получили текст сообщения c проверкой кодировки
            String textBytes = "";
            if (dlr.getDataCoding() == 0x08) textBytes = CharsetUtil.decode(textMessage, "UCS-2");
            else textBytes = CharsetUtil.decode(textMessage, "GSM");

            if (!dlr.getSourceAddress().getAddress().equals("1529")) {
                switch (textBytes.toLowerCase().trim()) {
                    case "стоп":
                    case "stop":
                        //Запускаем цепочку обработки входящего сообщения в случае если стоп
                        if (mDBConnection.RemoveServiceName(l_addr)) {
                            SmsLine StopSms = new SmsLine();
                            StopSms.setStatus(0);
                            StopSms.setSms_body(mDBConnection.getSettings("message_stop").replace("?", ""));
                            StopSms.setId_client(mDBConnection.getClient(l_addr).getId());
                            StopSms.setTransaction_id(transaction_id);
                            mDBConnection.setSingleSMS(StopSms, textBytes);
                            //Далее эту ветку обработает нить которая отправляет СМC которая берет из базы
                        }
                        break;
                    default:
                        if (textBytes.toLowerCase().equalsIgnoreCase(service_info_word)) {
                            //Запускаем цепочку обработки входящего сообщения запроса о статусе
                            SmsLine StopSms = new SmsLine();
                            StopSms.setStatus(0);
                            StopSms.setSms_body(mDBConnection.BCR());
                            StopSms.setId_client(mDBConnection.getClient(l_addr).getId());
                            StopSms.setTransaction_id(transaction_id);
                            mDBConnection.setSingleSMS(StopSms, textBytes);
                            //Далее эту ветку обработает нить которая отправляет СМC которая берет из базы
                        } else {
                            if (textBytes.lastIndexOf("dlvrd:") > 0) {
                            } //Если пришел ответ от quiet смс, тогда ни чего не делаем
                            else {
                                //Получаем на что абонент подписался
                                String service = mDBConnection.SignServiceName(l_addr, textBytes);
                                //Если он на все подписан
                                if (service.equals("all")) {
                                    text_message = mDBConnection.getSettings("AllServices_message");
                                } else {
                                    text_message = mDBConnection.getSettings("welcome_message_3200");
                                    text_message = text_message.replace("?", service);
                                }
                                SmsLine WelcomeSms = new SmsLine();
                                WelcomeSms.setStatus(0);
                                WelcomeSms.setSms_body(text_message);
                                WelcomeSms.setId_client(mDBConnection.getClient(l_addr).getId());
                                WelcomeSms.setTransaction_id(transaction_id);
                                mDBConnection.setSingleSMS(WelcomeSms, textBytes);
                            }
                        }
                        break;
                }
            } else {
                log.debug("Got DELIVER_SM from 1529");
                switch (textBytes.toLowerCase().trim()) {
                    case "start":
                        mDBConnection.RemoveClientRegistration(mDBConnection.getClient(l_addr).getId());
                        command_respond = 0x00;
                        break;
                }

                DSR.setCommandStatus(command_respond);
            }

            return DSR;
        }
        if (pduRequest.isRequest() && pduRequest.getClass() == EnquireLink.class) {
            EnquireLink el = (EnquireLink) pduRequest;
            EnquireLinkResp elr = new EnquireLinkResp();
            elr.setSequenceNumber(el.getSequenceNumber());
            return elr;
        }

        return super.firePduRequestReceived(pduRequest);
    }

    @Override
    public void fireExpectedPduResponseReceived(PduAsyncResponse pduAsyncResponse) {
        if (pduAsyncResponse.getResponse().getClass() == SubmitSmResp.class) {
            SubmitSm req = (SubmitSm) pduAsyncResponse.getRequest();
            log.debug("Got response for APPID={}", req.getReferenceObject());

            SubmitSmResp ssmr = (SubmitSmResp) pduAsyncResponse.getResponse();

            log.debug("Got response with MSG ID={} for seqnum={}", ssmr.getMessageId(), ssmr.getSequenceNumber());
        }
    }

    @Override
    public void fireChannelUnexpectedlyClosed() {
        client.bind();
    }

}

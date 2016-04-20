package kz.smpp.va;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.PduAsyncResponse;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.tlv.TlvConvertException;
import com.cloudhopper.smpp.type.Address;
import kz.smpp.client.Client;
import kz.smpp.client.ClientState;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.SmsLine;
import kz.smpp.utils.AllUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MySmppSessionHandler extends DefaultSmppSessionHandler {
    public static Logger log = LoggerFactory.getLogger(MySmppSessionHandler.class);
    protected Client client;
    MyDBConnection mDBConnection;
    String text_message = "";

    String service_word_stop = "";
    String code_in = "";
    String code_out = "";
    String code_payment = "";

    public MySmppSessionHandler(Client client, MyDBConnection mDBCon) {
        this.client = client;
        this.mDBConnection = new MyDBConnection();
        //Слово блокировка
        this.service_word_stop = mDBConnection.getSettings("service_word_stop");
        this.code_in = mDBConnection.getSettings("code_in");
        this.code_out = mDBConnection.getSettings("code_out");
        this.code_payment = mDBConnection.getSettings("code_payment");

    }

    @Override
    public PduResponse firePduRequestReceived(PduRequest pduRequest) {
        if (pduRequest.isRequest() && pduRequest.getClass() == DeliverSm.class) {
            log.debug("Got DELIVER_SM");

            //кодовое слово о выводе информации о сервисе
            DeliverSm dlr = (DeliverSm) pduRequest;
            //ok, команда ответа
            int command_respond = 0x00;

            //Формируем ответ
            PduResponse DSR = pduRequest.createResponse();
            DSR.setSequenceNumber(dlr.getSequenceNumber());

            String transaction_id = dlr.getDestAddress().getAddress();
            if (transaction_id.lastIndexOf("#") > 0)
                transaction_id = transaction_id.substring(transaction_id.lastIndexOf("#") + 1, transaction_id.length());

            String dest_addr = dlr.getDestAddress().getAddress();
            dest_addr = dest_addr.substring(0, dest_addr.lastIndexOf("#"));

            //Переводим в long отправителя - это наш абонент
            String arrd = dlr.getSourceAddress().getAddress();
            Long l_addr = Long.parseLong(arrd);

            //Ид клиента, в нашей системе, если клиеента нет - будет создан.
            int client_id = mDBConnection.setNewClient(l_addr).getId();
            if (client_id==0) client_id = mDBConnection.getClient(l_addr).getId();

            byte[] textMessage = dlr.getShortMessage();
            //Получили текст сообщения c проверкой кодировки
            String textBytes = "";
            if (dlr.getDataCoding() == 0x08) textBytes = CharsetUtil.decode(textMessage, "UCS-2");
            else textBytes = CharsetUtil.decode(textMessage, "GSM");
            //Адрес получатель может быть:
            //32001 - гороскоп DA1
            //32002 - Курс валют DA2
            //32003 - Прогноз погоды DA3
            //32004 - Анекдот DA4
            //32005 - информация
            switch (textBytes.toLowerCase().trim()) {
                case "da1":
                    FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("ascendant_welcome"), textBytes);
                    break;
                case "da2":
                    FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("rate_welcome"), textBytes);
                    break;
                case "da3":
                    FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("metcast_welcome"), textBytes);
                    break;
                case "da4":
                    FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("anecdote_welcome"), textBytes);
                    break;
                case "32005":
                    FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("info"), textBytes);
                    break;
                case "стоп":
                case "stop":
                    if (mDBConnection.RemoveServiceName(l_addr)) {
                        SmsLine StopSms = new SmsLine();
                        StopSms.setStatus(0);
                        StopSms.setSms_body(mDBConnection.getSettings("message_stop").replace("?", ""));
                        StopSms.setId_client(client_id);
                        StopSms.setTransaction_id(transaction_id);
                        mDBConnection.setSingleSMS(StopSms, textBytes);
                        //Далее эту ветку обработает нить которая отправляет СМC которая берет из базы
                    }
                    break;
                default:
                    if (textBytes.lastIndexOf("20") > 0) {
                        mDBConnection.setActivityLog(client_id, textBytes);
                        String currdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
                        SmsLine smLn = new SmsLine();
                        smLn.setId_client(client_id);
                        //Статус ожидает обработки, 1 - обработка закончена
                        smLn.setStatus(1);
                        //Тип контента
                        switch (dest_addr) {
                            case "32001":
                                smLn.setRate("4");
                                break;
                            case "32002":
                                smLn.setRate("3");
                                break;
                            case "32003":
                                smLn.setRate("5");
                                break;
                            case "32004":
                                smLn.setRate("2");
                                break;
                        }
                        //Сумма которую снимаем с клиента
                        smLn.setTransaction_id("20");
                        //Текущая дата
                        smLn.setDate(currdate);
                        mDBConnection.setUpdateSingleSMSHidden(smLn);
                    }
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
                        FillSmsLine(client_id, transaction_id, text_message, textBytes);
                    }
                    break;
            }
            DSR.setCommandStatus(command_respond);
            return DSR;
        }

        if (pduRequest.isRequest() && pduRequest.getClass() == EnquireLink.class)

        {
            EnquireLink el = (EnquireLink) pduRequest;
            EnquireLinkResp elr = new EnquireLinkResp();
            elr.setSequenceNumber(el.getSequenceNumber());
            return elr;
        }

        return super.

                firePduRequestReceived(pduRequest);

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

    public void FillSmsLine(int client_id, String transaction_id, String MessageToSend, String StcMsg) {
        SmsLine StopSms = new SmsLine();
        StopSms.setStatus(0);
        StopSms.setSms_body(MessageToSend);
        StopSms.setId_client(client_id);
        StopSms.setTransaction_id(transaction_id);
        mDBConnection.setSingleSMS(StopSms, StcMsg);
    }

}

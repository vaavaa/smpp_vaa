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
import kz.smpp.mysql.ContentType;
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
            log.debug("GOT DELIVER_SM");

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
            log.debug("Log 5");
            //Ид клиента, в нашей системе, если клиеента нет - будет создан.
            int client_id = mDBConnection.setNewClient(l_addr).getId();
            if (client_id == 0) client_id = mDBConnection.getClient(l_addr).getId();
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
                case "da":
                    break;
                case "да1":
                    int kaz_id = mDBConnection.getContentType("content_ascendant_kz").getId();
                    if (mDBConnection.GetClientType(client_id, kaz_id)) {
                        FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("ascendant_welcome_kz"),
                                textBytes, kaz_id);
                    } else {

                        FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("ascendant_welcome"),
                                textBytes, mDBConnection.getContentType("content_ascendant").getId());
                    }
                    break;
                case "да2":
                    int days_word = mDBConnection.getContentType("content_word").getId();
                    if (!mDBConnection.GetClientType(client_id, days_word)) {
                        FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("rate_welcome"),
                                textBytes, mDBConnection.getContentType("content_rate").getId());
                    }else {
                        FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("word_welcome"),
                                textBytes, mDBConnection.getContentType("content_word").getId());
                    }
                    break;
                case "да3":
                    FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("metcast_welcome"),
                            textBytes, mDBConnection.getContentType("content_metcast").getId());
                    break;
                case "да4":
                    int service_id = mDBConnection.getContentType("iphone_news").getId();
                    if (!mDBConnection.GetClientType(client_id, service_id)) {
                        FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("anecdote_welcome"),
                                textBytes, mDBConnection.getContentType("content_anecdot").getId());
                    } else {
                        FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("iphone_welcome"),
                                textBytes, service_id);
                    }

                    break;
                case "да5":
                    FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("info"),
                            textBytes, mDBConnection.getContentType("info_table").getId());
                    break;
                case "да6":
                    FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("ascendant_welcome_kz"),
                            textBytes, mDBConnection.getContentType("content_ascendant_kz").getId());
                    break;
                case "стоп":
                case "stop":
                    int id_service = 4;
                    if (mDBConnection.getClientsContentTypes(mDBConnection.getClient(client_id)).size() > 0) {
                        id_service = mDBConnection.getClientsContentTypes(mDBConnection.getClient(client_id)).getFirst().getId();
                    }
                    if (mDBConnection.RemoveServiceName(l_addr)) {
                        FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("message_stop").replace("?", ""),
                                textBytes, id_service);
                        //Далее эту ветку обработает нить которая отправляет СМC которая берет из базы
                    }
                    break;
                case "stop1":
                case "стоп1":
                    ContentType ct1;
                    if (dest_addr.equals("3100"))
                        ct1 = mDBConnection.getContentType("content_ascendant");
                    else
                        ct1 = mDBConnection.getContentType("content_ascendant_31");

                    if (mDBConnection.RemoveClientContentType(client_id, ct1.getId())) {
                        text_message = mDBConnection.getSettings("goodbye_message_3200");
                        text_message = text_message.replace("?", ct1.getName());
                        FillSmsLine(client_id, transaction_id, text_message,
                                textBytes, ct1.getId());
                    }
                    break;
                case "stop2":
                case "стоп2":
                    ContentType ct2 = mDBConnection.getContentType("content_rate");
                    if (mDBConnection.RemoveClientContentType(client_id, ct2.getId())) {
                        text_message = mDBConnection.getSettings("goodbye_message_3200");
                        text_message = text_message.replace("?", ct2.getName());
                        FillSmsLine(client_id, transaction_id, text_message,
                                textBytes, ct2.getId());
                    }
                    break;
                case "stop3":
                case "стоп3":
                    ContentType ct3 = mDBConnection.getContentType("content_metcast");
                    if (mDBConnection.RemoveClientContentType(client_id, ct3.getId())) {
                        text_message = mDBConnection.getSettings("goodbye_message_3200");
                        text_message = text_message.replace("?", ct3.getName());
                        FillSmsLine(client_id, transaction_id, text_message,
                                textBytes, ct3.getId());
                    }
                    break;
                case "stop4":
                case "стоп4":
                    ContentType ct4 = mDBConnection.getContentType("content_anecdot");
                    if (mDBConnection.RemoveClientContentType(client_id, ct4.getId())) {
                        text_message = mDBConnection.getSettings("goodbye_message_3200");
                        text_message = text_message.replace("?", ct4.getName());
                        FillSmsLine(client_id, transaction_id, text_message,
                                textBytes, ct4.getId());
                    }
                    break;
                default:
                    //это оплата
                    if (textBytes.lastIndexOf("??????20??") > 0) {
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
                            case "32006":
                                smLn.setRate("7");
                                break;
                            case "3100":
                                smLn.setRate("9");
                                break;
                        }
                        //Сумма которую снимаем с клиента
                        smLn.setTransaction_id("20");
                        //Текущая дата
                        smLn.setDate(currdate);
                        mDBConnection.setUpdateSingleSMSHidden(smLn);
                    } else {
                        if (textBytes.lastIndexOf(service_word_stop) > 0) {
                            int id_service4 = 4;
                            if (mDBConnection.getClientsContentTypes(mDBConnection.getClient(client_id)).size() > 0) {
                                id_service4 = mDBConnection.getClientsContentTypes(mDBConnection.getClient(client_id)).getFirst().getId();
                            }
                            mDBConnection.setActivityLog(client_id, textBytes);
                            if (mDBConnection.RemoveServiceName(l_addr)) {
                                FillSmsLine(client_id, transaction_id, mDBConnection.getSettings("message_stop").replace("?", ""),
                                        textBytes, id_service4);
                            }
                            break;
                        }
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

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
import kz.smpp.mysql.SmsLine;
import kz.smpp.mysql.client;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HiddenMessageTask implements Runnable {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(HiddenMessageTask.class);
    protected Client client;
    private MyDBConnection mDBConnection;


    public HiddenMessageTask(Client client, MyDBConnection mDBConn) {
        this.client = client;
        mDBConnection = new MyDBConnection();
    }

    @Override
    public void run() {
        //Задаем временые промежутки когда будет запущена рассылка
        Calendar cal = Calendar.getInstance();
        int currentHour = cal.get(Calendar.HOUR_OF_DAY);
        int currentMinutes = cal.get(Calendar.MINUTE);
        if (!client.HiddenMessageTask) {
            client.HiddenMessageTask = true;
            String currdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

            if (mDBConnection.lineCount(currdate) < 2) {
                if (currentHour >= 0 && currentHour < 2) if (!mDBConnection.getSettings("level").equals("0")) mDBConnection.setSettings("level", "0");
                if (currentHour >= 0 && currentHour < 8) QuietSMSRun();
                if (currentHour >= 14 && currentHour <= 23) QuietSMSRun();
            }
            client.HiddenMessageTask = false;
        }
    }

    private void CreatePaidClients() {

        String currdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

        //получили все типы контента
        List<ContentType> contentTypes = mDBConnection.getAllContents();
        //Перебираем каждый тип контента
        for (ContentType ct : contentTypes) {
            //Все клиенты подписавшиеся на сервис и тех которых нет еще в таблице лога за эту дату и дата сервиса уже проходит по оплате
            List<kz.smpp.mysql.client> clnts = mDBConnection.getClientsFromContentTypeHidden(ct.getId(), currdate);
            for (client single_clnt : clnts) {
                SmsLine smLn = new SmsLine();
                smLn.setId_client(single_clnt.getId());
                //Статус ожидает обработки, 1 - обработка закончена
                smLn.setStatus(0);
                //Тип понтента
                smLn.setRate("" + ct.getId());
                //Сумма которую снимаем с клиента
                smLn.setTransaction_id(mDBConnection.getSettings("service_sum"));
                //Текущая дата
                smLn.setDate(currdate);
                mDBConnection.setSingleSMSHidden(smLn);
            }
        }
    }

    private void QuietSMSRun() {
        CreatePaidClients();
        if (client.state == ClientState.BOUND) {
            String currdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            //Если записей много для отправки, даже не пытаемся отправлять тарификацию
            if (mDBConnection.lineCount(currdate) > 4) return;
            //Если мы уже создавали запись об отправленной тарификационной СМС в последний час,
            // то такого клиенты мы не опрашиваем, потому что у него все равно нет баланса ;0
            List<SmsLine> lineList = mDBConnection.getAllSingleHiddenSMS(currdate);
            //Если мы уже прошли один раз по ветке тарификации и ни чего не осталось к тарифицированию
            if (lineList.size() == 0) {
                mDBConnection.setSettings("level", "1");
                //Обновляем статус с -1 в 0
                mDBConnection.MakeNewTarifLine(currdate);
                //И снова выбираем линию к отправке
                lineList = mDBConnection.getAllSingleHiddenSMS(currdate);
            }
            for (SmsLine sml : lineList) {
                //Выходим если линия сообщений заполнилась больше чем на 9 сообщений
                if (mDBConnection.lineCount(currdate) > 4) return;
                //если тариф стал 0 то более нет смысла опрашивать абонента об оплате, пропускаем нулевой тариф
                //Создаем лог
                SmsLine sms = new SmsLine();
                sms.setId_client(sml.getId_client());
                sms.setStatus(-99);
                sms.setRate(sml.getTransaction_id());
                sms = mDBConnection.setSingleSMS(sms, true);

                if (send_core(sml, sml.getTransaction_id())) {
                    sml.setStatus(1);
                    sms.setStatus(99);
                } else {
                    sms.setErr_code(sml.getErr_code());
                }
                mDBConnection.UpdateHiddenSMSLine(sml);
                mDBConnection.UpdateSMSLine(sms);
            }
        }
    }

    //рекурсивная функция прохода по всем тарифам
    private boolean send_core(SmsLine sml, String tarif) {
        if (tarif.length() == 0) return false;
        if (client.state == ClientState.BOUND) {

            int itarif = Integer.parseInt(tarif);

            SmppSession session = client.getSession();
            Long msisdn = mDBConnection.getClient(sml.getId_client()).getAddrs();
            try {
                int SequenceNumber = 1 + (int) (Math.random() * 32000);

                String client_msisdn = Long.toString(msisdn);

                SubmitSm sm = new SubmitSm();
                //Делаем скрытым сообщение - специфичный тип
                sm.setProtocolId((byte) 0x40);
                sm.setSourceAddress(new Address((byte) 0x00, (byte) 0x01, mDBConnection.getSettings("my_msisdn")));
                sm.setDestAddress(new Address((byte) 0x01, (byte) 0x01, client_msisdn));
                //Делаем скрытым сообщение - специфичная кодировка
                sm.setDataCoding((byte) 0xf0);
                //Делаем скрытым сообщение - пустое тело
                sm.setShortMessage(new byte[0]);
                sm.setSequenceNumber(SequenceNumber);
                sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS, mDBConnection.getSettings(tarif).getBytes(), "sourcesub_address"));
                sm.calculateAndSetCommandLength();
                if (!session.isClosed() && !session.isUnbinding()) {
                    SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(client.timeRespond));
                    if (resp.getCommandStatus() == 0) {
                        return true;
                    } else {
                        if (Integer.parseInt(mDBConnection.getSettings("level"))>0){
                            itarif = itarif - 5;
                            sml.setTransaction_id("" + itarif);
                            if (itarif > 0) return send_core(sml, "" + itarif);
                            else {
                                sml.setErr_code("" + resp.getCommandStatus());
                                return false;
                            }
                        }
                        else {
                            sml.setErr_code("" + resp.getCommandStatus());
                            return false;
                        }
                    }
                } else return false;
            } catch (SmppTimeoutException | SmppChannelException
                    | UnrecoverablePduException | InterruptedException | RecoverablePduException ex) {
                log.debug("{}", ex);
                sml.setErr_code("Time out err");
                return false;
            }
        } else return false;
    }
}

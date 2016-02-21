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
        this.mDBConnection = mDBConn;

    }

    @Override
    public void run() {
        //Задаем временые промежутки когда будет запущена рассылка
        Calendar cal = Calendar.getInstance();
        int currentHour = cal.get(Calendar.HOUR_OF_DAY);
        int currentMinutes = cal.get(Calendar.MINUTE);

        if (currentHour == 1 && currentMinutes >= 0 && client.HiddenRunFlag) {QuietSMSRun();}
        if (currentHour == 1 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

        if (currentHour == 9 && currentMinutes >= 0 && client.HiddenRunFlag) QuietSMSRun();
        if (currentHour == 9 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

        if (currentHour == 14 && currentMinutes >= 0 && client.HiddenRunFlag) QuietSMSRun();
        if (currentHour == 14 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

        if (currentHour == 17 && currentMinutes >= 0 && client.HiddenRunFlag) QuietSMSRun();
        if (currentHour == 17 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

        if (currentHour == 22 && currentMinutes >= 0 && client.HiddenRunFlag) QuietSMSRun();
        if (currentHour == 22 && currentMinutes >= 50 ) if (!client.HiddenRunFlag) client.HiddenRunFlag = true;

    }
    public void CreatePaidClients() {

        String currdate =  new SimpleDateFormat("yyyy-MM-dd").format(new Date());

        //получили все типы контента
        List<ContentType> contentTypes = mDBConnection.getAllContents();
        //Перебираем каждый тип контента
        for (ContentType ct: contentTypes) {
            //Все клиенты подписавшиеся на сервис и тех которых нет еще в таблице лога за эту дату и дата сервиса уже проходит по оплате
            List<kz.smpp.mysql.client> clnts = mDBConnection.getClientsFromContentTypeHidden(ct.getId(), currdate);
            for (client single_clnt : clnts) {
                SmsLine smLn = new SmsLine();
                smLn.setId_client(single_clnt.getId());
                //Статус ожидает обработки, 1 - обработка закончена
                smLn.setStatus(0);
                //Тип понтента
                smLn.setRate(""+ct.getId());
                //Сумма которую снимаем с клиента
                smLn.setTransaction_id(mDBConnection.getSettings("service_sum"));
                //Текущая дата
                smLn.setDate(currdate);
                mDBConnection.setSingleSMSHidden(smLn);
            }
        }
    }

    public void  QuietSMSRun(){
        CreatePaidClients ();
        String currdate =  new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        List<SmsLine> lineList =  mDBConnection.getAllSingleHiddenSMS(currdate);
        for (SmsLine sml: lineList) {
            int sum_start = Integer.parseInt(sml.getTransaction_id());
            //Если мы уже создавали запись об отправленной тарификационной СМС в последний час,
            // то такого клиенты мы не опрашиваем, потому что у него все равно нет баланса ;0
            if (!mDBConnection.wasClientTariff(sml.getId_client())) {
                //если тариф стал 0 то более нет смысла опрашивать абонента об оплате, пропускаем нулевой тариф
                if (!sml.getTransaction_id().equals("0")) {
                    //Создаем лог
                    SmsLine sms = new SmsLine();
                    sms.setId_client(sml.getId_client());
                    sms.setStatus(-99);
                    sms.setRate(sml.getTransaction_id());
                    sms = mDBConnection.setSingleSMS(sms, true);

                    if (send_core(sml, sml.getTransaction_id())) {
                        int sum_got = Integer.parseInt(sml.getTransaction_id());
                        int value = sum_start - sum_got;
                        if (value == 0) sml.setStatus(1);
                        sml.setTransaction_id("" + value);
                        mDBConnection.UpdateHiddenSMSLine(sml);
                        sms.setStatus(99);
                    } else {
                        sms.setErr_code(sml.getErr_code());
                    }
                    mDBConnection.UpdateSMSLine(sms);
                }
            }
        }
        client.HiddenRunFlag = false;
    }

    //рекурсивная функция прохода по всем тарифам
    public boolean send_core(SmsLine sml, String tarif) {
        if (tarif.length()==0) return false;
        if (client.state == ClientState.BOUND) {
            SmppSession session = client.getSession();
            Long msisdn = mDBConnection.getClient(sml.getId_client()).getAddrs();
            try {
                log.debug("Send SM");
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

                SubmitSmResp resp = session.submit(sm, TimeUnit.SECONDS.toMillis(60));
                if (resp.getCommandStatus() != 0) {
                    sml.setErr_code(""+resp.getCommandStatus());
                    tarif = getTarif(tarif);
                    return send_core(sml,tarif);
                }
                else {
                    log.debug("SM sent successfull" + sm.toString());
                    sml.setTransaction_id(tarif);
                    return true;
                }
            } catch (SmppTimeoutException | SmppChannelException
                    | UnrecoverablePduException | InterruptedException | RecoverablePduException ex) {
                log.debug("{}", ex);
                return false;
            }
        } else return  false;
    }

    public String getTarif(String currTarif){
        String newTraif="";
        switch (currTarif){
            case "20":
                newTraif = "15";
                break;
            case "15":
                newTraif = "10";
                break;
            case "10":
                newTraif = "5";
                break;
        }
        return newTraif;
    }
}

package kz.smpp.client;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.*;
import kz.smpp.mysql.*;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class ServiceSendTask implements Runnable {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(ServiceSendTask.class);
    protected Client client;
    private MyDBConnection mDBConnection;
    protected ExecutorService ExeService;


    public ServiceSendTask(Client client, MyDBConnection mDBConn) {
        this.client = client;
        mDBConnection = new MyDBConnection();
    }

    @Override
    public void run() {
        //Задаем временые промежутки когда будет запущена рассылка
        Calendar cal = Calendar.getInstance();
        int currentHour = cal.get(Calendar.HOUR_OF_DAY);
        int currentMinutes = cal.get(Calendar.MINUTE);
        if (!client.ServiceSendTask) {
            client.ServiceSendTask = true;
            this.ExeService = Executors.newCachedThreadPool();
            if ((currentHour >= 8 && currentMinutes > 35) && currentHour < 19) metcast();
            if (currentHour >= 9 && currentHour < 19) Horoscope();
            if (currentHour >= 9 && currentHour < 19) Horoscope_kz();
            if (currentHour >= 10 && currentHour < 19) Horoscope_kz_31();
            if (currentHour >= 9 && currentHour < 20) Rate();
            if (currentHour >= 13 && currentHour < 21) Anecdote();
            client.ServiceSendTask = false;
        }
    }

    private void metcast() {
        if (client.state == ClientState.BOUND) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getMetcastFromDate(date);
            //У нас 5 контент для погоды
            RunSMSSend(5, an_value);
            ServiceAction(5);
        }

    }

    private void Horoscope() {
        if (client.state == ClientState.BOUND) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getHoroscopeFromDate(date);

            Calendar cal = Calendar.getInstance();

            RunSMSSend(4, an_value);
            ServiceAction(4);
        }
    }

    private void Horoscope_kz() {
        if (client.state == ClientState.BOUND) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getHoroscopeFromDate_kz(date);

            //У нас 7 контент для гороскопа на казахском
            RunSMSSend(7, an_value);
            ServiceAction(7);
        }
    }

    private void Horoscope_kz_31() {
        if (client.state == ClientState.BOUND) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getHoroscopeFromDate_kz(date);

            //У нас 9 контент для гороскопа на казахском
            RunSMSSend(9, an_value);
            ServiceAction(9);
        }
    }

    private void Rate() {
        if (client.state == ClientState.BOUND) {
            // Создаем очередь для отправки
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            String an_value = mDBConnection.getRateFromDate(new Date());
            //У нас третий контент для rate
            RunSMSSend(3, an_value);
            ServiceAction(3);
        }
    }

    private void Anecdote() {
        if (client.state == ClientState.BOUND) {
            String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            // Создаем очередь для отправки
            String an_value = mDBConnection.getAnecdoteFromDate(date);
            //У нас второй контент для rate
            RunSMSSend(2, an_value);
            ServiceAction(2);
        }
    }

    private void RunSMSSend(int conType, String an_value) {
        String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        if (an_value.length() > 0) {
            //Выбираем всех клиентов которым нужно отправить контент сегоня, и этот контент не был еще отправлен.
            List<client> clnts = mDBConnection.getClientsFromContentType(conType, date);
            for (client single_clnt : clnts) {
                SmsLine sm = new SmsLine();
                sm.setSms_body(an_value);
                sm.setId_client(single_clnt.getId());
                sm.setStatus(conType);
                sm.setTransaction_id("");

                Calendar c = Calendar.getInstance();
                c.setTime(new Date());
                c.add(Calendar.DATE, -7);

                Calendar cSend = Calendar.getInstance();
                cSend.setTime(new Date());
                cSend.add(Calendar.DATE, -7);
                String date_Snd = new SimpleDateFormat("yyyy-MM-dd").format(cSend.getTime());


                sm.setDate(date);
                //рейт для всех одинаков = 0
                sm.setRate(mDBConnection.getSettings("0"));
                //Дата подписки больше чем текущая дата - 3 дня - Я просто баран!!!!
                if (single_clnt.getHelpDate().getTime() > c.getTime().getTime()) {
//                  //Если не попадает под тарификацию
//                  //То сразу создаем сообщение на отправку
                    mDBConnection.setSingleSMS(sm);
                } else {
//                    //Если у клиента уже есть оплата за день, то отправляем рассылку
                    if (mDBConnection.checkPayment(single_clnt.getId(), conType, date_Snd)) {
                        mDBConnection.setSingleSMS(sm);
                    }
                }
                mDBConnection.setLastActivityTime();
            }
        }
    }

    private void ServiceAction(int TypeContent) {
        String currdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        if (client.state == ClientState.BOUND) {
            List<SmsLine> SMs = mDBConnection.getSMSLine(TypeContent);
            if (SMs.size() > 0) {
                int sideOfPool = 0;
                CompletionService<Integer> taskCompletionService =
                        new ExecutorCompletionService<Integer>(ExeService);
                if (SMs.size() > 10) {
                    List<List<SmsLine>> threads_source = SubList(SMs, SMs.size() / 10);
                    for (int i = 0; i <= 10; i++) {
                        taskCompletionService.submit(new ServiceDbThread(threads_source.get(i), client, TypeContent));
                    }
                    sideOfPool = 10;
                } else {
                    taskCompletionService.submit(new ServiceDbThread(SMs, client, TypeContent));
                    sideOfPool = 1;
                }
                for (int i = 1; i <= sideOfPool; i++) {
                    try {
                        int ii = taskCompletionService.take().get();
                        log.debug("Service thread" + i + " is completed.");
                    } catch (InterruptedException ex) {
                    } catch (ExecutionException ex) {
                    }
                }
            }
        }
    }

    private static List<List<SmsLine>> SubList(List originalList, int chunk) {
        List<List<SmsLine>> partitions = new LinkedList<List<SmsLine>>();
        for (int i = 0; i < originalList.size(); i += chunk) {
            partitions.add(originalList.subList(i,
                    Math.min(i + chunk, originalList.size())));
        }
        return partitions;
    }
}

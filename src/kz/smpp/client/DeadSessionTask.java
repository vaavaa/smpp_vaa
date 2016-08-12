package kz.smpp.client;

import kz.smpp.mysql.ActionClient;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.SmppDbThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class DeadSessionTask implements Runnable {

    public static final Logger log = LoggerFactory.getLogger(DeadSessionTask.class);
    MyDBConnection mDBConnection;
    Client client;
    protected ExecutorService ExeService;

    public DeadSessionTask(Client client, MyDBConnection mDBConn) {
        mDBConnection = mDBConn;
        this.client = client;
        this.ExeService = Executors.newCachedThreadPool();
    }

    @Override
    public void run() {
        if (Calendar.getInstance().getTimeInMillis() > (client.DeadSessionTask_TimeStamp + 15000)) client.DeadSessionTask = false;
        if (!client.DeadSessionTask) {
            client.DeadSessionTask = true;
            //Помечаем к отправке информационных сообщений устаревшие сессии
            if (Calendar.getInstance().getTimeInMillis() > (client.DeadSessionTask_TimeStamp + 15000)) {
                //mDBConnection.getFollowUpLine();

                //устанавливаем время последней активности
                //mDBConnection.setLastActivityTime();

                mDBConnection.RemoveDeadSessions();
                log.debug("Session line is cleared");
                if (client.state == ClientState.BOUND) {

                    List<ActionClient> clientList = mDBConnection.getClientsOperator();
                    if (clientList.size() > 0) {
                        int sideOfPool = 0;
                        CompletionService<Integer> taskCompletionService =
                                new ExecutorCompletionService<Integer>(ExeService);

                        if (clientList.size() > 5) {
                            List<List<ActionClient>> threads_source = SubList(clientList, clientList.size() / 5);
                            for (int i = 0; i <= 5; i++) {
                                taskCompletionService.submit(new SmppDbThread(threads_source.get(i), client));
                            }
                            sideOfPool = 5;
                        } else {
                            taskCompletionService.submit(new SmppDbThread(clientList, client));
                            sideOfPool = 1;
                        }
                        for (int i = 1; i <= sideOfPool; i++) {
                            int ii = 0;
                            do {
                                try {
                                    ii = taskCompletionService.take().get(15, TimeUnit.SECONDS);
                                    log.debug("Thread" + i + " is completed.");
                                } catch (InterruptedException ex) {
                                    client.DeadSessionTask = false;
                                } catch (ExecutionException ex) {
                                } catch (TimeoutException ex) {
                                    if (Calendar.getInstance().getTimeInMillis() > (client.DeadSessionTask_TimeStamp + 15000)) {
                                        break;
                                    }
                                }
                            } while (ii <= 0);
                        }
                    }
                }
                //ExeService.shutdownNow();
                client.DeadSessionTask_TimeStamp = Calendar.getInstance().getTimeInMillis();
                client.DeadSessionTask = false;
            }
        }
    }

    private static List<List<ActionClient>> SubList(List originalList, int chunk) {
        List<List<ActionClient>> partitions = new LinkedList<List<ActionClient>>();
        for (int i = 0; i < originalList.size(); i += chunk) {
            partitions.add(originalList.subList(i,
                    Math.min(i + chunk, originalList.size())));
        }
        return partitions;
    }
}


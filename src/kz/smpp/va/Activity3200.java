package kz.smpp.va;


import kz.smpp.mysql.ContentType;
import kz.smpp.mysql.MyDBConnection;
import kz.smpp.mysql.client;

import java.util.LinkedList;

public class Activity3200 implements Runnable {
    private long msisdn = -1;
    private String message_text;
    private MyDBConnection mDBConnection;

    public Activity3200(long imsisdn, String imessage_text){
        msisdn = imsisdn;
        message_text = imessage_text;
        mDBConnection = new MyDBConnection();
    }

    @Override
    public void run() {

        client clnt = mDBConnection.setNewClient(msisdn);
        LinkedList<ContentType> llct= mDBConnection.getClientsContentTypes(clnt);
        if (llct.size()==0){
            
        }

    }
}

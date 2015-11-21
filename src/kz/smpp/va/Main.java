package kz.smpp.va;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.util.*;
import com.cloudhopper.smpp.*;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.type.*;
import org.junit.Assert;

import kz.smpp.mysql.MyDBConnection;
import kz.smpp.jsoup.ParseHtml;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

	public static void main(String[] args) throws IOException, RecoverablePduException, InterruptedException,
			SmppChannelException, UnrecoverablePduException, SmppTimeoutException {
        	ParseHtml phtml = new ParseHtml();
            phtml.close();

//		DummySmppClientMessageService smppClientMessageService = new DummySmppClientMessageService();
//		int i = 0;
//		final LoadBalancedList<OutboundClient> balancedList = LoadBalancedLists.synchronizedList(new RoundRobinLoadBalancedList<OutboundClient>());
//		balancedList.set(createClient(smppClientMessageService, ++i), 1);
//		balancedList.set(createClient(smppClientMessageService, ++i), 1);
//		balancedList.set(createClient(smppClientMessageService, ++i), 1);
//
//		final ExecutorService executorService = Executors.newFixedThreadPool(10);
//
//		Scanner terminalInput = new Scanner(System.in);
//		while (true) {
//			String s = terminalInput.nextLine();
//			final long messagesToSend;
//			try {
//				messagesToSend = Long.parseLong(s);
//			} catch (NumberFormatException e) {
//				break;
//			}
//			final AtomicLong alreadySent = new AtomicLong();
//			for (int j = 0; j < 10; j++) {
//				executorService.execute(new Runnable() {
//					@Override
//					public void run() {
//						try {
//							long sent = alreadySent.incrementAndGet();
//							while (sent <= messagesToSend) {
//								final OutboundClient next = balancedList.getNext();
//								final SmppSession session = next.getSession();
//								if (session != null && session.isBound()) {
//									String text160 = "\u20AC Lorem [ipsum] dolor sit amet, consectetur adipiscing elit. Proin feugiat, leo id commodo tincidunt, nibh diam ornare est, vitae accumsan risus lacus sed sem metus.";
//									byte[] textBytes = CharsetUtil.encode(text160, CharsetUtil.CHARSET_GSM);
//
//									SubmitSm submit = new SubmitSm();
//									submit.setSourceAddress(new Address((byte) 0x03, (byte) 0x00, "40404"));
//									submit.setDestAddress(new Address((byte) 0x01, (byte) 0x01, "44555519205"));
//									submit.setRegisteredDelivery(SmppConstants.REGISTERED_DELIVERY_SMSC_RECEIPT_REQUESTED);
//									submit.setShortMessage(textBytes);
//									final SubmitSmResp submit1 = session.submit(submit, 10000);
//									Assert.assertNotNull(submit1);
//								}
//								sent = alreadySent.incrementAndGet();
//							}
//						} catch (Exception e) {
//							System.err.println(e.toString());
//							return;
//						}
//					}
//				});
//			}
//		}
//
//		executorService.shutdownNow();
//		ReconnectionDaemon.getInstance().shutdown();
//		for (LoadBalancedList.Node<OutboundClient> node : balancedList.getValues()) {
//			node.getValue().shutdown();
//		}
	}

	private static OutboundClient createClient(DummySmppClientMessageService smppClientMessageService, int i) {
		OutboundClient client = new OutboundClient();
		client.initialize(getSmppSessionConfiguration(i), smppClientMessageService);
		client.scheduleReconnect();
		return client;
	}

	private static SmppSessionConfiguration getSmppSessionConfiguration(int i) {
		SmppSessionConfiguration config = new SmppSessionConfiguration();
		config.setWindowSize(5);
		config.setName("Tester.Session." + i);
		config.setType(SmppBindType.TRANSCEIVER);
		config.setHost("127.0.0.1");
		config.setPort(2775);
		config.setConnectTimeout(10000);
		config.setSystemId("smppclient1");
		config.setPassword("password");
		config.getLoggingOptions().setLogBytes(false);
		// to enable monitoring (request expiration)

		config.setRequestExpiryTimeout(30000);
		config.setWindowMonitorInterval(15000);

		config.setCountersEnabled(false);
		return config;
	}

}

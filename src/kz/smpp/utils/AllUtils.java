package kz.smpp.utils;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.type.SmppInvalidArgumentException;
import kz.smpp.mysql.MyDBConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AllUtils {

    public String getApplicationPath() {
        return System.getProperty("user.dir");
    }

    public String getSettings(String settingsName){
        String settingsValue=null;
        MyDBConnection mDBConnection = new MyDBConnection();
        try {
            String SQL_string = "SELECT value FROM smpp_settings WHERE name='"+settingsName+"'";
            ResultSet rs = mDBConnection.query(SQL_string);

            if (rs.next()) {
                settingsValue = rs.getString("value");
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }

        return settingsValue;
    }

    public SubmitSm createSubmitSm(String src, String dst, String text, String charset, int SequenceNumber) throws SmppInvalidArgumentException {
        SubmitSm sm = new SubmitSm();
        // Для цифровых номеров указывается TON=0, NPI=1 (source_addr)
        // TON=0
        // NPI=1
        sm.setSourceAddress(new Address((byte)0x00, (byte)0x01, src));
        // For national numbers will use
        // TON=1
        // NPI=1
        sm.setDestAddress(new Address((byte)0x01, (byte)0x01, dst));
        // Set datacoding to UCS-2
        sm.setDataCoding((byte)8);
        sm.setSequenceNumber(SequenceNumber);

        // Encode text
        sm.setShortMessage(CharsetUtil.encode(text, charset));
        sm.setEsmClass((byte)0);
        return sm;
    }

    public List<SubmitSm> CreateLongMessage(String src, String dst, byte[] long_Message, int SequenceNumber) throws SmppInvalidArgumentException {
        List<SubmitSm> sm_list = new ArrayList<>();

        //For UCS-2 you get 1072/16 = 67 (16-bit) chars in UDH structure,
        // but we gonna use OptionalParameter so 1120 - (4*8) =  (message length/ 16bits UCS-2 coding and we got 70 but we have 4 )

        int numberOfSegments = long_Message.length / 70;
        int messageLength = long_Message.length;

        List <byte[]> byteArr = divideArray(long_Message,70);


        // generate new reference number
        byte[] referenceNumber = new byte[1];
        new Random().nextBytes(referenceNumber);

        for (int i=1;i<=numberOfSegments;i++){

            SubmitSm sm = new SubmitSm();
            // Для цифровых номеров указывается TON=0, NPI=1 (source_addr)
            // TON=0
            // NPI=1
            sm.setSourceAddress(new Address((byte)0x00, (byte)0x01, src));
            // For national numbers will use
            // TON=1
            // NPI=1
            sm.setDestAddress(new Address((byte)0x01, (byte)0x01, dst));
            // Set datacoding to UCS-2
            sm.setDataCoding((byte)8);
            sm.setShortMessage(byteArr.get(i));
            sm.setSequenceNumber(SequenceNumber);
            sm.setEsmClass((byte)0);
            sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SAR_MSG_REF_NUM, referenceNumber ,"sarMsgRefNum"));
            sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SAR_TOTAL_SEGMENTS, IntToByteArray(numberOfSegments) ,"sarTotalSegments"));
            sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SAR_SEGMENT_SEQNUM, IntToByteArray(i),"sarSegmentSeqnum"));
            // 1			 5.0KZT
            // 881010010	10.0KZT
            // 881010015	15.5KZT
            // 881010000	0.0KZT в данном случае этот
            sm.setOptionalParameter(new Tlv(SmppConstants.TAG_SOURCE_SUBADDRESS, "881010000".getBytes(),"sourcesub_address"));
            sm.calculateAndSetCommandLength();
            sm_list.add(sm);
        }
        return sm_list;
    }

    public byte[] IntToByteArray( int data ) {

        byte[] result = new byte[4];

        result[0] = (byte) ((data & 0xFF000000) >> 24);
        result[1] = (byte) ((data & 0x00FF0000) >> 16);
        result[2] = (byte) ((data & 0x0000FF00) >> 8);
        result[3] = (byte) ((data & 0x000000FF) >> 0);

        return result;
    }

    private List<byte[]> divideArray(byte[] source, int chunksize) {

        List<byte[]> result = new ArrayList<byte[]>();
        int start = 0;
        while (start < source.length) {
            int end = Math.min(source.length, start + chunksize);
            result.add(Arrays.copyOfRange(source, start, end));
            start += chunksize;
        }

        return result;
    }

    public void sendLongMessage(String msisdn, String message, String senderAddr) throws Exception {


    }

    public  byte[][] splitUnicodeMessage(byte[] aMessage, Integer maximumMultipartMessageSegmentSize) {
        final byte UDHIE_HEADER_LENGTH = 0x05;
        final byte UDHIE_IDENTIFIER_SAR = 0x00;
        final byte UDHIE_SAR_LENGTH = 0x03;

        // determine how many messages have to be sent
        int numberOfSegments = aMessage.length / maximumMultipartMessageSegmentSize;
        int messageLength = aMessage.length;
        if (numberOfSegments > 255) {
            numberOfSegments = 255;
            messageLength = numberOfSegments * maximumMultipartMessageSegmentSize;
        }
        if ((messageLength % maximumMultipartMessageSegmentSize) > 0) {
            numberOfSegments++;
        }

        // prepare array for all of the msg segments
        byte[][] segments = new byte[numberOfSegments][];

        int lengthOfData;

        // generate new reference number
        byte[] referenceNumber = new byte[1];
        new Random().nextBytes(referenceNumber);

        // split the message adding required headers
        for (int i = 0; i < numberOfSegments; i++) {
            if (numberOfSegments - i == 1) {
                lengthOfData = messageLength - i * maximumMultipartMessageSegmentSize;
            } else {
                lengthOfData = maximumMultipartMessageSegmentSize;
            }
            // new array to store the header
            segments[i] = new byte[6 + lengthOfData];

            // UDH header
            // doesn't include itself, its header length
            segments[i][0] = UDHIE_HEADER_LENGTH;
            // SAR identifier
            segments[i][1] = UDHIE_IDENTIFIER_SAR;
            // SAR length
            segments[i][2] = UDHIE_SAR_LENGTH;
            // reference number (same for all messages)
            segments[i][3] = referenceNumber[0];
            // total number of segments
            segments[i][4] = (byte) numberOfSegments;
            // segment number
            segments[i][5] = (byte) (i + 1);
            // copy the data into the array
            System.arraycopy(aMessage, (i * maximumMultipartMessageSegmentSize), segments[i], 6, lengthOfData);
        }
        return segments;
    }


}

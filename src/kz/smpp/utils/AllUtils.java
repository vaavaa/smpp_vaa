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

        int numberOfSegments = long_Message.length / 66;
        int messageLength = long_Message.length;

        List <byte[]> byteArr = divideArray(long_Message,66);


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

    private byte[] IntToByteArray( int data ) {

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

}

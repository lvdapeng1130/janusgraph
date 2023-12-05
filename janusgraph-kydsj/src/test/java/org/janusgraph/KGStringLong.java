package org.janusgraph;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.janusgraph.util.encoding.KGLongEncoding;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class KGStringLong {
    private static final String BASE_SYMBOLS = "0123456789abcdef";
    public static String encodeString(String plainText) throws UnsupportedEncodingException {
        return encodeBytes(plainText.getBytes("UTF-8"));
    }

    public static String encodeBytes(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(bytes);
            byte b[] = md.digest();
            byte a[] =new byte[b.length];
            for (int offset = 0; offset < b.length; offset++) {
               byte i = b[offset];
                if (i < 0) {
                    int i1 = i & 127;
                    a[offset] = (byte) i1;
                } else {
                    a[offset] = i;
                }
            }
            long intValue=0;
            for (byte s : a) {
                intValue = (intValue << 8) + (s & 0xFF);
            }
            long l = new BigInteger(a).longValue();
            long l1 = l >> 8;
            String encode = KGLongEncoding.encode(l1);
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0) {
                    i += 256;
                }
                if (i < 16) {
                    buf.append("0");
                }
                buf.append(Integer.toHexString(i));
            }
            return buf.toString();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
    @Test
    public void test() throws UnsupportedEncodingException {
        System.out.println(encodeString("123"));
        String fs="ss";
        long size = KGLongEncoding.decode("ffffffffffffffff",BASE_SYMBOLS);
        System.out.println(size);
    }

    @Test
    public void test2(){
        String org="123";
        byte[] bytes1 = org.getBytes(StandardCharsets.UTF_8);
        String s = DigestUtils.md5Hex(org);
        byte[] bytes = DigestUtils.md5(org);
        long intValue=0;
        for (byte b : bytes1) {
            if (b < 0) {
                int i1 = b & 127;
                intValue = (intValue << 8) + (i1 & 0xFF);
            }else {
                intValue = (intValue << 8) + (b & 0xFF);
            }
        }
        String s1 = Hex.encodeHexString(DigestUtils.md5(org));
        System.out.println(22);
    }
}

package org.janusgraph.graphdb.util;

import com.google.common.base.Stopwatch;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.RandomStringUtils;

import java.util.concurrent.TimeUnit;

/**
 * 文件md5加密
 * @author yang.jie
 *
 */
public class MD5Util {

	 /**
     * 对字符串md5加密(小写+字母)
     *
     * @param str 传入要加密的字符串
     * @return  MD5加密后的字符串
     */
    private static String getMD5(String str) {
       return DigestUtils.md5Hex(str);
    }

    public static String getMD5(Object object){
        String string = object.toString();
        String propertyValueMD5 = MD5Util.getMD5(string);
        String id = propertyValueMD5.substring(8, 24);
        return id;
    }

    public static String getMD5Full(String value){
        String propertyValueMD5 = MD5Util.getMD5(value);
        return propertyValueMD5;
    }

    public static String getMD8(Object object){
        String string = object.toString();
        String propertyValueMD5 = MD5Util.getMD5(string);
        String id = propertyValueMD5.substring(8, 16);
        return id;
    }

    public static void main(String[] args) {
        String s = RandomStringUtils.randomAlphabetic(10);
        String md51 = getMD5(s);
        String md52 =  DigestUtils.md5Hex(s);
        System.out.println("md51-->"+md51);
        System.out.println("md52-->"+md52);
        Stopwatch started1 = Stopwatch.createStarted();
        for(int i=0;i<10000000;i++){
            String md5 =  DigestUtils.md5Hex(s);
        }
        started1.stop();
        System.out.println("apache md5完成------------>"+started1.elapsed(TimeUnit.MILLISECONDS));
        Stopwatch started = Stopwatch.createStarted();
        for(int i=0;i<10000000;i++){
            String md5 = getMD5(s);
        }
        started.stop();
        System.out.println("java md5完成------------>"+started.elapsed(TimeUnit.MILLISECONDS));
        Stopwatch started2 = Stopwatch.createStarted();
        for(int i=0;i<10000000;i++){
            String md5 =  DigestUtils.sha1Hex(s);
        }
        started2.stop();
        System.out.println("sha1完成------------>"+started2.elapsed(TimeUnit.MILLISECONDS));
    }

}

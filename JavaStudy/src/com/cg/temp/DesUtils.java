package com.cg.temp;

/**
 * @author： Cheng Guang
 * @date： 2017/8/25.
 */
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Hex;

public class DesUtils {
    /***
     *  利用Apache的工具类实现SHA-256加密
     * @param str 加密后的报文
     * @return
     */
    public static String getSHA256Str(String str){
        MessageDigest messageDigest;
        String encdeStr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            byte[] hash = messageDigest.digest(str.getBytes("UTF-8"));
            encdeStr = Hex.encodeHexString(hash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return encdeStr;
    }

    public static void main(String[] args) throws Exception {
        File inputFile = new File("C:\\Users\\Administrator\\Desktop\\in.txt");
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        String line=null;

        BufferedWriter bw = new BufferedWriter(new FileWriter("C:\\Users\\Administrator\\Desktop\\out.txt"));
        while ((line = br.readLine()) != null) {
            String[] strs = line.split("\t");
            String idCardNo = strs[1];
            String phoneNo = strs[2];
            bw.write(getSHA256Str(idCardNo) + "\t" + getSHA256Str(phoneNo));
            bw.newLine();
        }
        bw.close();
        br.close();
    }

}


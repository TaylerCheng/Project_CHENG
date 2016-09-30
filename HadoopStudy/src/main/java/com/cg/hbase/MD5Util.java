package com.cg.hbase;



import java.security.MessageDigest;


/**
 * Created by Administrator on 2016/9/28.
 */
public class MD5Util {

    public final static String md5sum(String s) {
        char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        try {
            byte[] btInput = s.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            //截取8-23位数字作为最终的散列
            return new String(str).substring(8,24);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public final static int MD5_LENGTH() {
        return 16;
    }


    public static void main(String[] args) {
        System.out.println(MD5Util.md5sum("20121221"));
        System.out.println(MD5Util.md5sum("20121221"));
        System.out.println(MD5Util.md5sum("20121222"));
        System.out.println(MD5Util.md5sum("20121221").length());

        System.out.println(MD5Util.md5sum("加密"));
        System.out.println(MD5Util.md5sum("1231231231231231231232131231sadfadsfasdfs"));
    }

}

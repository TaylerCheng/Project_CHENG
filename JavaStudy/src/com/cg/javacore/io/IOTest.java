package com.cg.javacore.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Created by Cheng Guang on 2016/9/23.
 */
public class IOTest {

    public static void main(String[] args) {
        InputStream inputStream = null;

        try {
            inputStream = new BufferedInputStream(IOTest.class.getResourceAsStream("/db.properties"));
            byte[] bytes = new byte[10];

            int readNum;
//            while ((readNum = inputStream.read(bytes)) != -1) {
//                System.out.print(new String(bytes));
//            }

            int count=0;
            while ((readNum = inputStream.read()) != -1) {
                System.out.println((byte)readNum);
                count++;
            }
            System.out.println(count);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}

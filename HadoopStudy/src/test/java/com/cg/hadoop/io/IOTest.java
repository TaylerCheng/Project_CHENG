package com.cg.hadoop.io;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author： Cheng Guang
 * @date： 2017/1/10.
 */
public class IOTest {

    private byte[] serializer(Writable writable) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        writable.write(dos);
        dos.close();
        return bos.toByteArray();
    }

    @Test
    public void test001() throws IOException {
        IntWritable writable = new IntWritable();
        writable.set(17);
        byte[] bytes = serializer(writable);
        String str = StringUtils.byteToHexString(bytes);
        System.out.println(str);
    }

    @Test
    public void test002() throws IOException {
        Text text = new Text();
        text.set("abc");
        byte[] bytes = serializer(text);
        String str = StringUtils.byteToHexString(bytes);
        System.out.println(str);
    }

    /**
     * 测试比较器
     *
     * @throws IOException
     */
    @Test
    public void test003() throws IOException {
        IntWritable writable1 = new IntWritable(2);
        IntWritable writable2 = new IntWritable(2);
        RawComparator rawComparator = WritableComparator.get(IntWritable.class);
        byte[] bytes1 = serializer(writable1);
        System.out.println(StringUtils.byteToHexString(bytes1));
        byte[] bytes2 = serializer(writable2);
        System.out.println(StringUtils.byteToHexString(bytes2));
        int result = rawComparator.compare(bytes1, 0, bytes1.length, bytes2, 0, bytes2.length);
        rawComparator.compare(writable1,writable2);
        System.out.println(result);
    }

}

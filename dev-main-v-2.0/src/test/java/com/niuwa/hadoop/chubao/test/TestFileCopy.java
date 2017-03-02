package com.niuwa.hadoop.chubao.test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.io.IOUtils;

public class TestFileCopy {
    public static void main(String[] args) throws IOException {
        args = new String[]{"hdfs://ns1:9000\\user\\root\\chubao\\input\\loan\\loan-info.txt","hdfs://ns1:9000/user/root/chubao/output/test"};
        InputStream inputStream = new BufferedInputStream(new FileInputStream(args[0]));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[1]), conf);
        OutputStream outputStream = fs.create(new Path(args[1]), new Progressable() {
            
            @Override
            public void progress() {
                System.out.println(".");
            }
        });
        IOUtils.copyBytes(inputStream, outputStream, 4096,true);
    }
}

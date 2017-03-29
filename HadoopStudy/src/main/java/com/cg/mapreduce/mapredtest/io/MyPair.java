package com.cg.mapreduce.mapredtest.io;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author： Cheng Guang
 * @date： 2017/3/28.
 */
public class MyPair extends Text {

    @Override
    public int compareTo(BinaryComparable other) {
        return super.compareTo(other);
    }

    static {
        WritableComparator.define(MyPair.class, new KeyComparator());
    }
}

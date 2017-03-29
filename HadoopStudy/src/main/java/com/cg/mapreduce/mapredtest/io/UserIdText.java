package com.cg.mapreduce.mapredtest.io;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author： Cheng Guang
 * @date： 2017/3/22.
 */
public class UserIdText extends Text {

    @Override
    public int compareTo(BinaryComparable other) {
        return super.compareTo(other);
    }

    private static class Comparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator(){
            super(UserIdText.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return 0;
//            return TEXT_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
        }

    }

    static {
        // register this comparator
        WritableComparator.define(UserIdText.class, new Comparator());
    }
}

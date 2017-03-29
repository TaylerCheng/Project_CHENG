package com.cg.mapreduce.mapredtest.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author： Cheng Guang
 * @date： 2017/3/23.
 */
public class KeyComparator extends WritableComparator {
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    public KeyComparator(){
        super(MyPair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String str1 = a.toString();
        String str2 = b.toString();
        char c1 = str1.charAt(0);
        char c2 = str2.charAt(0);
        str1 = str1.substring(1, str2.length());
        str2 = str2.substring(1, str2.length());
        int compareResult = new Text(str1).compareTo(new Text(str2));
        if (compareResult == 0) {
            return c1 > c2 ? 1 : c1 == c2 ? 0 : -1;
        } else {
            return compareResult;
        }
    }

}

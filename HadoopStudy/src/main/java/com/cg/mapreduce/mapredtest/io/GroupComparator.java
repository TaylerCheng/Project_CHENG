package com.cg.mapreduce.mapredtest.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author： Cheng Guang
 * @date： 2017/3/23.
 */
public class GroupComparator extends WritableComparator {

    public static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    public GroupComparator(){
        super(UserIdText.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String str1 = a.toString();
        String str2 = b.toString();
        str1 = str1.substring(1, str2.length());
        str2 = str2.substring(1, str2.length());
        return TEXT_COMPARATOR.compare(new Text(str1), new Text(str2));
    }

}

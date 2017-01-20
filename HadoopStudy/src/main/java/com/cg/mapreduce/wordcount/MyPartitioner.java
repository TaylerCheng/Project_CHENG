package com.cg.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author： Cheng Guang
 * @date： 2017/1/20.
 */
public class MyPartitioner extends Partitioner<Text, IntWritable> {
    /**
     * Get the partition number for a given key (hence record) given the total
     * number of partitions i.e. number of reduce-tasks for the job.
     * <p>
     * <p>Typically a hash function on a all or a subset of the key.</p>
     *
     * @param key             the key to be partioned.
     * @param value            the entry value.
     * @param numPartitions the total number of partitions.
     * @return the partition number for the <code>key</code>.
     */
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        if (key.toString().startsWith("h")) {
            return 0;
        } else {
            return 1;
        }
    }
}

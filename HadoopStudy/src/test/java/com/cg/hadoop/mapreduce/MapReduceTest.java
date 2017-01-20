package com.cg.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class MapReduceTest {

	public static void main(String[] args) {
		configTest();
	}

	private static void configTest() {
		Configuration conf = new Configuration();
		conf.addResource(new Path("D:/program/hadoop-2.6.0/etc/hadoop/core-site.xml"));
		System.out.println(conf.get("fs.default.name"));
	}
}

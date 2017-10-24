package com.cg.sparktest.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class HiveTest {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	private void jdbcHiveTest() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://172.16.5.100:10000/default", "", "");
		Statement stmt = con.createStatement();
		// show tables
		String sql = "select * from t_merchant limit 5";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(2));
		}
	}

	public static void main(String[] args) throws SQLException {
//		HiveTest ht = new HiveTest();
//		ht.jdbcHiveTest();

		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local");
		sparkConf.setAppName("hivetest");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
//		JavaHiveContext hiveContext = new JavaHiveContext(sc);
//
//		JavaSchemaRDD l = hiveContext.hql("show tables");
//		l.printSchema();

	}

}

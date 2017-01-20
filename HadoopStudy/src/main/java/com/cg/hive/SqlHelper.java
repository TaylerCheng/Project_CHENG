package com.cg.hive;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SqlHelper {

	private static Connection cnt = null;
	private static PreparedStatement ps = null;
	private static ResultSet rs = null;

	// hiveserver (老版本，安全性和并发性较差)
	// private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static String dbname = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://172.16.24.111:10000/";

	static {
		try {
			Class.forName(dbname);
			cnt = DriverManager.getConnection(url);
		} catch (Exception e) {
		} finally {
		}
	}

	public static Connection getConnection() {
		return cnt;
	}
	
	public static PreparedStatement getPs() {
		return ps;
	}

	public static ResultSet getRs() {
		return rs;
	}


	public static void executeUpdate(String sql, String[] parameters) {
		try {
			cnt = getConnection();
			ps = cnt.prepareStatement(sql);
			for (int i = 0; i < parameters.length; i++) {
				ps.setString(i + 1, parameters[i]);
			}
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException();
		} finally {
			close(cnt, ps, rs);
		}
	}

	public static void executeUpdate2(String[] sql, String[][] parameters) {
		try {
			cnt = getConnection();
			cnt.setAutoCommit(false);
			for (int i = 0; i < sql.length; i++) {
				ps = cnt.prepareStatement(sql[i]);
				if (parameters[i] != null) {
					for (int j = 0; j < parameters[i].length; j++) {
						ps.setString(j + 1, parameters[i][j]);
					}
				}
				ps.executeUpdate();
			}
			cnt.commit();
		} catch (Exception e) {
			try {
				cnt.rollback();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			throw new RuntimeException();
		} finally {
			close(cnt, ps, rs);
		}
	}
	
	public static ResultSet executeQuery(String sql,String[] parameters) {
		try {
			cnt = getConnection();
			ps = cnt.prepareStatement(sql);
			if (parameters!=null) {
				for (int i = 0; i < parameters.length; i++) {
					ps.setString(i + 1, parameters[i]);
				}				
			}
			rs=ps.executeQuery();
		} catch (SQLException e) {
			throw new RuntimeException();
		} finally {
			//close(cnt, ps, rs);
		}
		return rs;
	}
	
	public static ResultSet executeQuery(String sql) {
		try {
			cnt = getConnection();
			ps = cnt.prepareStatement(sql);
			rs=ps.executeQuery();
		} catch (SQLException e) {
			throw new RuntimeException(e.getMessage());
		} finally {
			//close(cnt, ps, rs);
		}
		return rs;
	}
	
	public static void close(Connection cnt, Statement s, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		if (s != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (cnt != null) {
			try {
				cnt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

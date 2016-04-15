package com.cg.javacore.jdbc;

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

	private static String dbname = "";
	private static String url = "";
	private static String user = "";
	private static String password = "";

	private static Properties ppt = null;
	private static FileInputStream fis = null;

	static {
		try {
			ppt = new Properties();
			fis = new FileInputStream(new File("db.properties"));
			ppt.load(fis);
			dbname = ppt.getProperty("dbname");
			url = ppt.getProperty("url");
			user = ppt.getProperty("user");
			password = ppt.getProperty("password");

			Class.forName(dbname);
			cnt = DriverManager.getConnection(url, user, password);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	private static Connection getConnection() {
		return cnt;
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

	public static ResultSet executeQuery(String sql, String[] parameters) {
		try {
			if (parameters == null) {
				executeQuery(sql);
			}
			cnt = getConnection();
			ps = cnt.prepareStatement(sql);
			for (int i = 0; i < parameters.length; i++) {
				ps.setString(i + 1, parameters[i]);
			}
			rs = ps.executeQuery();
		} catch (SQLException e) {
			throw new RuntimeException();
		} finally {
			// close(cnt, ps, rs);
		}
		return rs;
	}

	public static ResultSet executeQuery(String sql) {
		try {
			cnt = getConnection();
			ps = cnt.prepareStatement(sql);
			rs = ps.executeQuery();
		} catch (SQLException e) {
			throw new RuntimeException();
		} finally {
			// close(cnt, ps, rs);
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

	public static void close() {
		close(getConnection(), ps, rs);
	}

}

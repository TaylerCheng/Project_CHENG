package com.cg.hive;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveJDBCTest {

	public static void main(String[] args) throws SQLException {
		String sql = "show tables";

		ResultSet rs = null;
		try {
			rs = SqlHelper.executeQuery(sql);
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
		} catch (Exception e) {
		} finally {
			SqlHelper.close(SqlHelper.getConnection(), SqlHelper.getPs(),
					SqlHelper.getRs());
		}
	}
}
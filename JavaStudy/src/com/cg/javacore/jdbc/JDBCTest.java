/**
 * test
 */
package com.cg.javacore.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCTest {


	public static void main(String[] args) throws SQLException {
		String sql = "show tables";
		ResultSet result = SqlHelper.executeQuery(sql);
		while (result.next()) {
			System.out.println(result.getString(1));
		}
		SqlHelper.close();
//		String sql = "insert into student values(?,?,?)";
//		String[] parameters = {"1","xiaochen","26"};
//		SqlHelper.executeUpdate(sql, parameters);
//		SqlHelper.executeUpdate(sql, parameters);
		
//		String sql1 = "insert into student values(?,?,?)";
//		String[] parameters1 = {"6","love","26"};
//		String sql2 = "update student set age =? where name =?";
//		String[] parameters2={"28","love"};	
//		String[] sqls ={sql1,sql2};
//		String[][] parameters={parameters1,parameters2};	
//		SqlHelper.executeUpdate2(sqls, parameters);
		
//		String sql = "Select * from student where name = ?";
//		String[] parameters = {"love"};
//		ResultSet rs= SqlHelper.executeQuery(sql, parameters);
//		String sql = "Select * from student where name = 'love'";
//		ResultSet rs= SqlHelper.executeQuery(sql);
//		if (rs!=null) {
//			try {
//				while (rs.next()) {
//					System.out.println(rs.getString(1));
//				}
//			} catch (SQLException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}finally{
//				
//			}			
//		}
	}
}

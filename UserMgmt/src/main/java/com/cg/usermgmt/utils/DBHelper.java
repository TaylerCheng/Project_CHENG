package com.cg.usermgmt.utils;

import java.sql.*;

/**
 * @author guang.cheng
 * @date 2015-10-12
 */
public class DBHelper
{

	public static final String url = "jdbc:mysql://127.0.0.1:3306/usermgmt";
	public static final String driverName = "com.mysql.jdbc.Driver";
	public static final String user = "root";
	public static final String password = "root";

	private static Connection cnt = null;
	private static PreparedStatement ps = null;
	private static ResultSet rs = null;

	static
	{
		try
		{
			Class.forName( driverName );
			cnt = DriverManager.getConnection( url, user, password );
		}
		catch ( Exception e )
		{
			e.printStackTrace( );
		}
	}

	private static Connection getConnection( ) throws SQLException
	{
		return cnt;
	}

	/**
	 * 执行单条sql语句更新
	 *
	 * @param sql
	 * @param parameters
	 */
	public static int executeUpdate( String sql, String[] parameters )
	{
		try
		{
			cnt = getConnection( );
			ps = cnt.prepareStatement( sql );
			for ( int i = 0; i < parameters.length; i++ )
			{
				ps.setString( i + 1, parameters[i] );
			}
			int flag = ps.executeUpdate( );
			return flag;
		}
		catch ( SQLException e )
		{
			throw new RuntimeException( );
		}
		finally
		{
			close( cnt, ps, rs );
		}
	}

	/**
	 * 执行多条sql语句更新
	 *
	 * @param sqls
	 * @param parameters
	 */
	public static void executeTransUpdate( String[] sqls,
			String[][] parameters )
	{
		try
		{
			cnt = getConnection( );
			cnt.setAutoCommit( false );
			for ( int i = 0; i < sqls.length; i++ )
			{
				ps = cnt.prepareStatement( sqls[i] );
				if ( parameters[i] != null )
				{
					for ( int j = 0; j < parameters[i].length; j++ )
					{
						ps.setString( j + 1, parameters[i][j] );
					}
				}
				ps.executeUpdate( );
			}
			cnt.commit( );
		}
		catch ( Exception e )
		{
			try
			{
				cnt.rollback( );
			}
			catch ( Exception e1 )
			{
				// TODO Auto-generated catch block
				e1.printStackTrace( );
			}
			throw new RuntimeException( );
		}
		finally
		{
			close( cnt, ps, rs );
		}
	}

	/**
	 * 执行带参数的sql查询
	 */
	public static ResultSet executeQuery( String sql, String[] parameters )
			throws SQLException
	{
		cnt = getConnection( );
		ps = cnt.prepareStatement( sql );
		if ( parameters != null )
		{
			for ( int i = 0; i < parameters.length; i++ )
			{
				ps.setString( i + 1, parameters[i] );
			}
		}
		rs = ps.executeQuery( );

		return rs;
	}

	/**
	 * 执行不带参数的sql查询
	 */
	public static ResultSet executeQuery( String sql ) throws SQLException
	{

		cnt = getConnection( );
		ps = cnt.prepareStatement( sql );
		rs = ps.executeQuery( );

		return rs;
	}

	public static void close( )
	{
		close( cnt, ps, rs );
	}

	private static void close( Connection cnt, Statement ps, ResultSet rs )
	{
		if ( rs != null )
		{
			try
			{
				rs.close( );
			}
			catch ( SQLException e )
			{
				// TODO Auto-generated catch block
				e.printStackTrace( );
			}

		}
		if ( ps != null )
		{
			try
			{
				ps.close( );
			}
			catch ( SQLException e )
			{
				// TODO Auto-generated catch block
				e.printStackTrace( );
			}
		}
		if ( cnt != null )
		{
			try
			{
				cnt.close( );
			}
			catch ( SQLException e )
			{
				// TODO Auto-generated catch block
				e.printStackTrace( );
			}
		}
	}
}

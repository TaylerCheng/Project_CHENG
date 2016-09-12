package com.cg.usermgmt.test;

import com.cg.usermgmt.model.dao.UserDao;
import com.cg.usermgmt.utils.DBHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by cheng on 2016/5/29.
 */
public class DBTest
{

	private static Logger logger = LoggerFactory.getLogger( UserDao.class );

	public static void main( String[] args )
	{

		String sql = "select * from user";
		try
		{
			ResultSet rs = DBHelper.executeQuery( sql );
			if ( rs != null && rs.next())
			{
				logger.info(
						"数据库访问成功，username = " + rs.getString( "username" ) );
			}
		}
		catch ( SQLException e )
		{
			logger.info( "数据库访问失败，username = ", e );
		}
		finally
		{
			DBHelper.close( );
		}
	}
}

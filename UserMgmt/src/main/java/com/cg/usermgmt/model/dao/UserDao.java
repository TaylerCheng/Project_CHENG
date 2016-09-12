package com.cg.usermgmt.model.dao;

import com.cg.usermgmt.utils.DBHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by cheng on 2016/5/29.
 */
public class UserDao
{

	private static Logger logger = LoggerFactory.getLogger( UserDao.class );

	public boolean validate( String username, String password )
	{
		String sql = "select * from user where username=? and password = ?;";
		try
		{
			ResultSet rs = DBHelper
					.executeQuery( sql, new String[]{username, password} );
			if ( rs != null && rs.next() )
			{
				logger.info(
						"数据库访问成功，username = " + rs.getString( "username" ) );
				return true;
			}
		}
		catch ( SQLException e )
		{
			logger.info( "数据库访问失败，username = " + username, e );
		}
		finally
		{
			DBHelper.close( );
		}
		return false;
	}

}

package com.cg.usermgmt.controller;

import com.cg.usermgmt.model.dao.UserDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by cheng on 2016/5/29.
 */
public class LoginClServlet extends HttpServlet
{

	private final Logger logger = LoggerFactory
			.getLogger( LoginClServlet.class );

	private UserDao userDao = new UserDao( );

	public void doGet( HttpServletRequest request,
			HttpServletResponse response )
			throws ServletException, IOException
	{
		String username = request.getParameter( "username" );
		String password = request.getParameter( "password" );
		//从session中获取验证码
		String sessionCheckCode = (String) request.getSession( ).getAttribute(
				"checkCode" );
		String checkCode = request.getParameter( "checkcode" );

		if ( userDao.validate( username, password ) )
		{
			logger.info( "用户验证成功，username = " + username );
			request.getRequestDispatcher( "/MainFrameServlet" )
					.forward( request, response );
		}
		else
		{
			logger.info( "用户验证失败，username = " + username );
			request.getRequestDispatcher( "/LoginErrServlet" )
					.forward( request, response );
		}

	}

	public void doPost( HttpServletRequest request,
			HttpServletResponse response )
			throws ServletException, IOException
	{
		doGet( request, response );
	}
}
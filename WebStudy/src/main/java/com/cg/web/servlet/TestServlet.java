package com.cg.web.servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by Cheng Guang on 2016/9/6.
 */
public class TestServlet extends HttpServlet
{

	@Override
	protected void doGet( HttpServletRequest req, HttpServletResponse resp )
			throws ServletException, IOException
	{
		System.out.println( req.getRequestURI( ) );
	}

	@Override
	protected void doPost( HttpServletRequest req, HttpServletResponse resp )
			throws ServletException, IOException
	{
		this.doGet( req, resp );
	}
}
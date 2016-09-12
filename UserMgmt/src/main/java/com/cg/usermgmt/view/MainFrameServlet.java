package com.cg.usermgmt.view;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

public class MainFrameServlet extends HttpServlet
{

	public void doGet( HttpServletRequest request,
			HttpServletResponse response )
			throws ServletException, IOException
	{
		response.setHeader( "Content-Type", "text/html; charset=UTF-8" );
		// 创建输出流
		OutputStream out = response.getOutputStream( );
		PrintWriter pw = new PrintWriter( out );
		pw.println( "<h1>你好，登录成功~~~</h1>" );
		// 关闭输出流
		pw.close( );
	}

	public void doPost( HttpServletRequest request,
			HttpServletResponse response )
			throws ServletException, IOException
	{
		doGet( request, response );
	}
}
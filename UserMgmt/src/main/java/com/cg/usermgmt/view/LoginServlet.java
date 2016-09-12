package com.cg.usermgmt.view;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

public class LoginServlet extends HttpServlet
{

	public void doGet( HttpServletRequest request,
			HttpServletResponse response )
			throws ServletException, IOException
	{
		response.setHeader( "Content-Type", "text/html; charset=UTF-8" );
		// 创建输出流
		OutputStream out = response.getOutputStream( );
		PrintWriter pw = new PrintWriter( out );
		pw.println( "<h1>用户登录</h1>" );
		pw.println( "<form action = '/mgmt/LoginClServlet' method='GET'><br/>" );
		pw.println( "用户名：<input type ='text' name ='username'/><br/>" );
		pw.println( "密    码：<input type ='password' name ='password'/><br/>" );
		pw.println( "验证码：<input type ='text' name ='checkcode'/> <img id='image' src='/mgmt/CreateCheckCodeServlet'/> <br/>" );
		pw.println( "<input type = 'submit' value ='登录'/><br/>" );
		pw.println( "</form>" );
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
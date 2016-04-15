
package com.cg.mchtdata.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class JundgeServlet extends HttpServlet
{

	public void doGet( HttpServletRequest request, HttpServletResponse response )
			throws ServletException, IOException
	{
		// 获取请求流
		ServletInputStream sis = request.getInputStream( );

		String sessionCheckCode = (String) request.getSession( ).getAttribute(
				"checkCode" );
		System.out.println( "sessionCheckCode= " + sessionCheckCode );

		String checkCode = request.getParameter( "checkcode" );
		System.out.println( "checkCode= " + checkCode );

		System.out.println( "id= " + request.getParameter( "id" ) );
		// 创建输出流
		// OutputStream out = response.getOutputStream( );
		// PrintWriter pw = new PrintWriter( out );
		// pw.println( token );
		// pw.close( );
	}

	public void doPost( HttpServletRequest request, HttpServletResponse response )
			throws ServletException, IOException
	{
		doGet( request, response );
	}
}

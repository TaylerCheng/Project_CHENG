
package com.cg.mchtdata.controller;

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

public class LoginServlet extends HttpServlet
{

	public void doGet( HttpServletRequest request, HttpServletResponse response )
			throws ServletException, IOException
	{
		// 创建输出流
		OutputStream out = response.getOutputStream( );
		PrintWriter pw = new PrintWriter( out );
		pw.println( "<h1>用户登录</h1>" );
		pw.println( "<form action = '/WebExportdata/JundgeServlet' method='GET'><br/>" );
		pw.println( "用户名：<input type ='text' name ='id'/><br/>" );
		pw.println( "密    码：<input type ='password' name ='password'/><br/>" );
		pw.println( "验证码：<input type ='text' name ='checkcode'/><img id='image' src='/WebExportdata/CreateCheckCode'/><br/>" );
		pw.println( "<input type = 'submit' value ='登录'/><br/>" );
		pw.println( "</form>" );
		// 关闭输出流
		pw.close( );
	}

	public void doPost( HttpServletRequest request, HttpServletResponse response )
			throws ServletException, IOException
	{
		// 获取请求流
		request.setCharacterEncoding( "utf-8" );
		ServletInputStream sis = request.getInputStream( );
		BufferedReader in = new BufferedReader( new InputStreamReader( sis ) );
		String token = request.getParameter( "token" );
		System.out.println( "token = " + token );
		System.out.println( request.getRequestURL( ) );
		String line;
		if ( ( line = in.readLine( ) ) != null )
		{
			// System.out.println( line );
		}

		// 创建输出流
		OutputStream out = response.getOutputStream( );
		// 创建缓冲区
		byte buffer[] = new byte[1024];
		int len = 0;
		// 循环将输入流中的内容读取到缓冲区当中
		while ( ( len = sis.read( buffer ) ) > 0 )
		{
			// 输出缓冲区的内容到浏览器，实现文件下载
			out.write( buffer, 0, len );
		}
		// 关闭输出流
		out.close( );
		// 关闭文件输入流
		in.close( );
	}
}
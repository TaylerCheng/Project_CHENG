
package com.cg.mchtdata.servlet;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class BookMall
 */
public class BookMall extends HttpServlet
{

	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet( HttpServletRequest request,
			HttpServletResponse response ) throws ServletException, IOException
	{
		// 创建输出流
		OutputStream out = response.getOutputStream( );
		PrintWriter pw = new PrintWriter( out );
		pw.println( "<h1>向上书城</h1>" );
		pw.println( "java书 <a href = /MyProject/BuyBook?id=1>加入购物车</a><br/>" );
		pw.println( "Time杂志 <a href = /MyProject/BuyBook?id=2>加入购物车</a><br/>" );
		pw.println( "Ruby书 <a href = /MyProject/BuyBook?id=3>加入购物车</a>" );
		pw.close( );
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost( HttpServletRequest request,
			HttpServletResponse response ) throws ServletException, IOException
	{
		// TODO Auto-generated method stub
	}

}
